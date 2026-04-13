/**
 * Tree-sitter based code chunker — interactive JSONL protocol.
 * Uses web-tree-sitter (WASM) for cross-platform support.
 *
 * Protocol:
 *   → stdin:  {"cmd":"parse","file":"/path/to/File.fs","maxChars":3000}
 *   ← stdout: {"ok":true,"chunks":[...]}
 *
 *   → stdin:  {"cmd":"quit"}
 *   (process exits)
 *
 * Each chunk has: { name, kind, startLine, endLine, content, context, module, filePath }
 *
 * Structure:
 *   languages/*.js   — language plugins (add a file to support a new language)
 *   chunker-core.js  — generic AST engine (stable, language-agnostic)
 *   ts-chunker.js    — parser cache, JSONL protocol (this file, stable)
 */

const Parser = require('web-tree-sitter');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const { LANGUAGES } = require('./languages/index');
const { extractChunks, extractImports, extractTypeRefs, extractSignatures } = require('./chunker-core');

// ─── WASM file resolution ─────────────────────────────────────────

const WASM_FILES = {
    'tree-sitter-fsharp':      'tree-sitter-fsharp/tree-sitter-fsharp.wasm',
    'tree-sitter-c-sharp':     'tree-sitter-c-sharp/tree-sitter-c_sharp.wasm',
    'tree-sitter-javascript':  'tree-sitter-javascript/tree-sitter-javascript.wasm',
    'tree-sitter-typescript':  'tree-sitter-typescript/tree-sitter-typescript.wasm',
    'tree-sitter-python':      'tree-sitter-python/tree-sitter-python.wasm',
    'tree-sitter-go':          'tree-sitter-go/tree-sitter-go.wasm',
    'tree-sitter-rust':        'tree-sitter-rust/tree-sitter-rust.wasm',
};

function resolveWasm(grammarName) {
    const rel = WASM_FILES[grammarName];
    if (!rel) return null;
    return path.resolve(__dirname, 'node_modules', rel);
}

// ─── Language cache (async, caches loaded Language objects) ────────

const languages = {};

async function getLanguage(ext) {
    if (languages[ext]) return languages[ext];

    const lang = LANGUAGES[ext];
    if (!lang) return null;

    const wasmPath = resolveWasm(lang.grammar);
    if (!wasmPath || !fs.existsSync(wasmPath)) {
        console.error(`WASM not found for ${lang.grammar}: ${wasmPath}`);
        return null;
    }

    const language = await Parser.Language.load(wasmPath);
    languages[ext] = { language, lang };
    return languages[ext];
}

// ─── Command handlers ────────────────────────────────────────────

async function handleCommand(parser, cmd) {
    if (cmd.cmd === 'quit') {
        process.exit(0);
    }

    if (cmd.cmd === 'languages') {
        return { ok: true, languages: Object.keys(LANGUAGES) };
    }

    const filePath = cmd.file;
    const ext = path.extname(filePath).slice(1);
    const cached = await getLanguage(ext);
    if (!cached) {
        return { ok: false, error: `Unsupported extension: .${ext}` };
    }

    parser.setLanguage(cached.language);
    const code = fs.readFileSync(filePath, 'utf8');
    const tree = parser.parse(code);

    switch (cmd.cmd) {
        case 'parse': {
            const maxChars = cmd.maxChars || 3000;
            const chunks = extractChunks(tree, cached.lang, filePath, maxChars);
            return { ok: true, chunks };
        }
        case 'imports': {
            const imports = extractImports(tree, cached.lang, filePath);
            return { ok: true, imports };
        }
        case 'typerefs': {
            const typeRefs = extractTypeRefs(tree, cached.lang, filePath);
            return { ok: true, typeRefs };
        }
        case 'signatures': {
            const signatures = extractSignatures(tree, cached.lang, filePath);
            return { ok: true, signatures };
        }
        default:
            return { ok: false, error: `Unknown command: ${cmd.cmd}` };
    }
}

// ─── Sequential JSONL loop ───────────────────────────────────────

async function main() {
    const runtimeWasm = path.resolve(__dirname, 'node_modules', 'web-tree-sitter', 'tree-sitter.wasm');
    await Parser.init({
        locateFile: () => runtimeWasm,
    });

    const parser = new Parser();
    const rl = readline.createInterface({ input: process.stdin, terminal: false });

    for await (const line of rl) {
        try {
            const cmd = JSON.parse(line);
            const result = await handleCommand(parser, cmd);
            console.log(JSON.stringify(result));
        } catch (e) {
            console.log(JSON.stringify({ ok: false, error: e.message }));
        }
    }
}

main().catch(e => {
    console.error('Fatal: ' + e.message);
    process.exit(1);
});
