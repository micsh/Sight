# AITeam.CodeSight

Code intelligence tool for any codebase. Indexes source code using tree-sitter AST parsing, embedding models, and LLM summaries. Provides semantic search, reference tracking, impact analysis, and code exploration via 12 JS primitives evaluated in Jint.

## Quick start

```
code-sight index                              # build index
code-sight search 'refs("MyType", {limit:5})' # query
code-sight modules                            # project map
code-sight repl                               # interactive
```

## Architecture

- **F# + Jint** for all logic
- **Node.js + tree-sitter** for AST parsing (spawned as child process)
- **Embedding model** (OpenAI-compatible) for semantic search
- **Local LLM** (optional, Bonsai/llama-server) for code summaries

## Status

Stage 1: Scaffold + incremental indexing
