open System
open System.IO
open AITeam.CodeSight

let printUsage () =
    eprintfn "AITeam.CodeSight — code intelligence for any codebase"
    eprintfn ""
    eprintfn "Usage:"
    eprintfn "  code-sight index [--repo <path>]                     Build/update index"
    eprintfn "  code-sight modules [--repo <path>]                   Show project map"
    eprintfn "  code-sight search <js> [--repo <path>] [--scope <s>] Run a query"
    eprintfn "  code-sight intel <question> [--repo <path>]          Ask about the codebase"
    eprintfn "  code-sight repl [--repo <path>] [--scope <s>]        Interactive mode"
    eprintfn "  code-sight scopes [--repo <path>]                    List available scopes"
    eprintfn ""

let parseArgs (args: string[]) =
    let mutable repo = Environment.CurrentDirectory
    let mutable command = ""
    let mutable query = ""
    let mutable scope = ""
    let mutable i = 0
    while i < args.Length do
        match args.[i] with
        | "--repo" when i + 1 < args.Length ->
            repo <- args.[i + 1]
            i <- i + 2
        | "--scope" when i + 1 < args.Length ->
            scope <- args.[i + 1]
            i <- i + 2
        | "index" | "modules" | "repl" | "scopes" ->
            command <- args.[i]
            i <- i + 1
        | "intel" when i + 1 < args.Length ->
            command <- "intel"
            query <- args.[i + 1]
            i <- i + 2
        | "intel" ->
            command <- "intel"
            i <- i + 1
        | "search" when i + 1 < args.Length ->
            command <- "search"
            query <- args.[i + 1]
            i <- i + 2
        | "search" ->
            command <- "search"
            i <- i + 1
        | arg when command = "" ->
            command <- "search"
            query <- arg
            i <- i + 1
        | _ -> i <- i + 1
    repo, command, query, scope

let runIndex (cfg: CodeSightConfig) =
    let hashesPath = Path.Combine(cfg.IndexDir, "hashes.json")
    Directory.CreateDirectory(cfg.IndexDir) |> ignore

    // Index ALL scope dirs (union) so every scope can query
    let allDirs = cfg.Scopes |> Array.collect (fun s -> s.Dirs) |> Array.distinct
    let cfgAll = { cfg with SrcDirs = allDirs }

    let allFilesAbs = TreeSitterChunker.findSourceFiles cfgAll
    let toRel (f: string) = Path.GetRelativePath(cfg.RepoRoot, f).Replace("\\", "/")
    let allFilesRel = allFilesAbs |> Array.map toRel
    eprintfn "Found %d source files in %A (scopes: %s)" allFilesAbs.Length allDirs (cfg.Scopes |> Array.map (fun s -> s.Name) |> String.concat ", ")

    // Compute current hashes (relative path → hash)
    let currentHashes = Array.zip allFilesRel allFilesAbs |> Array.map (fun (rel, abs) -> rel, FileHashing.hashFile abs) |> Map.ofArray
    let oldHashes = FileHashing.loadHashes hashesPath

    let changed = currentHashes |> Map.toArray |> Array.filter (fun (f, h) -> match Map.tryFind f oldHashes with Some old -> old <> h | None -> true) |> Array.map fst
    let removed = oldHashes |> Map.toArray |> Array.filter (fun (f, _) -> not (currentHashes.ContainsKey f)) |> Array.map fst
    let unchanged = currentHashes |> Map.toArray |> Array.filter (fun (f, h) -> match Map.tryFind f oldHashes with Some old -> old = h | None -> false) |> Array.map fst

    // Map relative back to absolute for chunking
    let relToAbs = Array.zip allFilesRel allFilesAbs |> Map.ofArray
    let absOf rel = Map.find rel relToAbs

    if changed.Length = 0 && removed.Length = 0 then
        eprintfn "Index is up to date (%d files, no changes)" allFilesAbs.Length
    else
        eprintfn "  Changed: %d, Unchanged: %d, Removed: %d" changed.Length unchanged.Length removed.Length

        let changedAbs = changed |> Array.map absOf

        // Chunk changed files
        eprintfn "▶ Chunking %d changed files..." changed.Length
        let newChunks = TreeSitterChunker.chunkFiles cfgAll changedAbs
        eprintfn "  %d chunks from changed files" newChunks.Length

        // Load existing index for unchanged chunks
        let existingIdx = IndexStore.load cfg.IndexDir

        // Merge: keep unchanged chunks, add new
        let unchangedSet = Set.ofArray unchanged
        let oldChunks =
            match existingIdx with
            | Some idx -> idx.Chunks |> Array.filter (fun c -> unchangedSet.Contains c.FilePath)
            | None -> [||]
        let allChunkEntries =
            let newEntries = newChunks |> Array.map (fun c ->
                { FilePath = c.FilePath; Module = c.Module; Name = c.Name; Kind = c.Kind
                  StartLine = c.StartLine; EndLine = c.EndLine; Summary = ""; Signature = ""; Extra = Map.empty })
            Array.append oldChunks newEntries

        // Extract imports and signatures (full — fast)
        eprintfn "▶ Extracting imports..."
        let imports = TreeSitterChunker.extractImports cfgAll allFilesAbs |> Array.map (fun i -> i.FilePath, i.Module)
        eprintfn "  %d import edges" imports.Length

        eprintfn "▶ Extracting signatures..."
        let signatures = TreeSitterChunker.extractSignatures cfgAll allFilesAbs
        eprintfn "  %d signatures" signatures.Length

        eprintfn "▶ Extracting type refs..."
        let typeRefs = TreeSitterChunker.extractTypeRefs cfgAll allFilesAbs |> Array.map (fun r -> r.FilePath, r.TypeRefs)
        eprintfn "  %d files with type refs" typeRefs.Length

        // Match signatures to chunks
        let sigLookup = signatures |> Array.map (fun s -> (s.FilePath, s.Name, s.StartLine), s.Signature) |> dict
        let finalChunks =
            allChunkEntries |> Array.map (fun c ->
                let sig' =
                    match sigLookup.TryGetValue((c.FilePath, c.Name, c.StartLine)) with
                    | true, v -> v
                    | _ ->
                        let shortName = c.Name.Split('.') |> Array.last
                        match sigLookup.TryGetValue((c.FilePath, shortName, c.StartLine)) with
                        | true, v -> v | _ -> c.Signature
                if sig' <> "" then { c with Signature = sig' } else c)

        // Embeddings: keep old for unchanged, compute new
        eprintfn "▶ Computing embeddings for %d new chunks..." (finalChunks.Length - oldChunks.Length)
        // For now, store empty embeddings — will be computed when embedding server is available
        let codeEmbs =
            match existingIdx with
            | Some idx when idx.CodeEmbeddings.Length = oldChunks.Length ->
                // Pad with empty arrays for new chunks
                let newCount = finalChunks.Length - oldChunks.Length
                Array.append idx.CodeEmbeddings (Array.init newCount (fun _ -> [||]))
            | _ -> Array.init finalChunks.Length (fun _ -> [||])
        let sumEmbs =
            match existingIdx with
            | Some idx when idx.SummaryEmbeddings.Length = oldChunks.Length ->
                let newCount = finalChunks.Length - oldChunks.Length
                Array.append idx.SummaryEmbeddings (Array.init newCount (fun _ -> [||]))
            | _ -> Array.init finalChunks.Length (fun _ -> [||])

        let dim = if codeEmbs.Length > 0 && codeEmbs.[0].Length > 0 then codeEmbs.[0].Length else 0

        let index : CodeIndex = {
            Chunks = finalChunks
            CodeEmbeddings = codeEmbs
            SummaryEmbeddings = sumEmbs
            Imports = imports
            TypeRefs = typeRefs
            EmbeddingDim = dim
        }
        IndexStore.save cfg.IndexDir index
        FileHashing.saveHashes hashesPath currentHashes
        eprintfn "✓ Index built: %d chunks, %d imports, %d signatures" finalChunks.Length imports.Length signatures.Length

[<EntryPoint>]
let main args =
    let repo, command, query, scope = parseArgs args

    match command with
    | "index" ->
        let cfg = Config.load repo
        runIndex cfg
        0
    | "scopes" ->
        let cfg = Config.load repo
        for s in cfg.Scopes do
            eprintfn "  %-12s → %s" s.Name (s.Dirs |> String.concat ", ")
        0
    | "modules" | "search" | "repl" | "intel" ->
        let cfg = Config.load repo
        match IndexStore.load cfg.IndexDir with
        | None ->
            eprintfn "No index found. Run 'code-sight index' first."
            1
        | Some fullIndex ->
            // Filter index by scope if specified
            let index =
                if scope = "" then fullIndex
                else
                    let scopeDirs = Config.scopeDirs cfg scope
                    let inScope (filePath: string) =
                        scopeDirs |> Array.exists (fun d -> filePath.Replace("\\", "/").StartsWith(d + "/") || filePath.Replace("\\", "/").StartsWith(d + "\\"))
                    { fullIndex with
                        Chunks = fullIndex.Chunks |> Array.filter (fun c -> inScope c.FilePath)
                        Imports = fullIndex.Imports |> Array.filter (fun (f, _) -> inScope f)
                        TypeRefs = fullIndex.TypeRefs |> Array.filter (fun (f, _) -> inScope f)
                        // Embeddings need re-indexing to match — for now keep all (search will return some out-of-scope)
                        CodeEmbeddings = fullIndex.CodeEmbeddings
                        SummaryEmbeddings = fullIndex.SummaryEmbeddings }

            // Lazy-load source chunks
            let chunksRef = lazy (
                let scopeCfg = if scope = "" then cfg else { cfg with SrcDirs = Config.scopeDirs cfg scope }
                let allFiles = TreeSitterChunker.findSourceFiles scopeCfg
                if allFiles.Length > 0 then Some (TreeSitterChunker.chunkFiles scopeCfg allFiles)
                else None)
            // For modules/files/context/impact/imports/deps — no chunks needed
            // Pass None initially; primitives that need chunks will force the lazy
            let mutable engine = QueryEngine.create index None cfg.EmbeddingUrl
            let needsChunks = [| "expand"; "grep"; "refs"; "neighborhood"; "similar" |]
            let ensureChunks (js: string) =
                if needsChunks |> Array.exists (fun p -> js.Contains(p)) then
                    if not chunksRef.IsValueCreated then
                        engine <- QueryEngine.create index chunksRef.Value cfg.EmbeddingUrl

            match command with
            | "modules" ->
                printfn "%s" (QueryEngine.eval engine "modules()")
                0
            | "search" ->
                if query = "" then
                    eprintfn "Usage: code-sight search '<js query>'"
                    1
                else
                    ensureChunks query
                    printfn "%s" (QueryEngine.eval engine query)
                    0
            | "repl" ->
                eprintfn "code-sight REPL. Type JS queries, 'quit' to exit."
                eprintfn "  search(q,opts), refs(name,opts), grep(pattern,opts), modules(),"
                eprintfn "  files(p?), context(file), expand(id), neighborhood(id,opts),"
                eprintfn "  impact(type), imports(file), deps(pattern), similar(id,opts)"
                eprintfn ""
                // Pre-load chunks for repl since user will likely need them
                engine <- QueryEngine.create index chunksRef.Value cfg.EmbeddingUrl
                let mutable running = true
                while running do
                    eprintf "> "
                    let line = System.Console.ReadLine()
                    if line = null || line.Trim() = "quit" || line.Trim() = "exit" then
                        running <- false
                    elif line.Trim() <> "" then
                        printfn "%s" (QueryEngine.eval engine (line.Trim()))
                        printfn ""
                0
            | "intel" ->
                if query = "" then
                    eprintfn "Usage: code-sight intel '<question>'"
                    1
                else
                    // Ensure chunks loaded for the mini-agent's code_search calls
                    engine <- QueryEngine.create index chunksRef.Value cfg.EmbeddingUrl
                    let modulesCache = QueryEngine.eval engine "modules()"
                    let playbooksDir =
                        // 1. Per-repo override
                        let repoPlaybooks = Path.Combine(repo, ".code-intel", "playbooks")
                        if Directory.Exists repoPlaybooks then repoPlaybooks
                        else
                        // 2. Alongside exe
                        let exePlaybooks = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "playbooks")
                        if Directory.Exists exePlaybooks then exePlaybooks
                        else
                        // 3. Source tree (dev mode)
                        Path.Combine(repo, "playbooks")
                    eprintfn "Playbook: %s → dispatching to mini-model..." (Intel.classifyPlaybook query)
                    let result = Intel.run engine playbooksDir query modulesCache |> Async.AwaitTask |> Async.RunSynchronously
                    printfn "%s" result
                    0
            | _ -> 1
    | "" ->
        printUsage()
        0
    | other ->
        eprintfn "Unknown command: %s" other
        printUsage()
        1


