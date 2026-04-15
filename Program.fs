open System
open System.IO
open AITeam.CodeSight

let printUsage () =
    printfn "AITeam.CodeSight — code intelligence for any codebase"
    printfn ""
    printfn "Usage:"
    printfn "  code-sight index [--repo <path>]                     Build/update index"
    printfn "  code-sight index --files <f1> <f2> ... [--repo <path>] Re-index specific files"
    printfn "  code-sight modules [--repo <path>]                   Show project map"
    printfn "  code-sight search <js> [--json] [--repo <path>] [--scope <s>] Run a query"
    printfn "  code-sight eval <js> [--json] [--repo <path>]                 Alias for search"
    printfn "  code-sight eval - [--json] [--repo <path>]                    Read expression from stdin"
    printfn "  code-sight intel <question> [--repo <path>]          Ask about the codebase"
    printfn "  code-sight repl [--repo <path>] [--scope <s>]        Interactive mode"
    printfn "  code-sight scopes [--repo <path>]                    List available scopes"
    printfn "  code-sight fn add <name> <body> [options]            Define a reusable function"
    printfn "  code-sight fn list [--verbose] [--json]              List saved functions"
    printfn "  code-sight fn rm <name> [--repo <path>]              Remove a function"
    printfn "  code-sight --help                                    Show this help"
    printfn ""
    printfn "Functions:"
    printfn "  Save chains of primitives as reusable functions. Example:"
    printfn "    code-sight fn add deepSearch --params \"q\" \"search(q).concat(similar(search(q)[0]))\""
    printfn "    code-sight search \"deepSearch('auth')\""
    printfn ""
    printfn "  Options for 'fn add':"
    printfn "    --params \"a,b\"          Comma-separated parameter names"
    printfn "    --desc \"description\"    Optional description"
    printfn "    --file <path>           Read function body from a file"
    printfn ""
    printfn "Primitives (available in search expressions):"
    printfn "  search(query, {limit, kind, file})  Semantic search across indexed chunks"
    printfn "  modules()                           Project map — directories, symbols, imports"
    printfn "  context(file)                       File outline with types, functions, imports"
    printfn "  expand(refId)                       Expand R# ref to full source content"
    printfn "  neighborhood(refId, {before, after}) Surrounding symbols in the same file"
    printfn "  similar(refId, {limit})              Semantically similar chunks"
    printfn "  grep(pattern, {limit, kind, file})   Regex search over source content"
    printfn "  files(pattern)                       List indexed source files"
    printfn "  impact(type)                         Find all references to a type"
    printfn "  imports(file)                        Show what a file imports"
    printfn "  deps(pattern)                        Show dependency graph for a module"
    printfn "  refs(name, {limit})                  Find all references to a symbol"
    printfn "  callers(name, {limit})               Find call sites of a qualified name"
    printfn "  walk(name, {depth, limit})           Walk the dependency graph from a symbol"
    printfn "  changed(gitRef)                      Chunks in files changed since a git ref"
    printfn "  hotspots({by, min})                  Structural metrics per file (chunks/loc/fanIn/fanOut)"
    printfn "  explain(refId)                       Debug: show index metadata for a ref"
    printfn "  saveSession(name)                    Save current ref session as a named snapshot"
    printfn "  loadSession(name)                    Load a previously saved ref session"
    printfn "  sessions()                           List saved sessions"
    printfn "  trace(from, to)                      BFS shortest path between two files"
    printfn "  arch(file)                           Architectural context from arch tool"
    printfn ""
    printfn "Composition helpers:"
    printfn "  pipe(value, fn1, fn2, ...)           Thread value through functions"
    printfn "  tap(value, fn)                       Run fn for side-effects, return value"
    printfn "  mergeBy(key, arr1, arr2, ...)        Union arrays with dedup by key"
    printfn "  print(value)                         Debug output to stderr"
    printfn ""
    printfn "Note: UDFs are available in search/eval and intel. Intel auto-discovers UDFs."
    printfn ""

let parseArgs (args: string[]) =
    let mutable repo = Environment.CurrentDirectory
    let mutable command = ""
    let mutable query = ""
    let mutable scope = ""
    let mutable fnName = ""
    let mutable fnParams = ""
    let mutable fnDesc = ""
    let mutable fnFile = ""
    let mutable verbose = false
    let mutable jsonOut = false
    let files = ResizeArray<string>()
    let mutable i = 0
    while i < args.Length do
        match args.[i] with
        | "--repo" when i + 1 < args.Length ->
            repo <- args.[i + 1]
            i <- i + 2
        | "--scope" when i + 1 < args.Length ->
            scope <- args.[i + 1]
            i <- i + 2
        | "--help" | "-h" ->
            command <- "help"
            i <- i + 1
        | "--files" ->
            i <- i + 1
            while i < args.Length && not (args.[i].StartsWith("--")) do
                files.Add(args.[i])
                i <- i + 1
        | "index" | "modules" | "repl" | "scopes" ->
            command <- args.[i]
            i <- i + 1
        | "fn" when i + 1 < args.Length ->
            match args.[i + 1] with
            | "add" when i + 2 < args.Length ->
                command <- "fn-add"
                fnName <- args.[i + 2]
                i <- i + 3
                while i < args.Length do
                    match args.[i] with
                    | "--params" when i + 1 < args.Length ->
                        fnParams <- args.[i + 1]
                        i <- i + 2
                    | "--desc" when i + 1 < args.Length ->
                        fnDesc <- args.[i + 1]
                        i <- i + 2
                    | "--file" when i + 1 < args.Length ->
                        fnFile <- args.[i + 1]
                        i <- i + 2
                    | "--repo" when i + 1 < args.Length ->
                        repo <- args.[i + 1]
                        i <- i + 2
                    | _ when query = "" ->
                        query <- args.[i]
                        i <- i + 1
                    | _ -> i <- i + 1
            | "list" ->
                command <- "fn-list"
                i <- i + 2
                while i < args.Length do
                    match args.[i] with
                    | "--verbose" | "-v" -> verbose <- true; i <- i + 1
                    | "--json" -> jsonOut <- true; i <- i + 1
                    | "--repo" when i + 1 < args.Length -> repo <- args.[i + 1]; i <- i + 2
                    | _ -> i <- i + 1
            | "rm" when i + 2 < args.Length ->
                command <- "fn-rm"
                fnName <- args.[i + 2]
                i <- i + 3
            | _ ->
                command <- "fn-list"
                i <- i + 2
        | "intel" when i + 1 < args.Length ->
            command <- "intel"
            query <- args.[i + 1]
            i <- i + 2
        | "intel" ->
            command <- "intel"
            i <- i + 1
        | "search" | "eval" when i + 1 < args.Length ->
            command <- "search"
            query <- args.[i + 1]
            i <- i + 2
            while i < args.Length do
                match args.[i] with
                | "--json" -> jsonOut <- true; i <- i + 1
                | "--repo" when i + 1 < args.Length -> repo <- args.[i + 1]; i <- i + 2
                | "--scope" when i + 1 < args.Length -> scope <- args.[i + 1]; i <- i + 2
                | _ -> i <- i + 1
        | "search" | "eval" ->
            command <- "search"
            i <- i + 1
        | arg when command = "" ->
            command <- "search"
            query <- arg
            i <- i + 1
        | _ -> i <- i + 1
    repo, command, query, scope, files.ToArray(), fnName, fnParams, fnDesc, fnFile, verbose, jsonOut

/// Ensure index dir exists and has a .gitignore so repos don't need to add it.
let private ensureIndexDir (dir: string) =
    Directory.CreateDirectory(dir) |> ignore
    let gi = Path.Combine(dir, ".gitignore")
    if not (File.Exists gi) then File.WriteAllText(gi, "# Auto-generated by code-sight — keep index out of source control\n*\n!.gitignore\n")

let runIndex (cfg: CodeSightConfig) =
    let hashesPath = Path.Combine(cfg.IndexDir, "hashes.json")
    ensureIndexDir cfg.IndexDir

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
        let unchangedSet = Set.ofArray unchanged

        // Chunk changed files
        eprintfn "▶ Chunking %d changed files..." changed.Length
        let newChunks = TreeSitterChunker.chunkFiles cfgAll changedAbs
        eprintfn "  %d chunks from changed files" newChunks.Length

        // Load cached source chunks for unchanged files
        let cachedSourceChunks =
            match IndexStore.loadSourceChunks cfg.IndexDir with
            | Some cached -> cached |> Array.filter (fun c -> unchangedSet.Contains c.FilePath)
            | None -> [||]

        // Merge all source chunks (cached unchanged + new)
        let allSourceChunks = Array.append cachedSourceChunks newChunks

        // Load existing index for unchanged chunks
        let existingIdx = IndexStore.load cfg.IndexDir
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
        let newChunkCount = finalChunks.Length - oldChunks.Length
        eprintfn "▶ Computing embeddings for %d new chunks..." newChunkCount

        // Prepare texts for embedding: context + content for code, name + kind for summary
        let newChunkTexts =
            finalChunks.[oldChunks.Length..]
            |> Array.map (fun c ->
                let context = if c.Module <> "" then sprintf "%s\n%s:%s" c.Module c.Kind c.Name else sprintf "%s:%s" c.Kind c.Name
                sprintf "%s\n%s" context (match newChunks |> Array.tryFind (fun ch -> ch.FilePath = c.FilePath && ch.Name = c.Name && ch.StartLine = c.StartLine) with Some ch -> ch.Content | None -> c.Name))

        let embedBatch (texts: string[]) =
            if texts.Length = 0 then [||]
            else
                let results = ResizeArray()
                for batch in texts |> Array.chunkBySize cfg.EmbeddingBatchSize do
                    match EmbeddingService.embed cfg.EmbeddingUrl batch |> Async.AwaitTask |> Async.RunSynchronously with
                    | Some embs -> results.AddRange(embs)
                    | None ->
                        eprintfn "  Warning: embedding server unavailable — storing empty embeddings"
                        results.AddRange(Array.init batch.Length (fun _ -> [||]))
                results.ToArray()

        let newCodeEmbs = embedBatch newChunkTexts
        let codeEmbs =
            match existingIdx with
            | Some idx when idx.CodeEmbeddings.Length = oldChunks.Length ->
                Array.append idx.CodeEmbeddings newCodeEmbs
            | _ -> embedBatch (finalChunks |> Array.map (fun c -> sprintf "%s:%s %s" c.Kind c.Name c.Summary))

        // Summary embeddings: use summary text (if available) or name
        let newSumTexts = finalChunks.[oldChunks.Length..] |> Array.map (fun c -> if c.Summary <> "" then c.Summary else sprintf "%s %s" c.Kind c.Name)
        let newSumEmbs = embedBatch newSumTexts
        let sumEmbs =
            match existingIdx with
            | Some idx when idx.SummaryEmbeddings.Length = oldChunks.Length ->
                Array.append idx.SummaryEmbeddings newSumEmbs
            | _ -> embedBatch (finalChunks |> Array.map (fun c -> if c.Summary <> "" then c.Summary else sprintf "%s %s" c.Kind c.Name))

        if codeEmbs.Length > 0 && codeEmbs.[0].Length > 0 then
            eprintfn "  %d embeddings (%d dimensions)" codeEmbs.Length codeEmbs.[0].Length
        else
            eprintfn "  Embeddings not available (no server or empty responses)"

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
        IndexStore.saveSourceChunks cfg.IndexDir allSourceChunks
        FileHashing.saveHashes hashesPath currentHashes
        eprintfn "✓ Index built: %d chunks, %d imports, %d signatures" finalChunks.Length imports.Length signatures.Length

/// Re-index only the specified files, merging with cached index for everything else.
let runIndexFiles (cfg: CodeSightConfig) (files: string[]) =
    let hashesPath = Path.Combine(cfg.IndexDir, "hashes.json")
    ensureIndexDir cfg.IndexDir

    let allDirs = cfg.Scopes |> Array.collect (fun s -> s.Dirs) |> Array.distinct
    let cfgAll = { cfg with SrcDirs = allDirs }

    // Resolve files to absolute + relative paths
    let resolvedFiles =
        files |> Array.choose (fun f ->
            let abs =
                if Path.IsPathRooted f then f
                else Path.GetFullPath(Path.Combine(cfg.RepoRoot, f))
            if File.Exists abs then
                let rel = Path.GetRelativePath(cfg.RepoRoot, abs).Replace("\\", "/")
                Some (rel, abs)
            else
                eprintfn "  Warning: file not found, skipping: %s" f
                None)

    if resolvedFiles.Length = 0 then
        eprintfn "No valid files to index."
    else
        let changedRel = resolvedFiles |> Array.map fst
        let changedAbs = resolvedFiles |> Array.map snd
        let changedSet = Set.ofArray changedRel

        eprintfn "▶ Re-indexing %d files..." resolvedFiles.Length
        for rel, _ in resolvedFiles do eprintfn "  %s" rel

        // Everything NOT in the changed set is unchanged
        let oldHashes = FileHashing.loadHashes hashesPath
        let unchangedSet =
            oldHashes |> Map.toArray
            |> Array.filter (fun (f, _) -> not (changedSet.Contains f))
            |> Array.map fst |> Set.ofArray

        // Chunk changed files
        eprintfn "▶ Chunking %d files..." changedAbs.Length
        let newChunks = TreeSitterChunker.chunkFiles cfgAll changedAbs
        eprintfn "  %d chunks from changed files" newChunks.Length

        // Load cached source chunks for unchanged files
        let cachedSourceChunks =
            match IndexStore.loadSourceChunks cfg.IndexDir with
            | Some cached -> cached |> Array.filter (fun c -> unchangedSet.Contains c.FilePath)
            | None -> [||]
        let allSourceChunks = Array.append cachedSourceChunks newChunks

        // Load existing index for unchanged chunks
        let existingIdx = IndexStore.load cfg.IndexDir
        let oldChunks =
            match existingIdx with
            | Some idx -> idx.Chunks |> Array.filter (fun c -> unchangedSet.Contains c.FilePath)
            | None -> [||]
        let allChunkEntries =
            let newEntries = newChunks |> Array.map (fun c ->
                { FilePath = c.FilePath; Module = c.Module; Name = c.Name; Kind = c.Kind
                  StartLine = c.StartLine; EndLine = c.EndLine; Summary = ""; Signature = ""; Extra = Map.empty })
            Array.append oldChunks newEntries

        // Imports and signatures (only re-extract for changed files, keep cached for rest)
        let allFilesAbs =
            let cachedFiles = unchangedSet |> Set.toArray |> Array.map (fun rel -> Path.GetFullPath(Path.Combine(cfg.RepoRoot, rel)))
            Array.append cachedFiles changedAbs

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
        let newChunkCount = finalChunks.Length - oldChunks.Length
        eprintfn "▶ Computing embeddings for %d new chunks..." newChunkCount

        let newChunkTexts =
            finalChunks.[oldChunks.Length..]
            |> Array.map (fun c ->
                let context = if c.Module <> "" then sprintf "%s\n%s:%s" c.Module c.Kind c.Name else sprintf "%s:%s" c.Kind c.Name
                sprintf "%s\n%s" context (match newChunks |> Array.tryFind (fun ch -> ch.FilePath = c.FilePath && ch.Name = c.Name && ch.StartLine = c.StartLine) with Some ch -> ch.Content | None -> c.Name))

        let embedBatch (texts: string[]) =
            if texts.Length = 0 then [||]
            else
                let results = ResizeArray()
                for batch in texts |> Array.chunkBySize cfg.EmbeddingBatchSize do
                    match EmbeddingService.embed cfg.EmbeddingUrl batch |> Async.AwaitTask |> Async.RunSynchronously with
                    | Some embs -> results.AddRange(embs)
                    | None ->
                        eprintfn "  Warning: embedding server unavailable — storing empty embeddings"
                        results.AddRange(Array.init batch.Length (fun _ -> [||]))
                results.ToArray()

        let newCodeEmbs = embedBatch newChunkTexts
        let codeEmbs =
            match existingIdx with
            | Some idx when idx.CodeEmbeddings.Length = oldChunks.Length ->
                Array.append idx.CodeEmbeddings newCodeEmbs
            | _ -> embedBatch (finalChunks |> Array.map (fun c -> sprintf "%s:%s %s" c.Kind c.Name c.Summary))

        let newSumTexts = finalChunks.[oldChunks.Length..] |> Array.map (fun c -> if c.Summary <> "" then c.Summary else sprintf "%s %s" c.Kind c.Name)
        let newSumEmbs = embedBatch newSumTexts
        let sumEmbs =
            match existingIdx with
            | Some idx when idx.SummaryEmbeddings.Length = oldChunks.Length ->
                Array.append idx.SummaryEmbeddings newSumEmbs
            | _ -> embedBatch (finalChunks |> Array.map (fun c -> if c.Summary <> "" then c.Summary else sprintf "%s %s" c.Kind c.Name))

        if codeEmbs.Length > 0 && codeEmbs.[0].Length > 0 then
            eprintfn "  %d embeddings (%d dimensions)" codeEmbs.Length codeEmbs.[0].Length

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
        IndexStore.saveSourceChunks cfg.IndexDir allSourceChunks

        // Update hashes only for the changed files
        let updatedHashes =
            oldHashes
            |> Map.toArray |> Array.filter (fun (f, _) -> not (changedSet.Contains f))
            |> Array.append (resolvedFiles |> Array.map (fun (rel, abs) -> rel, FileHashing.hashFile abs))
            |> Map.ofArray
        FileHashing.saveHashes hashesPath updatedHashes
        eprintfn "✓ Index updated: %d chunks, %d imports, %d signatures" finalChunks.Length imports.Length signatures.Length

[<EntryPoint>]
let main args =
    let repo, command, query, scope, files, fnName, fnParams, fnDesc, fnFile, verbose, jsonOut = parseArgs args

    match command with
    | "help" ->
        printUsage()
        0
    | "fn-add" ->
        let body =
            if fnFile <> "" then
                let path = if File.Exists fnFile then fnFile else Path.Combine(repo, fnFile)
                if File.Exists path then File.ReadAllText(path)
                else eprintfn "File not found: %s" fnFile; ""
            else query
        if body = "" then
            eprintfn "No function body provided. Pass it as an argument or use --file <path>."
            1
        else
            let ps = if fnParams = "" then [||] else fnParams.Split(',') |> Array.map (fun s -> s.Trim())
            let fn = { Name = fnName; Params = ps; Body = body; Description = fnDesc }
            match FunctionStore.add repo fn with
            | Ok msg -> printfn "%s" msg; 0
            | Error msg -> eprintfn "Error: %s" msg; 1
    | "fn-list" ->
        let fns = FunctionStore.load repo
        if fns.Length = 0 then
            if jsonOut then printfn "[]"
            else printfn "No functions defined. Use 'code-sight fn add <name> <body>' to create one."
        elif jsonOut then
            let options = System.Text.Json.JsonSerializerOptions(WriteIndented = true)
            let arr = fns |> Array.map (fun f ->
                dict [ "name", box f.Name; "params", box f.Params; "body", box f.Body; "description", box f.Description ])
            printfn "%s" (System.Text.Json.JsonSerializer.Serialize(arr, options))
        elif verbose then
            printfn "%d function(s) in %s:" fns.Length repo
            printfn ""
            for f in fns do
                let joined = f.Params |> String.concat ", "
                let ps = if f.Params.Length = 0 then "()" else sprintf "(%s)" joined
                printfn "  %s%s" f.Name ps
                if f.Description <> "" then printfn "    %s" f.Description
                printfn "    body: %s" f.Body
                printfn ""
        else
            printfn "%d function(s):" fns.Length
            for f in fns do
                let joined = f.Params |> String.concat ", "
                let ps = if f.Params.Length = 0 then "()" else sprintf "(%s)" joined
                let desc = if f.Description <> "" then sprintf " — %s" f.Description else ""
                printfn "  %s%s%s" f.Name ps desc
        0
    | "fn-rm" ->
        match FunctionStore.remove repo fnName with
        | Ok msg -> printfn "%s" msg; 0
        | Error msg -> eprintfn "Error: %s" msg; 1
    | "index" ->
        let cfg = Config.load repo
        if files.Length > 0 then runIndexFiles cfg files
        else runIndex cfg
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

            // Lazy-load source chunks — try cache first, fall back to re-chunking
            let chunksRef = lazy (
                match IndexStore.loadSourceChunks cfg.IndexDir with
                | Some cached ->
                    eprintfn "  [loaded %d cached source chunks]" cached.Length
                    Some cached
                | None ->
                    eprintfn "  [no cache — re-chunking source files...]"
                    let scopeCfg = if scope = "" then cfg else { cfg with SrcDirs = Config.scopeDirs cfg scope }
                    let allFiles = TreeSitterChunker.findSourceFiles scopeCfg
                    if allFiles.Length > 0 then Some (TreeSitterChunker.chunkFiles scopeCfg allFiles)
                    else None)
            // For modules/files/context/impact/imports/deps — no chunks needed
            // Pass None initially; primitives that need chunks will force the lazy
            let mutable engine = QueryEngine.create index None cfg.EmbeddingUrl cfg.IndexDir cfg.RepoRoot cfg.SrcDirs
            let needsChunks = [| "expand"; "grep"; "refs"; "neighborhood"; "similar"; "walk"; "callers"; "explain" |]
            let ensureChunks (js: string) =
                if needsChunks |> Array.exists (fun p -> js.Contains(p)) then
                    if not chunksRef.IsValueCreated then
                        engine <- QueryEngine.create index chunksRef.Value cfg.EmbeddingUrl cfg.IndexDir cfg.RepoRoot cfg.SrcDirs

            match command with
            | "modules" ->
                printfn "%s" (QueryEngine.eval engine "modules()")
                0
            | "search" ->
                let actualQuery =
                    if query = "-" then
                        use reader = new StreamReader(Console.OpenStandardInput())
                        reader.ReadToEnd().Trim()
                    else query
                if actualQuery = "" then
                    eprintfn "Usage: code-sight search '<js query>'"
                    1
                else
                    ensureChunks actualQuery
                    let result =
                        if jsonOut then QueryEngine.evalJson engine actualQuery
                        else QueryEngine.eval engine actualQuery
                    printfn "%s" result
                    0
            | "repl" ->
                eprintfn "code-sight REPL. Type JS queries, 'quit' to exit."
                eprintfn "  search(q,opts), refs(name,opts), grep(pattern,opts), modules(),"
                eprintfn "  files(p?), context(file), expand(id), neighborhood(id,opts),"
                eprintfn "  impact(type), imports(file), deps(pattern), similar(id,opts)"
                eprintfn ""
                engine <- QueryEngine.create index chunksRef.Value cfg.EmbeddingUrl cfg.IndexDir cfg.RepoRoot cfg.SrcDirs
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
                    engine <- QueryEngine.create index chunksRef.Value cfg.EmbeddingUrl cfg.IndexDir cfg.RepoRoot cfg.SrcDirs
                    let modulesCache= QueryEngine.eval engine "modules()"
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
                    eprintfn "Dispatching to mini-model..."
                    // Build UDF signatures for the AI tool description
                    let userFns = FunctionStore.load repo
                    let udfSigs =
                        if userFns.Length = 0 then ""
                        else
                            userFns
                            |> Array.truncate 15
                            |> Array.map (fun f ->
                                let ps = if f.Params.Length = 0 then "()" else sprintf "(%s)" (f.Params |> String.concat ",")
                                let desc = if f.Description <> "" then sprintf " — %s" f.Description else ""
                                sprintf "%s%s%s" f.Name ps desc)
                            |> String.concat "; "
                    let result = Intel.run engine playbooksDir query modulesCache udfSigs |> Async.AwaitTask |> Async.RunSynchronously
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



