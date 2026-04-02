open System
open System.IO
open AITeam.CodeSight

let printUsage () =
    eprintfn "AITeam.CodeSight — code intelligence for any codebase"
    eprintfn ""
    eprintfn "Usage:"
    eprintfn "  code-sight index [--repo <path>]     Build/update index (incremental)"
    eprintfn "  code-sight modules [--repo <path>]   Show project map"
    eprintfn "  code-sight search <js> [--repo <path>]  Run a query"
    eprintfn "  code-sight repl [--repo <path>]      Interactive mode"
    eprintfn ""

let parseArgs (args: string[]) =
    let mutable repo = Environment.CurrentDirectory
    let mutable command = ""
    let mutable query = ""
    let mutable i = 0
    while i < args.Length do
        match args.[i] with
        | "--repo" when i + 1 < args.Length ->
            repo <- args.[i + 1]
            i <- i + 2
        | "index" | "modules" | "repl" ->
            command <- args.[i]
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
    repo, command, query

let runIndex (cfg: CodeSightConfig) =
    let hashesPath = Path.Combine(cfg.IndexDir, "hashes.json")
    Directory.CreateDirectory(cfg.IndexDir) |> ignore

    // Find source files, normalize to relative paths for consistent hashing
    let allFilesAbs = TreeSitterChunker.findSourceFiles cfg
    let toRel (f: string) = Path.GetRelativePath(cfg.RepoRoot, f).Replace("\\", "/")
    let allFilesRel = allFilesAbs |> Array.map toRel
    eprintfn "Found %d source files in %A" allFilesAbs.Length cfg.SrcDirs

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
        let newChunks = TreeSitterChunker.chunkFiles cfg changedAbs
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
        let imports = TreeSitterChunker.extractImports cfg allFilesAbs |> Array.map (fun i -> i.FilePath, i.Module)
        eprintfn "  %d import edges" imports.Length

        eprintfn "▶ Extracting signatures..."
        let signatures = TreeSitterChunker.extractSignatures cfg allFilesAbs
        eprintfn "  %d signatures" signatures.Length

        eprintfn "▶ Extracting type refs..."
        let typeRefs = TreeSitterChunker.extractTypeRefs cfg allFilesAbs |> Array.map (fun r -> r.FilePath, r.TypeRefs)
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
    let repo, command, query = parseArgs args

    match command with
    | "index" ->
        let cfg = Config.load repo
        runIndex cfg
        0
    | "modules" | "search" | "repl" ->
        eprintfn "Command '%s' requires Stage 2 (query primitives). Use the FSX CLI for now." command
        1
    | "" ->
        printUsage()
        0
    | other ->
        eprintfn "Unknown command: %s" other
        printUsage()
        1

