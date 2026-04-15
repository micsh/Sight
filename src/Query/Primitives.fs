namespace AITeam.CodeSight

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions

/// Ref tracking for expand/neighborhood across queries.
/// Persists to .code-intel/refs.json for cross-call ref survival.
type QuerySession(indexDir: string) =
    let refsPath = Path.Combine(indexDir, "refs.json")
    let refs = Dictionary<string, int>()
    let mutable counter = 0

    do  // Load persisted refs
        if File.Exists refsPath then
            try
                let json = File.ReadAllText(refsPath)
                let doc = System.Text.Json.JsonDocument.Parse(json)
                for prop in doc.RootElement.EnumerateObject() do
                    refs.[prop.Name] <- prop.Value.GetInt32()
                    let num = prop.Name.Substring(1) |> int
                    if num > counter then counter <- num
            with _ -> ()

    member _.NextRef(chunkIdx) =
        counter <- counter + 1
        let id = sprintf "R%d" counter
        refs.[id] <- chunkIdx
        // Persist
        try
            let dict = Dictionary<string, int>()
            for kv in refs do dict.[kv.Key] <- kv.Value
            let json = System.Text.Json.JsonSerializer.Serialize(dict)
            File.WriteAllText(refsPath, json)
        with _ -> ()
        id

    member _.GetRef(id: string) =
        match refs.TryGetValue(id) with true, v -> Some v | _ -> None

    member _.SaveSession(name: string) =
        let sessDir = Path.Combine(indexDir, "sessions")
        Directory.CreateDirectory(sessDir) |> ignore
        let dict = Dictionary<string, int>()
        for kv in refs do dict.[kv.Key] <- kv.Value
        let json = System.Text.Json.JsonSerializer.Serialize(dict)
        File.WriteAllText(Path.Combine(sessDir, name + ".json"), json)

    member _.LoadSession(name: string) =
        let sessPath = Path.Combine(indexDir, "sessions", name + ".json")
        if File.Exists sessPath then
            let json = File.ReadAllText(sessPath)
            let doc = System.Text.Json.JsonDocument.Parse(json)
            refs.Clear()
            counter <- 0
            for prop in doc.RootElement.EnumerateObject() do
                refs.[prop.Name] <- prop.Value.GetInt32()
                let num = prop.Name.Substring(1) |> int
                if num > counter then counter <- num
            true
        else false

    member _.ListSessions() =
        let sessDir = Path.Combine(indexDir, "sessions")
        if Directory.Exists sessDir then
            Directory.GetFiles(sessDir, "*.json")
            |> Array.map (fun f -> Path.GetFileNameWithoutExtension(f))
        else [||]

    member _.RefCount = refs.Count

/// Mutable dictionary builder — Jint needs writable dictionaries.
[<AutoOpen>]
module DictHelper =
    let mdict (pairs: (string * obj) list) =
        let d = Dictionary<string, obj>()
        for (k, v) in pairs do d.[k] <- v
        d

/// All 12 query primitives. Each returns Dictionary/Array that Jint can consume.
module Primitives =

    let private embedQuery (url: string) (query: string) =
        EmbeddingService.embed url [| query |]
        |> Async.AwaitTask |> Async.RunSynchronously
        |> Option.map (fun e -> e.[0])

    /// First meaningful code line (skip blanks, comments, attributes).
    let private previewLine (content: string) =
        content.Split('\n')
        |> Array.tryFind (fun l ->
            let t = l.Trim()
            t.Length > 3 && not (t.StartsWith("//")) && not (t.StartsWith("(*"))
            && not (t.StartsWith("[<")) && not (t.StartsWith("#")))
        |> Option.map (fun l -> l.Trim())
        |> Option.defaultValue ""

    /// Normalize path separators for cross-platform comparison.
    let private normPath (p: string) = p.Replace('\\', '/')

    /// Find source chunk matching an index entry. Uses stable chunk ID as primary key.
    let private findSource (chunks: CodeChunk[] option) (c: ChunkEntry) =
        chunks |> Option.bind (fun chs ->
            let targetCid = IndexStore.chunkId c.FilePath c.Name c.StartLine
            // Primary: match by stable chunk ID
            match chs |> Array.tryFind (fun ch -> IndexStore.chunkId ch.FilePath ch.Name ch.StartLine = targetCid) with
            | Some _ as hit -> hit
            | None ->
                // Fallback: normalized path + name + line (handles pre-CID caches)
                chs |> Array.tryFind (fun ch ->
                    normPath ch.FilePath = normPath c.FilePath && ch.Name = c.Name && ch.StartLine = c.StartLine))

    // ── search ──

    let search (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (embeddingUrl: string)
               (query: string) (limit: int) (kind: string) (filePattern: string) =
        match embedQuery embeddingUrl query with
        | None -> [||]
        | Some qEmb ->
            IndexStore.search index qEmb (limit * 3)
            |> Array.filter (fun (i, _) ->
                let c = index.Chunks.[i]
                (String.IsNullOrEmpty(kind) || c.Kind = kind) &&
                (String.IsNullOrEmpty(filePattern) || c.FilePath.Contains(filePattern, StringComparison.OrdinalIgnoreCase)))
            |> Array.truncate limit
            |> Array.map (fun (i, sim) ->
                let c = index.Chunks.[i]
                let id = session.NextRef(i)
                let preview = findSource chunks c |> Option.map (fun ch -> previewLine ch.Content) |> Option.defaultValue ""
                let d = mdict [ "id", box id; "score", box (Math.Round(float sim, 3)); "kind", box c.Kind; "name", box c.Name; "file", box (Path.GetFileName c.FilePath); "path", box c.FilePath; "line", box c.StartLine; "signature", box c.Signature; "summary", box c.Summary; "preview", box preview ]
                for kv in c.Extra do d.[kv.Key] <- box kv.Value
                d)

    // ── context ──

    let context (index: CodeIndex) (session: QuerySession) (fileName: string) =
        // Detect ambiguous filename matches
        let matchingFiles =
            index.Chunks |> Array.map (fun c -> c.FilePath)
            |> Array.distinct
            |> Array.filter (fun fp -> IndexStore.matchFile fp fileName)
        if matchingFiles.Length > 1 then
            let listing = matchingFiles |> Array.map (fun f -> sprintf "  %s" f) |> String.concat "\n"
            mdict [ "error", box (sprintf "'%s' is ambiguous (%d matches):\n%s\nUse a more specific path, e.g. context('%s')" fileName matchingFiles.Length listing matchingFiles.[0]) ]
        else
        let chunks = IndexStore.fileContextByName index fileName
        let imps = IndexStore.fileImports index fileName
        let baseName = Path.GetFileNameWithoutExtension(fileName).ToLowerInvariant()
        let depFiles = IndexStore.dependents index baseName
        mdict [
            "file", box fileName
            "chunks", box (chunks |> Array.map (fun (i, c) ->
                let id = session.NextRef(i)
                let d = mdict [ "id", box id; "kind", box c.Kind; "name", box c.Name; "line", box c.StartLine; "endLine", box c.EndLine; "signature", box c.Signature; "summary", box c.Summary ]
                for kv in c.Extra do d.[kv.Key] <- box kv.Value
                d))
            "imports", box imps
            "dependents", box (depFiles |> Array.map Path.GetFileName)
        ]

    // ── impact ──

    let impact (index: CodeIndex) (session: QuerySession) (typeName: string) =
        let files = IndexStore.typeImpact index typeName
        files |> Array.map (fun f ->
            let fileChunks = IndexStore.fileContextByName index (Path.GetFileName f)
            let relevant = fileChunks |> Array.filter (fun (_, c) -> c.Summary.Contains(typeName, StringComparison.OrdinalIgnoreCase) || c.Name.Contains(typeName, StringComparison.OrdinalIgnoreCase))
            if relevant.Length > 0 then
                let id = session.NextRef(fst relevant.[0])
                let c = index.Chunks.[fst relevant.[0]]
                mdict [ "file", box (Path.GetFileName f); "id", box id; "summary", box c.Summary ]
            else mdict [ "file", box (Path.GetFileName f); "id", box ""; "summary", box "" ])

    // ── imports / deps ──

    let imports (index: CodeIndex) (fileName: string) = IndexStore.fileImports index fileName
    let deps (index: CodeIndex) (modulePattern: string) = IndexStore.dependents index modulePattern |> Array.map Path.GetFileName

    // ── expand ──

    let expand (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (refId: string) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found" refId) ]
        | Some chunkIdx ->
            let c = index.Chunks.[chunkIdx]
            let code = findSource chunks c |> Option.map (fun ch -> ch.Content) |> Option.defaultValue "(source not loaded)"
            let imps = IndexStore.fileImports index (Path.GetFileName c.FilePath)
            let baseName = Path.GetFileNameWithoutExtension(c.FilePath).ToLowerInvariant()
            let depFiles = IndexStore.dependents index baseName
            let d = mdict [ "id", box refId; "kind", box c.Kind; "name", box c.Name; "file", box (Path.GetFileName c.FilePath); "line", box c.StartLine; "endLine", box c.EndLine; "signature", box c.Signature; "summary", box c.Summary; "code", box code; "imports", box imps; "dependents", box (depFiles |> Array.map Path.GetFileName) ]
            for kv in c.Extra do d.[kv.Key] <- box kv.Value
            d

    // ── neighborhood ──

    let neighborhood (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (refId: string) (beforeCount: int) (afterCount: int) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found" refId) ]
        | Some chunkIdx ->
            let target = index.Chunks.[chunkIdx]
            let fileChunks = index.Chunks |> Array.indexed |> Array.filter (fun (_, c) -> c.FilePath = target.FilePath) |> Array.sortBy (fun (_, c) -> c.StartLine)
            let targetPos = fileChunks |> Array.tryFindIndex (fun (i, _) -> i = chunkIdx) |> Option.defaultValue 0
            let before' = min beforeCount 5
            let after' = min afterCount 5
            let mkCompact (i, c: ChunkEntry) =
                let id = session.NextRef(i)
                let preview = findSource chunks c |> Option.map (fun ch -> ch.Content.Split('\n').[0].Trim()) |> Option.defaultValue ""
                mdict [ "id", box id; "kind", box c.Kind; "name", box c.Name; "line", box c.StartLine; "endLine", box c.EndLine; "signature", box c.Signature; "summary", box c.Summary; "preview", box preview ]
            let beforeChunks = fileChunks.[max 0 (targetPos - before') .. max 0 (targetPos - 1)] |> Array.map mkCompact
            let afterChunks = fileChunks.[min (fileChunks.Length - 1) (targetPos + 1) .. min (fileChunks.Length - 1) (targetPos + after')] |> Array.filter (fun (i, _) -> i <> chunkIdx) |> Array.map mkCompact
            let targetCode = findSource chunks target |> Option.map (fun ch -> ch.Content) |> Option.defaultValue "(source not loaded)"
            let imps = IndexStore.fileImports index (Path.GetFileName target.FilePath)
            mdict [ "file", box (Path.GetFileName target.FilePath); "imports", box imps; "before", box beforeChunks; "target", box (mdict [ "id", box refId; "kind", box target.Kind; "name", box target.Name; "line", box target.StartLine; "endLine", box target.EndLine; "signature", box target.Signature; "summary", box target.Summary; "code", box targetCode ]); "after", box afterChunks ]

    // ── similar ──

    let similar (index: CodeIndex) (session: QuerySession) (refId: string) (limit: int) =
        match session.GetRef(refId) with
        | None -> [| mdict [ "error", box (sprintf "ref %s not found" refId) ] |]
        | Some chunkIdx ->
            IndexStore.similar index chunkIdx limit true
            |> Array.map (fun (i, sim) ->
                let c = index.Chunks.[i]
                let id = session.NextRef(i)
                let d = mdict [ "id", box id; "score", box (Math.Round(float sim, 3)); "kind", box c.Kind; "name", box c.Name; "file", box (Path.GetFileName c.FilePath); "line", box c.StartLine; "signature", box c.Signature; "summary", box c.Summary ]
                for kv in c.Extra do d.[kv.Key] <- box kv.Value
                d)

    // ── grep ──

    let grep (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (pattern: string) (limit: int) (kind: string) (filePattern: string) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'code-sight index' first" ] |]
        | Some allChunks ->
            let regex = try Regex(pattern, RegexOptions.IgnoreCase ||| RegexOptions.Compiled) with _ -> Regex(Regex.Escape(pattern), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    if (String.IsNullOrEmpty(kind) || c.Kind = kind) && (String.IsNullOrEmpty(filePattern) || c.FilePath.Contains(filePattern, StringComparison.OrdinalIgnoreCase)) then
                        match findSource (Some allChunks) c with
                        | Some ch when regex.IsMatch(ch.Content) ->
                            let matchLine = ch.Content.Split('\n') |> Array.tryFind (fun l -> regex.IsMatch(l)) |> Option.map (fun l -> l.Trim()) |> Option.defaultValue ""
                            let id = session.NextRef(i)
                            let d = mdict [ "id", box id; "kind", box c.Kind; "name", box c.Name; "file", box (Path.GetFileName c.FilePath); "path", box c.FilePath; "line", box c.StartLine; "matchLine", box matchLine; "signature", box c.Signature; "summary", box c.Summary ]
                            for kv in c.Extra do d.[kv.Key] <- box kv.Value
                            results.Add(d)
                        | _ -> ()
            results.ToArray()

    // ── files ──

    let private matchesPattern (pattern: string) (value: string) =
        if String.IsNullOrEmpty(pattern) then true
        elif pattern.Contains('*') || pattern.Contains('?') then
            let escaped = Regex.Escape(pattern).Replace(@"\*", ".*").Replace(@"\?", ".")
            Regex.IsMatch(value, sprintf "^%s$" escaped, RegexOptions.IgnoreCase)
        else
            value.Contains(pattern, StringComparison.OrdinalIgnoreCase)

    let files (index: CodeIndex) (pattern: string) =
        let isGlob = not (String.IsNullOrEmpty pattern) && (pattern.Contains('*') || pattern.Contains('?'))
        let matchPath = not (String.IsNullOrEmpty pattern) && not isGlob
        index.Chunks |> Array.groupBy (fun c -> c.FilePath)
        |> Array.choose (fun (filePath, chunks) ->
            let fileName = Path.GetFileName(filePath)
            // Glob patterns match filename only; plain substrings also match full path
            if matchesPattern pattern fileName || (matchPath && matchesPattern pattern filePath) then
                let kinds = chunks |> Array.map (fun c -> c.Kind) |> Array.distinct |> Array.sort
                let imps = IndexStore.fileImports index fileName
                Some (mdict [ "file", box fileName; "path", box filePath; "chunks", box chunks.Length; "kinds", box (kinds |> String.concat ","); "imports", box imps.Length ])
            else None)
        |> Array.sortBy (fun d -> string d.["file"])

    // ── modules ──

    let modules (index: CodeIndex) (srcDirs: string[]) =
        let srcSet = srcDirs |> Array.map (fun s -> s.ToLowerInvariant()) |> Set.ofArray
        index.Chunks |> Array.groupBy (fun c ->
            let parts = c.FilePath.Replace("\\", "/").Split('/')
            // Find the first directory that matches a configured srcDir
            let srcIdx = parts |> Array.tryFindIndex (fun p -> srcSet.Contains(p.ToLowerInvariant()))
            match srcIdx with
            | Some i when i + 1 < parts.Length -> parts.[i + 1]
            | _ ->
                // Bucket by top-level directory or "other"
                if parts.Length >= 2 then
                    let top = parts.[0]
                    if top.ToLowerInvariant().Contains("test") then sprintf "tests/%s" top
                    else top
                else "other")
        |> Array.sortBy fst
        |> Array.map (fun (proj, chunks) ->
            let fileNames = chunks |> Array.map (fun c -> Path.GetFileName c.FilePath) |> Array.distinct |> Array.sort
            let types = chunks |> Array.filter (fun c -> c.Kind = "type") |> Array.map (fun c -> c.Name) |> Array.distinct |> Array.truncate 8
            let topSummaries = chunks |> Array.filter (fun c -> c.Kind = "type" || c.Kind = "module") |> Array.truncate 3 |> Array.map (fun c -> sprintf "%s (%s)" c.Name c.Summary)
            mdict [ "module", box proj; "files", box fileNames.Length; "chunks", box chunks.Length; "fileList", box (fileNames |> String.concat ", "); "topTypes", box (types |> String.concat ", "); "summaries", box (topSummaries |> String.concat "; ") ])

    // ── refs ──

    let refs (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (name: string) (limit: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'code-sight index' first" ] |]
        | Some allChunks ->
            let regex = Regex(sprintf @"\b%s\b" (Regex.Escape name), RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    // Skip only the exact definition chunk (name matches exactly)
                    // Don't skip chunks that merely contain the name in their chunk name
                    if c.Name <> name then
                        match findSource (Some allChunks) c with
                        | Some ch when regex.IsMatch(ch.Content) ->
                            let matchLine = ch.Content.Split('\n') |> Array.tryFind (fun l -> regex.IsMatch(l)) |> Option.map (fun l -> l.Trim()) |> Option.defaultValue ""
                            let id = session.NextRef(i)
                            let d = mdict [ "id", box id; "kind", box c.Kind; "name", box c.Name; "file", box (Path.GetFileName c.FilePath); "line", box c.StartLine; "signature", box c.Signature; "summary", box c.Summary; "matchLine", box matchLine ]
                            for kv in c.Extra do d.[kv.Key] <- box kv.Value
                            results.Add(d)
                        | _ -> ()
            results.ToArray()

    // ── walk ──

    /// walk(name, {depth, limit}) — recursive reference tracing.
    /// Chains refs() calls: finds refs of name, then refs of those, up to depth hops.
    let walk (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (startName: string) (maxDepth: int) (limitPerHop: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'code-sight index' first" ] |]
        | Some allChunks ->
            let visited = HashSet<string>()
            let results = ResizeArray<Dictionary<string, obj>>()
            let maxResults = maxDepth * limitPerHop * 3

            let rec traceHop (name: string) (depth: int) (trail: string list) =
                if depth > maxDepth || visited.Contains(name) || results.Count >= maxResults then ()
                else
                    visited.Add(name) |> ignore
                    // Reuse refs logic directly
                    let regex = Regex(sprintf @"\b%s\b" (Regex.Escape name), RegexOptions.Compiled)
                    let mutable hitCount = 0
                    for i in 0..index.Chunks.Length-1 do
                        if hitCount < limitPerHop && results.Count < maxResults then
                            let c = index.Chunks.[i]
                            if c.Name <> name then
                                match findSource (Some allChunks) c with
                                | Some ch when regex.IsMatch(ch.Content) ->
                                    let matchLine = ch.Content.Split('\n') |> Array.tryFind (fun l -> regex.IsMatch(l)) |> Option.map (fun l -> l.Trim()) |> Option.defaultValue ""
                                    let currentTrail = trail @ [sprintf "%s (%s:%d)" c.Name (Path.GetFileName c.FilePath) c.StartLine]
                                    let id = session.NextRef(i)
                                    results.Add(mdict [
                                        "id", box id; "hop", box depth; "name", box c.Name
                                        "file", box (Path.GetFileName c.FilePath); "line", box c.StartLine
                                        "matchLine", box matchLine; "trail", box (currentTrail |> String.concat " → ")
                                    ])
                                    hitCount <- hitCount + 1
                                    // Next hop: use the chunk's short name
                                    let nextName = c.Name.Split('.') |> Array.last
                                    if nextName.Length > 2 && not (nextName.Contains("_part")) then
                                        traceHop nextName (depth + 1) currentTrail
                                | _ -> ()

            traceHop startName 1 [startName]
            results.ToArray()

    // ── callers ──

    /// callers(qualifiedName, {limit}) — find call sites of a qualified name like "Parser.parseAll".
    /// Unlike refs() which matches bare tokens, callers() searches for the full qualified pattern
    /// and also matches unqualified uses within the defining module.
    let callers (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (qualifiedName: string) (limit: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'code-sight index' first" ] |]
        | Some allChunks ->
            let parts = qualifiedName.Split('.')
            let shortName = parts |> Array.last
            let moduleName = if parts.Length > 1 then parts.[0..parts.Length-2] |> String.concat "." else ""
            let qualifiedRegex = Regex(sprintf @"\b%s\b" (Regex.Escape qualifiedName), RegexOptions.Compiled)
            let bareRegex = Regex(sprintf @"\b%s\b" (Regex.Escape shortName), RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    if c.Name = shortName || c.Name = qualifiedName then ()
                    else
                        match findSource (Some allChunks) c with
                        | Some ch ->
                            let isQualified = qualifiedRegex.IsMatch(ch.Content)
                            let isBareInModule = moduleName <> "" && c.Module = moduleName && bareRegex.IsMatch(ch.Content)
                            if isQualified || isBareInModule then
                                let matchLines =
                                    ch.Content.Split('\n')
                                    |> Array.filter (fun l -> qualifiedRegex.IsMatch(l) || (isBareInModule && bareRegex.IsMatch(l)))
                                    |> Array.map (fun l -> l.Trim())
                                    |> Array.truncate 3
                                let id = session.NextRef(i)
                                let callType = if isQualified then "qualified" else "local"
                                let d = mdict [
                                    "id", box id; "kind", box c.Kind; "name", box c.Name
                                    "file", box (Path.GetFileName c.FilePath); "path", box c.FilePath
                                    "line", box c.StartLine; "callType", box callType
                                    "matchLine", box (matchLines |> String.concat " | ")
                                    "signature", box c.Signature; "summary", box c.Summary ]
                                for kv in c.Extra do d.[kv.Key] <- box kv.Value
                                results.Add(d)
                        | None -> ()
            results.ToArray()

    // ── changed ──

    /// changed(gitRef) — find chunks in files that changed since a git ref (branch, tag, or SHA).
    let changed (index: CodeIndex) (session: QuerySession) (repoRoot: string) (gitRef: string) =
        try
            let psi = System.Diagnostics.ProcessStartInfo("git", sprintf "diff --name-only %s" gitRef)
            psi.WorkingDirectory <- repoRoot
            psi.RedirectStandardOutput <- true
            psi.RedirectStandardError <- true
            psi.UseShellExecute <- false
            psi.CreateNoWindow <- true
            use proc = System.Diagnostics.Process.Start(psi)
            let output = proc.StandardOutput.ReadToEnd()
            proc.WaitForExit()
            if proc.ExitCode <> 0 then
                [| mdict [ "error", box (sprintf "git diff failed for ref '%s'" gitRef) ] |]
            else
                let changedFiles =
                    output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                    |> Array.map (fun f -> f.Trim().Replace('\\', '/'))
                    |> Set.ofArray
                let results = ResizeArray()
                for i in 0..index.Chunks.Length-1 do
                    let c = index.Chunks.[i]
                    let normFile = c.FilePath.Replace('\\', '/')
                    if changedFiles.Contains(normFile) then
                        let id = session.NextRef(i)
                        let d = mdict [
                            "id", box id; "kind", box c.Kind; "name", box c.Name
                            "file", box (Path.GetFileName c.FilePath); "path", box c.FilePath
                            "line", box c.StartLine; "signature", box c.Signature; "summary", box c.Summary ]
                        for kv in c.Extra do d.[kv.Key] <- box kv.Value
                        results.Add(d)
                results.ToArray()
        with ex ->
            [| mdict [ "error", box (sprintf "git not available: %s" ex.Message) ] |]

    // ── hotspots ──

    /// hotspots({by, min}) — structural metrics per file: chunks, LOC, fanIn, fanOut, kinds.
    let hotspots (index: CodeIndex) (sortBy: string) (minChunks: int) =
        // Compute fanOut per file (how many modules this file imports)
        let fanOutMap =
            index.Imports |> Array.groupBy fst
            |> Array.map (fun (f, imps) -> Path.GetFileName(f), imps.Length) |> dict
        // Compute fanIn per file (how many files import this file's module)
        let fanInMap =
            index.Imports |> Array.groupBy snd
            |> Array.map (fun (m, importers) -> m, importers.Length) |> dict
        // Group chunks by file
        index.Chunks |> Array.groupBy (fun c -> c.FilePath)
        |> Array.choose (fun (filePath, chunks) ->
            if chunks.Length < minChunks then None
            else
                let fileName = Path.GetFileName(filePath)
                let loc = chunks |> Array.sumBy (fun c -> c.EndLine - c.StartLine + 1)
                let kinds = chunks |> Array.map (fun c -> c.Kind) |> Array.distinct |> Array.sort
                let moduleName = chunks.[0].Module
                let fanOut = match fanOutMap.TryGetValue(fileName) with true, v -> v | _ -> 0
                let fanIn = match fanInMap.TryGetValue(moduleName) with true, v -> v | _ -> 0
                Some (mdict [
                    "file", box fileName; "path", box filePath; "chunks", box chunks.Length
                    "loc", box loc; "fanIn", box fanIn; "fanOut", box fanOut
                    "kinds", box (kinds |> String.concat ",") ]))
        |> Array.sortByDescending (fun d ->
            match sortBy with
            | "loc" -> d.["loc"] :?> int
            | "fanIn" -> d.["fanIn"] :?> int
            | "fanOut" -> d.["fanOut"] :?> int
            | _ -> d.["chunks"] :?> int)

    // ── explain ──

    /// explain(refId) — debug primitive showing index metadata and findSource diagnosis.
    let explain (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (refId: string) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found in session" refId) ]
        | Some idx when idx < 0 || idx >= index.Chunks.Length ->
            mdict [ "error", box (sprintf "ref %s points to chunk %d but index has %d chunks" refId idx index.Chunks.Length) ]
        | Some idx ->
            let c = index.Chunks.[idx]
            let cid = IndexStore.chunkId c.FilePath c.Name c.StartLine
            let sourceMatch =
                match chunks with
                | None -> "source chunks not loaded"
                | Some chs ->
                    // Try CID match
                    let cidMatch = chs |> Array.tryFind (fun ch ->
                        IndexStore.chunkId ch.FilePath ch.Name ch.StartLine = cid)
                    match cidMatch with
                    | Some ch -> sprintf "CID match (%s), content length: %d" cid ch.Content.Length
                    | None ->
                        // Try triple-key fallback
                        let tripleMatch = chs |> Array.tryFind (fun ch ->
                            ch.FilePath = c.FilePath && ch.Name = c.Name && ch.StartLine = c.StartLine)
                        match tripleMatch with
                        | Some ch -> sprintf "triple-key match (no CID), content length: %d" ch.Content.Length
                        | None ->
                            let normPath (p: string) = p.Replace('\\', '/')
                            let pathMatch = chs |> Array.tryFind (fun ch -> normPath ch.FilePath = normPath c.FilePath && ch.Name = c.Name)
                            match pathMatch with
                            | Some ch -> sprintf "partial match (name+path, line differs: source=%d vs index=%d), content length: %d" ch.StartLine c.StartLine ch.Content.Length
                            | None -> sprintf "NO MATCH — findSource will return None. CID=%s, FilePath=%s, Name=%s, StartLine=%d" cid c.FilePath c.Name c.StartLine
            let d = mdict [
                "refId", box refId; "chunkIdx", box idx; "cid", box cid
                "filePath", box c.FilePath; "module", box c.Module; "name", box c.Name
                "kind", box c.Kind; "startLine", box c.StartLine; "endLine", box c.EndLine
                "summary", box c.Summary; "signature", box c.Signature
                "sourceMatch", box sourceMatch ]
            for kv in c.Extra do d.[kv.Key] <- box kv.Value
            d

    // ── trace ──

    /// trace(from, to) — BFS shortest path between two files/modules over the import graph.
    let trace (index: CodeIndex) (fromName: string) (toName: string) =
        let resolveFile (name: string) =
            let files = index.Chunks |> Array.map (fun c -> Path.GetFileName c.FilePath) |> Array.distinct
            files |> Array.tryFind (fun f -> f.Contains(name, StringComparison.OrdinalIgnoreCase))
            |> Option.orElseWith (fun () ->
                files |> Array.tryFind (fun f -> Path.GetFileNameWithoutExtension(f).Equals(name, StringComparison.OrdinalIgnoreCase)))

        match resolveFile fromName, resolveFile toName with
        | None, _ -> [| mdict [ "error", box (sprintf "could not resolve '%s' to a file" fromName) ] |]
        | _, None -> [| mdict [ "error", box (sprintf "could not resolve '%s' to a file" toName) ] |]
        | Some startFile, Some endFile when startFile = endFile ->
            [| mdict [ "path", box [| startFile |]; "length", box 0 ] |]
        | Some startFile, Some endFile ->
            // Build file→file adjacency from imports
            // An import like "crate::broker::delivery::..." or "System.IO" is matched to any file
            // whose name (sans extension) appears as a segment in the import path
            let allFiles = index.Chunks |> Array.map (fun c -> Path.GetFileName c.FilePath) |> Array.distinct
            let fileStems = allFiles |> Array.map (fun f -> f, Path.GetFileNameWithoutExtension(f).ToLowerInvariant()) |> dict
            let adj = Dictionary<string, HashSet<string>>()
            for f in allFiles do adj.[f] <- HashSet()
            for (filePath, importedModule) in index.Imports do
                let src = Path.GetFileName filePath
                let mLower = importedModule.ToLowerInvariant()
                // Match import against file stems: "delivery" in "crate::broker::delivery::DeliveryEngine"
                for kv in fileStems do
                    if kv.Key <> src && mLower.Contains(kv.Value) && kv.Value.Length >= 3 then
                        if adj.ContainsKey src then adj.[src].Add(kv.Key) |> ignore

            // BFS (bidirectional edges: if A imports B, both A→B and B→A are traversable)
            let visited = HashSet<string>()
            let parent = Dictionary<string, string>()
            let queue = Queue<string>()
            queue.Enqueue(startFile)
            visited.Add(startFile) |> ignore
            let mutable found = false
            while queue.Count > 0 && not found do
                let current = queue.Dequeue()
                let neighbors = ResizeArray<string>()
                // Forward edges
                if adj.ContainsKey current then
                    for n in adj.[current] do if not (visited.Contains n) then neighbors.Add(n)
                // Reverse edges
                for kv in adj do
                    if kv.Value.Contains(current) && not (visited.Contains kv.Key) then
                        neighbors.Add(kv.Key)
                for next in neighbors |> Seq.distinct do
                    if not (visited.Contains next) then
                        visited.Add(next) |> ignore
                        parent.[next] <- current
                        if next = endFile then found <- true
                        queue.Enqueue(next)
            if not found then
                [| mdict [ "error", box (sprintf "no path from '%s' to '%s'" startFile endFile) ] |]
            else
                let path = ResizeArray<string>()
                let mutable cur = endFile
                while cur <> startFile do
                    path.Add(cur)
                    cur <- parent.[cur]
                path.Add(startFile)
                path.Reverse()
                [| mdict [ "path", box (path.ToArray()); "length", box (path.Count - 1) ] |]

    // ── arch ──

    /// arch(file) — get architectural context from the arch tool. Graceful fallback if unavailable.
    let arch (repoRoot: string) (filePath: string) =
        try
            let psi = System.Diagnostics.ProcessStartInfo("arch", sprintf "context --file %s --json" filePath)
            psi.WorkingDirectory <- repoRoot
            psi.RedirectStandardOutput <- true
            psi.RedirectStandardError <- true
            psi.UseShellExecute <- false
            psi.CreateNoWindow <- true
            use proc = System.Diagnostics.Process.Start(psi)
            let output = proc.StandardOutput.ReadToEnd()
            proc.WaitForExit()
            if proc.ExitCode <> 0 then
                // Check if it's "not found" vs actual error
                if output.Contains("not found") then
                    mdict [ "available", box false; "reason", box "arch not initialized for this repo (run 'arch init')" ]
                else
                    mdict [ "error", box (sprintf "arch exited with code %d" proc.ExitCode) ]
            else
                try
                    let doc = System.Text.Json.JsonDocument.Parse(output)
                    let root = doc.RootElement
                    // Check for error field
                    match root.TryGetProperty("error") with
                    | true, errVal -> mdict [ "available", box false; "reason", box (errVal.GetString()) ]
                    | _ ->
                        let str (p: string) = match root.TryGetProperty(p) with true, v when v.ValueKind <> System.Text.Json.JsonValueKind.Null -> v.GetString() | _ -> ""
                        let arr (p: string) =
                            match root.TryGetProperty(p) with
                            | true, v when v.ValueKind = System.Text.Json.JsonValueKind.Array ->
                                [| for item in v.EnumerateArray() -> item.GetString() |] |> Array.filter (fun s -> s <> null && s <> "")
                            | _ -> [||]
                        let ownerModule = str "owner_module"
                        let boundary = str "boundary"
                        let dependsOn = arr "depends_on"
                        let usedBy = arr "used_by"
                        let peerModules = arr "peer_modules"
                        let d = mdict [
                            "available", box true; "file", box filePath
                            "ownerModule", box ownerModule; "boundary", box boundary
                            "dependsOn", box dependsOn; "usedBy", box usedBy
                            "peerModules", box peerModules ]
                        d
                with _ ->
                    mdict [ "error", box "failed to parse arch output" ]
        with
        | :? System.ComponentModel.Win32Exception ->
            mdict [ "available", box false; "reason", box "arch tool not found on PATH" ]
        | ex ->
            mdict [ "available", box false; "reason", box ex.Message ]