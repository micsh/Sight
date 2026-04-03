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

    /// Find source chunk matching an index entry. Used by expand/grep/refs/neighborhood.
    let private findSource (chunks: CodeChunk[] option) (c: ChunkEntry) =
        chunks |> Option.bind (fun chs ->
            chs |> Array.tryFind (fun ch ->
                ch.FilePath = c.FilePath && ch.Name = c.Name && ch.StartLine = c.StartLine))

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
        | None -> [||]
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
        | None -> [||]
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

    let files (index: CodeIndex) (pattern: string) =
        index.Chunks |> Array.groupBy (fun c -> c.FilePath)
        |> Array.choose (fun (filePath, chunks) ->
            let fileName = Path.GetFileName(filePath)
            if String.IsNullOrEmpty(pattern) || fileName.Contains(pattern, StringComparison.OrdinalIgnoreCase) || filePath.Contains(pattern, StringComparison.OrdinalIgnoreCase) then
                let kinds = chunks |> Array.map (fun c -> c.Kind) |> Array.distinct |> Array.sort
                let imps = IndexStore.fileImports index fileName
                Some (mdict [ "file", box fileName; "path", box filePath; "chunks", box chunks.Length; "kinds", box (kinds |> String.concat ","); "imports", box imps.Length ])
            else None)
        |> Array.sortBy (fun d -> string d.["file"])

    // ── modules ──

    let modules (index: CodeIndex) =
        index.Chunks |> Array.groupBy (fun c ->
            let parts = c.FilePath.Replace("\\", "/").Split('/')
            let srcIdx = parts |> Array.tryFindIndex (fun p -> p = "src")
            match srcIdx with Some i when i + 1 < parts.Length -> parts.[i + 1] | _ -> "other")
        |> Array.sortBy fst
        |> Array.map (fun (proj, chunks) ->
            let fileNames = chunks |> Array.map (fun c -> Path.GetFileName c.FilePath) |> Array.distinct |> Array.sort
            let types = chunks |> Array.filter (fun c -> c.Kind = "type") |> Array.map (fun c -> c.Name) |> Array.distinct |> Array.truncate 8
            let topSummaries = chunks |> Array.filter (fun c -> c.Kind = "type" || c.Kind = "module") |> Array.truncate 3 |> Array.map (fun c -> sprintf "%s (%s)" c.Name c.Summary)
            mdict [ "module", box proj; "files", box fileNames.Length; "chunks", box chunks.Length; "fileList", box (fileNames |> String.concat ", "); "topTypes", box (types |> String.concat ", "); "summaries", box (topSummaries |> String.concat "; ") ])

    // ── refs ──

    let refs (index: CodeIndex) (session: QuerySession) (chunks: CodeChunk[] option) (name: string) (limit: int) =
        match chunks with
        | None -> [||]
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
        | None -> [||]
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