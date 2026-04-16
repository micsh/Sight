namespace AITeam.KnowledgeSight

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open AITeam.Sight.Core

/// Ref tracking for expand/neighborhood across queries.
type QuerySession(indexDir: string) =
    let refsPath = Path.Combine(indexDir, "refs.json")
    let refs = Dictionary<string, int>()
    let mutable counter = 0

    do
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

/// All query primitives for knowledge/doc operations.
module Primitives =

    let private embedQuery (url: string) (query: string) =
        EmbeddingService.embed url [| sprintf "search_query: %s" query |]
        |> Async.AwaitTask |> Async.RunSynchronously
        |> Option.map (fun e -> e.[0])

    /// Normalize path separators for cross-platform comparison.
    let private normPath (p: string) = p.Replace('\\', '/')

    /// Find source chunk matching an index entry. Uses stable chunk ID as primary key.
    let private findSource (chunks: DocChunk[] option) (c: ChunkEntry) =
        chunks |> Option.bind (fun chs ->
            let targetCid = IndexStore.chunkId c.FilePath c.Heading c.StartLine
            match chs |> Array.tryFind (fun ch -> IndexStore.chunkId ch.FilePath ch.Heading ch.StartLine = targetCid) with
            | Some _ as hit -> hit
            | None ->
                chs |> Array.tryFind (fun ch ->
                    normPath ch.FilePath = normPath c.FilePath && ch.Heading = c.Heading && ch.StartLine = c.StartLine))

    // ── catalog (like modules in code-sight) ──

    let catalog (index: DocIndex) =
        index.Chunks |> Array.groupBy (fun c ->
            let parts = c.FilePath.Replace("\\", "/").Split('/')
            // Group by first directory under repo (e.g., "knowledge", "design", "pocs")
            let dotsIdx = parts |> Array.tryFindIndex (fun p -> p.StartsWith("."))
            match dotsIdx with
            | Some i when i + 1 < parts.Length -> parts.[i] + "/" + parts.[i + 1]
            | Some i -> parts.[i]
            | None ->
                if parts.Length >= 2 then parts.[parts.Length - 2]
                else "root")
        |> Array.sortBy fst
        |> Array.map (fun (dir, chunks) ->
            let fileNames = chunks |> Array.map (fun c -> Path.GetFileName c.FilePath) |> Array.distinct |> Array.sort
            let allTags = chunks |> Array.collect (fun c -> c.Tags.Split(',') |> Array.filter ((<>) "")) |> Array.distinct |> Array.truncate 8
            let topTitles = chunks |> Array.filter (fun c -> c.Level <= 1) |> Array.truncate 3 |> Array.map (fun c -> c.Heading)
            mdict [ "directory", box dir; "docs", box fileNames.Length; "sections", box chunks.Length
                    "fileList", box (fileNames |> String.concat ", ")
                    "topTags", box (allTags |> String.concat ", ")
                    "titles", box (topTitles |> String.concat "; ") ])

    // ── search ──

    let search (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (embeddingUrl: string)
               (query: string) (limit: int) (tag: string) (filePattern: string) =
        match embedQuery embeddingUrl query with
        | None -> [| mdict [ "error", box "embedding server not available — search requires embeddings" ] |]
        | Some qEmb ->
            IndexStore.search index qEmb (limit * 3)
            |> Array.filter (fun (i, _) ->
                let c = index.Chunks.[i]
                (String.IsNullOrEmpty(tag) || c.Tags.Contains(tag, StringComparison.OrdinalIgnoreCase)) &&
                (String.IsNullOrEmpty(filePattern) || c.FilePath.Contains(filePattern, StringComparison.OrdinalIgnoreCase)))
            |> Array.truncate limit
            |> Array.map (fun (i, sim) ->
                let c = index.Chunks.[i]
                let id = session.NextRef(i)
                mdict [ "id", box id; "score", box (Math.Round(float sim, 3))
                        "heading", box c.Heading; "headingPath", box c.HeadingPath
                        "file", box (Path.GetFileName c.FilePath); "path", box c.FilePath
                        "line", box c.StartLine; "summary", box c.Summary
                        "tags", box c.Tags; "links", box c.LinkCount; "words", box c.WordCount ])

    // ── context ──

    let context (index: DocIndex) (session: QuerySession) (fileName: string) =
        // Detect ambiguous filename matches
        let matchingFiles =
            index.Chunks |> Array.map (fun c -> c.FilePath.Replace("\\", "/"))
            |> Array.distinct
            |> Array.filter (fun fp -> IndexStore.matchFile fp fileName)
        if matchingFiles.Length > 1 then
            let listing = matchingFiles |> Array.map (fun f -> sprintf "  %s" f) |> String.concat "\n"
            mdict [ "error", box (sprintf "'%s' is ambiguous (%d matches):\n%s\nUse a more specific path, e.g. context('%s')" fileName matchingFiles.Length listing matchingFiles.[0]) ]
        else
        let fileChunks = IndexStore.fileChunks index fileName
        let backlinks = IndexStore.backlinks index fileName
        let outlinks = IndexStore.outlinks index fileName
        let fm = index.Frontmatters |> Map.tryFind fileName
                 |> Option.orElseWith (fun () ->
                    index.Frontmatters |> Map.toSeq |> Seq.tryFind (fun (k, _) -> IndexStore.matchFile k fileName) |> Option.map snd)
        mdict [
            "file", box fileName
            "title", box (fm |> Option.map (fun f -> f.Title) |> Option.defaultValue "")
            "status", box (fm |> Option.map (fun f -> f.Status) |> Option.defaultValue "")
            "tags", box (fm |> Option.map (fun f -> f.Tags |> String.concat ", ") |> Option.defaultValue "")
            "related", box (fm |> Option.map (fun f -> f.Related |> String.concat ", ") |> Option.defaultValue "")
            "sections", box (fileChunks |> Array.map (fun (i, c) ->
                let id = session.NextRef(i)
                mdict [ "id", box id; "heading", box c.Heading; "level", box c.Level
                        "line", box c.StartLine; "summary", box c.Summary
                        "words", box c.WordCount; "links", box c.LinkCount ]))
            "backlinks", box (backlinks |> Array.map (fun l ->
                mdict [ "from", box (Path.GetFileName l.SourceFile); "section", box l.SourceHeading; "text", box l.LinkText ]))
            "outlinks", box (outlinks |> Array.map (fun l ->
                let resolved = if l.TargetResolved <> "" then Path.GetFileName l.TargetResolved else sprintf "⚠ %s" l.TargetPath
                mdict [ "to", box resolved; "text", box l.LinkText; "section", box l.SourceHeading ]))
        ]

    // ── expand ──

    let expand (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (refId: string) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found" refId) ]
        | Some chunkIdx ->
            let c = index.Chunks.[chunkIdx]
            let content = findSource chunks c |> Option.map (fun ch -> ch.Content) |> Option.defaultValue "(source not loaded)"
            let backlinks = IndexStore.backlinks index (Path.GetFileName c.FilePath)
            mdict [ "id", box refId; "heading", box c.Heading; "headingPath", box c.HeadingPath
                    "file", box (Path.GetFileName c.FilePath); "line", box c.StartLine
                    "endLine", box c.EndLine; "summary", box c.Summary
                    "tags", box c.Tags; "content", box content
                    "backlinks", box (backlinks |> Array.map (fun l -> sprintf "%s (%s)" (Path.GetFileName l.SourceFile) l.LinkText)) ]

    // ── neighborhood ──

    let neighborhood (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (refId: string) (beforeCount: int) (afterCount: int) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found" refId) ]
        | Some chunkIdx ->
            let target = index.Chunks.[chunkIdx]
            let fileChunks = index.Chunks |> Array.indexed |> Array.filter (fun (_, c) -> c.FilePath = target.FilePath) |> Array.sortBy (fun (_, c) -> c.StartLine)
            let targetPos = fileChunks |> Array.tryFindIndex (fun (i, _) -> i = chunkIdx) |> Option.defaultValue 0
            let mkCompact (i, c: ChunkEntry) =
                let id = session.NextRef(i)
                mdict [ "id", box id; "heading", box c.Heading; "level", box c.Level
                        "line", box c.StartLine; "summary", box c.Summary; "words", box c.WordCount ]
            let beforeChunks = fileChunks.[max 0 (targetPos - beforeCount) .. max 0 (targetPos - 1)] |> Array.map mkCompact
            let afterChunks = fileChunks.[min (fileChunks.Length - 1) (targetPos + 1) .. min (fileChunks.Length - 1) (targetPos + afterCount)] |> Array.filter (fun (i, _) -> i <> chunkIdx) |> Array.map mkCompact
            let targetContent = findSource chunks target |> Option.map (fun ch -> ch.Content) |> Option.defaultValue "(source not loaded)"
            mdict [ "file", box (Path.GetFileName target.FilePath)
                    "before", box beforeChunks
                    "target", box (mdict [ "id", box refId; "heading", box target.Heading; "level", box target.Level
                                           "line", box target.StartLine; "summary", box target.Summary; "content", box targetContent ])
                    "after", box afterChunks ]

    // ── similar ──

    let similar (index: DocIndex) (session: QuerySession) (refId: string) (limit: int) =
        match session.GetRef(refId) with
        | None -> [| mdict [ "error", box (sprintf "ref %s not found" refId) ] |]
        | Some chunkIdx ->
            IndexStore.similar index chunkIdx limit
            |> Array.map (fun (i, sim) ->
                let c = index.Chunks.[i]
                let id = session.NextRef(i)
                mdict [ "id", box id; "score", box (Math.Round(float sim, 3))
                        "heading", box c.Heading; "file", box (Path.GetFileName c.FilePath)
                        "line", box c.StartLine; "summary", box c.Summary; "tags", box c.Tags ])

    // ── grep ──

    let grep (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (pattern: string) (limit: int) (filePattern: string) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks ->
            let regex = try Regex(pattern, RegexOptions.IgnoreCase ||| RegexOptions.Compiled) with _ -> Regex(Regex.Escape(pattern), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    if String.IsNullOrEmpty(filePattern) || c.FilePath.Contains(filePattern, StringComparison.OrdinalIgnoreCase) then
                        match findSource (Some allChunks) c with
                        | Some ch when regex.IsMatch(ch.Content) ->
                            let matchLine = ch.Content.Split('\n') |> Array.tryFind (fun l -> regex.IsMatch(l)) |> Option.map (fun l -> l.Trim()) |> Option.defaultValue ""
                            let id = session.NextRef(i)
                            results.Add(mdict [ "id", box id; "heading", box c.Heading; "file", box (Path.GetFileName c.FilePath)
                                                "path", box c.FilePath; "line", box c.StartLine; "matchLine", box matchLine
                                                "summary", box c.Summary; "tags", box c.Tags ])
                        | _ -> ()
            results.ToArray()

    // ── mentions (like refs in code-sight) ──

    let mentions (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (term: string) (limit: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks ->
            let regex = Regex(sprintf @"\b%s\b" (Regex.Escape term), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    match findSource (Some allChunks) c with
                    | Some ch when regex.IsMatch(ch.Content) ->
                        let matchLine = ch.Content.Split('\n') |> Array.tryFind (fun l -> regex.IsMatch(l)) |> Option.map (fun l -> l.Trim()) |> Option.defaultValue ""
                        let count = regex.Matches(ch.Content).Count
                        let id = session.NextRef(i)
                        results.Add(mdict [ "id", box id; "heading", box c.Heading; "file", box (Path.GetFileName c.FilePath)
                                            "line", box c.StartLine; "matchLine", box matchLine; "count", box count
                                            "summary", box c.Summary; "tags", box c.Tags ])
                    | _ -> ()
            results.ToArray()

    // ── files ──

    let files (index: DocIndex) (pattern: string) =
        index.Chunks |> Array.groupBy (fun c -> c.FilePath)
        |> Array.choose (fun (filePath, chunks) ->
            let fileName = Path.GetFileName(filePath)
            if String.IsNullOrEmpty(pattern) || fileName.Contains(pattern, StringComparison.OrdinalIgnoreCase) || filePath.Contains(pattern, StringComparison.OrdinalIgnoreCase) then
                let fm = index.Frontmatters |> Map.tryFind filePath
                let title = fm |> Option.map (fun f -> f.Title) |> Option.defaultValue ""
                let tags = fm |> Option.map (fun f -> f.Tags |> String.concat ",") |> Option.defaultValue ""
                let backlinks = IndexStore.backlinks index filePath
                Some (mdict [ "file", box fileName; "path", box filePath; "sections", box chunks.Length
                              "title", box title; "tags", box tags; "backlinks", box backlinks.Length
                              "words", box (chunks |> Array.sumBy (fun c -> c.WordCount)) ])
            else None)
        |> Array.sortBy (fun d -> string d.["file"])

    // ── backlinks ──

    let backlinks (index: DocIndex) (session: QuerySession) (fileName: string) =
        IndexStore.backlinks index fileName
        |> Array.map (fun l ->
            mdict [ "from", box (Path.GetFileName l.SourceFile); "section", box l.SourceHeading
                    "text", box l.LinkText; "line", box l.Line
                    "resolved", box (if l.TargetResolved <> "" then "✓" else "✗") ])

    // ── links (outgoing) ──

    let links (index: DocIndex) (fileName: string) =
        IndexStore.outlinks index fileName
        |> Array.map (fun l ->
            let resolved = if l.TargetResolved <> "" then Path.GetFileName l.TargetResolved else sprintf "⚠ %s" l.TargetPath
            mdict [ "to", box resolved; "text", box l.LinkText; "section", box l.SourceHeading; "line", box l.Line ])

    // ── orphans — docs with no incoming links ──

    let orphans (index: DocIndex) =
        let allFiles = index.Chunks |> Array.map (fun c -> c.FilePath) |> Array.distinct
        let linkedFiles = index.Links |> Array.choose (fun l -> if l.TargetResolved <> "" then Some l.TargetResolved else None) |> Set.ofArray
        allFiles
        |> Array.filter (fun f -> not (linkedFiles.Contains f))
        |> Array.map (fun f ->
            let fm = index.Frontmatters |> Map.tryFind f
            let title = fm |> Option.map (fun f -> f.Title) |> Option.defaultValue ""
            let sections = index.Chunks |> Array.filter (fun c -> c.FilePath = f)
            mdict [ "file", box (Path.GetFileName f); "path", box f; "title", box title; "sections", box sections.Length ])

    // ── broken — links pointing to nonexistent docs ──

    let broken (index: DocIndex) =
        index.Links
        |> Array.filter (fun l -> l.TargetResolved = "")
        |> Array.map (fun l ->
            mdict [ "from", box (Path.GetFileName l.SourceFile); "target", box l.TargetPath
                    "text", box l.LinkText; "section", box l.SourceHeading; "line", box l.Line ])

    // ── placement — where should new content go? ──

    let placement (index: DocIndex) (embeddingUrl: string) (content: string) (limit: int) =
        match embedQuery embeddingUrl content with
        | None -> [| mdict [ "error", box "embedding server not available — placement requires embeddings" ] |]
        | Some qEmb ->
            // Find most similar sections, then group by file to suggest placement
            let hits = IndexStore.search index qEmb (limit * 3)
            let byFile =
                hits |> Array.groupBy (fun (i, _) -> index.Chunks.[i].FilePath)
                |> Array.map (fun (file, matches) ->
                    let avgScore = matches |> Array.averageBy (fun (_, s) -> float s)
                    let bestMatch = matches |> Array.maxBy snd
                    let bestChunk = index.Chunks.[fst bestMatch]
                    file, avgScore, bestChunk.Heading, bestChunk.HeadingPath)
                |> Array.sortByDescending (fun (_, score, _, _) -> score)
                |> Array.truncate limit
            byFile |> Array.map (fun (file, score, heading, headingPath) ->
                let fm = index.Frontmatters |> Map.tryFind file
                let title = fm |> Option.map (fun f -> f.Title) |> Option.defaultValue ""
                mdict [ "file", box (Path.GetFileName file); "score", box (Math.Round(score, 3))
                        "nearSection", box heading; "sectionPath", box headingPath; "title", box title ])

    // ── walk — traverse the link graph ──

    let walk (index: DocIndex) (session: QuerySession) (startFile: string) (maxDepth: int) (direction: string) =
        let visited = HashSet<string>()
        let results = ResizeArray<Dictionary<string, obj>>()

        let rec trace (file: string) (depth: int) (trail: string list) =
            if depth > maxDepth || visited.Contains(file) || results.Count >= maxDepth * 10 then ()
            else
                visited.Add(file) |> ignore
                let neighbors =
                    if direction = "in" then
                        IndexStore.backlinks index file |> Array.map (fun l -> l.SourceFile, l.LinkText)
                    else
                        IndexStore.outlinks index file |> Array.map (fun l -> l.TargetResolved, l.LinkText)
                    |> Array.filter (fun (f, _) -> f <> "" && not (visited.Contains f))
                    |> Array.distinctBy fst

                for (nextFile, linkText) in neighbors do
                    let nextTrail = trail @ [sprintf "%s (%s)" (Path.GetFileName nextFile) linkText]
                    results.Add(mdict [
                        "hop", box depth; "file", box (Path.GetFileName nextFile)
                        "path", box nextFile; "via", box linkText
                        "trail", box (nextTrail |> String.concat " → ")
                    ])
                    trace nextFile (depth + 1) nextTrail

        let startResolved =
            index.Chunks |> Array.tryFind (fun c -> IndexStore.matchFile c.FilePath startFile) |> Option.map (fun c -> c.FilePath) |> Option.defaultValue startFile
        trace startResolved 1 [Path.GetFileName startResolved]
        results.ToArray()

    // ── novelty — what's new in this text vs existing knowledge? ──

    /// Heuristic: does this paragraph look like knowledge vs casual musing?
    let private knowledgeSignal (para: string) (index: DocIndex) =
        let lower = para.ToLowerInvariant()
        let mutable score = 0

        // Prescriptive language (knowledge patterns)
        let prescriptive = [| " should "; " must "; " always "; " never "; " when "; " ensure "; " requires "; " depends on "; " means that " |]
        for p in prescriptive do if lower.Contains(p) then score <- score + 2

        // Causal connectors (reasoning)
        let causal = [| " because "; " therefore "; " so that "; " in order to "; " consequence "; " implies "; " leads to " |]
        for c in causal do if lower.Contains(c) then score <- score + 2

        // Declarative structure (definitions/facts)
        let declarative = [| " is a "; " are "; " defines "; " represents "; " consists of "; " handles "; " processes " |]
        for d in declarative do if lower.Contains(d) then score <- score + 1

        // Hedging / uncertainty (musing patterns — deduct)
        let hedging = [| " maybe "; " perhaps "; " i wonder "; " not sure "; " might "; " could be "; " i think "; "?" |]
        for h in hedging do if lower.Contains(h) then score <- score - 2

        // Concrete code references (file names, types from the index)
        let codeRefRegex = Regex(@"\b\w+\.(fs|cs|js|ts|py|md)\b", RegexOptions.Compiled)
        let codeRefs = codeRefRegex.Matches(para).Count
        score <- score + codeRefs * 2

        // Type/module names from the index
        let indexNames = index.Chunks |> Array.map (fun c -> c.Heading) |> Array.distinct
        let nameHits = indexNames |> Array.filter (fun name -> name.Length > 3 && para.Contains(name, StringComparison.OrdinalIgnoreCase))
        score <- score + nameHits.Length

        // Length bonus — very short paragraphs are rarely knowledge
        if para.Length < 50 then score <- score - 2

        score

    /// Split text into paragraphs, embed each, compare to index.
    /// Classifies each paragraph as: off-topic, musing, novel, or covered.
    let novelty (index: DocIndex) (embeddingUrl: string) (text: string) (threshold: float) =
        let paragraphs =
            text.Split([| "\n\n"; "\r\n\r\n" |], StringSplitOptions.RemoveEmptyEntries)
            |> Array.map (fun p -> p.Trim())
            |> Array.filter (fun p -> p.Length > 30 && not (p.StartsWith("```")) && not (p.StartsWith("|")))

        if paragraphs.Length = 0 then [||]
        else
            let prefixed = paragraphs |> Array.map (fun p -> sprintf "search_query: %s" (p.Substring(0, min 200 p.Length)))
            let embeddings =
                match EmbeddingService.embed embeddingUrl prefixed |> Async.AwaitTask |> Async.RunSynchronously with
                | Some embs -> embs
                | None -> [||]

            if embeddings.Length = 0 then [||]
            else
                paragraphs |> Array.mapi (fun i para ->
                    if i >= embeddings.Length || embeddings.[i].Length = 0 then
                        mdict [ "paragraph", box (para.Substring(0, min 80 para.Length) + "..."); "status", box "error"; "score", box 0.0 ]
                    else
                        let hits = IndexStore.search index embeddings.[i] 1
                        let bestScore, bestChunk =
                            if hits.Length > 0 then
                                let idx, sim = hits.[0]
                                float sim, Some index.Chunks.[idx]
                            else 0.0, None

                        let kSignal = knowledgeSignal para index
                        let status =
                            if bestScore < 0.5 then "off-topic"       // not in the project's semantic space
                            elif kSignal < 0 then "musing"            // in-space but reads like discussion, not knowledge
                            elif kSignal < 1 && bestScore < 0.6 then "musing" // weak signal + low relevance = not knowledge
                            elif bestScore >= threshold then "covered" // already captured
                            else "novel"                              // relevant, looks like knowledge, not yet captured

                        let preview = if para.Length > 120 then para.Substring(0, 120) + "..." else para
                        let nearDoc = bestChunk |> Option.map (fun c -> Path.GetFileName c.FilePath) |> Option.defaultValue ""
                        let nearHeading = bestChunk |> Option.map (fun c -> c.Heading) |> Option.defaultValue ""
                        mdict [ "paragraph", box preview; "status", box status
                                "score", box (Math.Round(bestScore, 3)); "signal", box kSignal
                                "nearDoc", box nearDoc; "nearSection", box nearHeading ])

    // ── gaps — cross-document entity coverage analysis ──

    /// Extract entity references from markdown text via regex.
    /// Returns normalized entity names: identifiers, file names, modules, paths.
    let private extractEntityRefs (text: string) =
        let refs = HashSet<string>(StringComparer.OrdinalIgnoreCase)

        // Backticked file names: `SearchTools.fs`, `config.yaml`
        for m in Regex.Matches(text, @"`([A-Za-z]\w+\.(?:fs|cs|fsx|js|ts|py|go|rs|yaml|json|md|toml))`") do
            refs.Add(m.Groups.[1].Value) |> ignore

        // Bare source file names in prose: SearchTools.fs, AppOrchestrator.cs
        for m in Regex.Matches(text, @"\b([A-Z][A-Za-z]+\.(?:fs|cs|js|ts|py))\b") do
            refs.Add(m.Groups.[1].Value) |> ignore

        // Module-style names: AITeam.Orchestration, System.IO
        for m in Regex.Matches(text, @"\b((?:[A-Z][A-Za-z]+\.){1,4}[A-Z][A-Za-z]+)\b") do
            let full = m.Groups.[1].Value
            if not (full.EndsWith(".fs") || full.EndsWith(".cs") || full.EndsWith(".js") || full.EndsWith(".md")) then
                refs.Add(full) |> ignore
                // Also add short form (strip common prefixes)
                for prefix in [| "AITeam."; "System."; "Microsoft." |] do
                    if full.StartsWith(prefix) then
                        refs.Add(full.Substring(prefix.Length)) |> ignore

        // Backticked CamelCase identifiers: `DeliveryEngine`, `ICapabilityResolver`
        for m in Regex.Matches(text, @"`([A-Z][A-Za-z]{2,}(?:\.[A-Z][A-Za-z]+)*)`") do
            let v = m.Groups.[1].Value
            if not (Regex.IsMatch(v, @"\.\w{1,4}$")) then // skip file extensions already caught
                refs.Add(v) |> ignore

        // Path references: `src/foo/Bar.fs`, `.agents/tools/thing.py`
        for m in Regex.Matches(text, @"`((?:\.agents|src|tests|architecture)/[^\s`]+)`") do
            refs.Add(Path.GetFileName(m.Groups.[1].Value)) |> ignore

        refs |> Seq.toArray

    /// Normalize an entity name for deduplication.
    /// Strips common prefixes and file extensions so `DeliveryEngine.fs` and `DeliveryEngine` merge.
    let private normalizeEntity (name: string) =
        let stripped =
            [| "AITeam."; "System."; "Microsoft." |]
            |> Array.fold (fun (s: string) prefix ->
                if s.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) then s.Substring(prefix.Length) else s) name
        // Strip known file extensions for dedup
        let noExt =
            Regex.Replace(stripped, @"\.(fs|cs|fsx|js|ts|py|go|rs|yaml|json|md|toml)$", "", RegexOptions.IgnoreCase)
        noExt.ToLowerInvariant()

    /// Cross-document gap analysis. Works on whatever's in the index.
    /// Groups chunks by source file, extracts entity refs, builds a bipartite
    /// entity→files index, classifies: shared / isolated / god-node.
    let gaps (index: DocIndex) (chunks: DocChunk[] option) (scope: string) (minDocs: int) (signal: string) =
        // Use source chunks for content (richer), fall back to summaries from index
        let contentByChunk =
            match chunks with
            | Some chs ->
                chs |> Array.map (fun c -> c.FilePath, c.Content)
            | None ->
                index.Chunks |> Array.map (fun c -> c.FilePath, c.Summary)

        // Group content by source file
        let fileContents =
            contentByChunk
            |> Array.groupBy fst
            |> Array.map (fun (file, pairs) ->
                let combined = pairs |> Array.map snd |> String.concat "\n"
                file, combined)

        let totalFiles = fileContents.Length
        if totalFiles < 2 then
            [| mdict [ "note", box "Need at least 2 indexed files for gap analysis."; "files", box totalFiles ] |]
        else

        // Extract entity refs per file (filter noise: min 3 chars, no empty)
        let fileEntities =
            fileContents
            |> Array.map (fun (file, content) ->
                let entities =
                    extractEntityRefs content
                    |> Array.map normalizeEntity
                    |> Array.filter (fun e -> e.Length >= 3)
                    |> Array.distinct
                file, entities)

        // Build bipartite index: entity → set<file>
        let entityToFiles = Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
        for (file, entities) in fileEntities do
            let displayName = Path.GetFileNameWithoutExtension(file)
            for entity in entities do
                if not (entityToFiles.ContainsKey(entity)) then
                    entityToFiles.[entity] <- HashSet<string>(StringComparer.OrdinalIgnoreCase)
                entityToFiles.[entity].Add(displayName) |> ignore

        // Filter by scope if provided (exact or prefix match, not substring)
        // Normalize scope input the same way entities are normalized (lowercase, strip prefixes/extensions)
        let scopeNorm = if String.IsNullOrWhiteSpace(scope) then "" else normalizeEntity scope
        let filtered =
            entityToFiles
            |> Seq.filter (fun kv ->
                if scopeNorm = "" then true
                else kv.Key = scopeNorm || kv.Key.StartsWith(scopeNorm + ".") || scopeNorm.StartsWith(kv.Key + "."))
            |> Seq.filter (fun kv -> kv.Value.Count >= 1)
            |> Seq.toArray

        // Classify and build results
        // God-node: top 5% of files or at least 5 — the most-connected concepts
        let godThreshold = max 5 (int (ceil (float totalFiles * 0.05)))

        // Importance heuristic for ranking isolated entities:
        // - Longer names are more specific/meaningful (penalize short generic terms)
        // - Names matching indexed file names are more important
        // - Dotted names (module.method) suggest concrete code references
        // - Names with mixed segments suggest compound identifiers
        let indexedFileNames =
            fileContents |> Array.map (fun (f, _) -> Path.GetFileNameWithoutExtension(f).ToLowerInvariant()) |> Set.ofArray
        let entityImportance (entity: string) =
            let lengthScore = min 5 (entity.Length / 3) // 0-5 points for length
            let fileBonus = if indexedFileNames.Contains(entity) then 3 else 0
            let structureBonus =
                if entity.Contains(".") then 2  // dotted module name (e.g., deliveryengine.send)
                elif entity.Length > 12 then 1  // long single word likely a compound identifier
                else 0
            lengthScore + fileBonus + structureBonus

        let preSignal =
            filtered
            |> Array.map (fun kv ->
                let entity = kv.Key
                let files = kv.Value |> Seq.sort |> Seq.toArray
                let count = files.Length
                let sig' =
                    if count >= godThreshold then "god-node"
                    elif count >= 2 then "shared"
                    else "isolated"
                let importance = entityImportance entity
                entity, files, count, sig', importance)

        let results =
            preSignal
            // Apply signal filter
            |> Array.filter (fun (_, _, count, sig', _) ->
                (signal = "" || signal = sig') && count >= minDocs)
            // Sort: god-nodes first, then shared by count desc, then isolated by importance desc
            |> Array.sortBy (fun (entity, _, count, sig', importance) ->
                let priority = match sig' with "god-node" -> 0 | "shared" -> 1 | _ -> 2
                priority, -count, -importance, entity)

        if results.Length = 0 then
            let totalEntities = entityToFiles.Count
            let scopeMatched = preSignal.Length
            let msg =
                if scopeNorm <> "" && scopeMatched = 0 then
                    sprintf "No entities matching scope '%s' found. The index has %d entities across %d files. Scope uses exact/prefix matching on normalized (lowercase) names — try a shorter prefix or gaps() with no scope to see available entities." scope totalEntities totalFiles
                elif scopeNorm <> "" && signal <> "" then
                    let actualSignals = preSignal |> Array.map (fun (_, _, _, s, _) -> s) |> Array.distinct |> String.concat ", "
                    sprintf "Scope '%s' matched %d entities, but none have signal '%s'. Found signals: %s. Try gaps({scope: '%s'}) without signal filter." scope scopeMatched signal actualSignals scope
                elif signal <> "" then
                    sprintf "No entities with signal '%s' found (minDocs=%d). The index has %d entities across %d files." signal minDocs totalEntities totalFiles
                else "No cross-document entity references found."
            [| mdict [ "note", box msg; "total_entities", box totalEntities; "total_files", box totalFiles ] |]
        else
            results
            |> Array.map (fun (entity, files, count, sig', importance) ->
                mdict [ "entity", box entity
                        "sources", box (files |> String.concat ", ")
                        "count", box count
                        "signal", box sig'
                        "importance", box importance
                        "total_files", box totalFiles ])

    // ── cluster — suggest subfolder groupings for an overcrowded directory ──

    /// Cosine similarity between two vectors.
    let private cosine (a: float32[]) (b: float32[]) =
        if a.Length = 0 || b.Length = 0 then 0.0f
        else
            let mutable dot = 0.0f
            let mutable na = 0.0f
            let mutable nb = 0.0f
            for i in 0 .. a.Length - 1 do
                dot <- dot + a.[i] * b.[i]
                na <- na + a.[i] * a.[i]
                nb <- nb + b.[i] * b.[i]
            if na = 0.0f || nb = 0.0f then 0.0f
            else dot / (sqrt na * sqrt nb)

    /// Simple greedy clustering: assign each doc to the nearest existing cluster center,
    /// or start a new cluster if similarity to all centers is below threshold.
    let private greedyCluster (items: (string * float32[])[]) (threshold: float) =
        let clusters = ResizeArray<ResizeArray<string> * float32[]>()
        for (name, emb) in items do
            let mutable bestIdx = -1
            let mutable bestSim = 0.0f
            for ci in 0 .. clusters.Count - 1 do
                let _, center = clusters.[ci]
                let sim = cosine emb center
                if sim > bestSim then bestSim <- sim; bestIdx <- ci
            if float bestSim >= threshold && bestIdx >= 0 then
                let members, _ = clusters.[bestIdx]
                members.Add(name)
            else
                let members = ResizeArray<string>()
                members.Add(name)
                clusters.Add((members, emb))
        clusters |> Seq.map (fun (members, _) -> members.ToArray()) |> Seq.toArray

    /// Suggest subfolder groupings for docs in a directory.
    /// Uses embeddings to cluster docs by semantic similarity.
    let cluster (index: DocIndex) (dir: string) (threshold: float) =
        let normDir = dir.Replace("\\", "/").TrimEnd('/')
        // Find docs in the target directory
        let docsInDir =
            index.Chunks
            |> Array.filter (fun c -> c.Level <= 1) // top-level sections only (one per doc)
            |> Array.filter (fun c ->
                let rel = c.FilePath.Replace("\\", "/")
                // Match docs directly in the target dir (not in subdirs)
                if normDir = "" || normDir = "." then
                    not (Path.GetFileName(rel) <> rel) // root-level only
                else
                    // Check if file is in this dir (works with both absolute and relative paths)
                    let dirWithSlash = normDir + "/"
                    let inDir = rel.StartsWith(dirWithSlash) || rel.Contains("/" + dirWithSlash)
                    if not inDir then false
                    else
                        // Only direct children, not in subdirs
                        let startIdx =
                            let i = rel.IndexOf(dirWithSlash)
                            if i >= 0 then i + dirWithSlash.Length else dirWithSlash.Length
                        let afterDir = rel.Substring(startIdx)
                        not (afterDir.Contains("/")))
            |> Array.distinctBy (fun c -> c.FilePath)

        if docsInDir.Length < 4 then
            // Not enough docs to warrant splitting
            [| mdict [ "suggestion", box "Folder has fewer than 4 docs — no split needed."; "docs", box docsInDir.Length ] |]
        else
            // Get embeddings for these docs
            let docEmbeddings =
                docsInDir |> Array.choose (fun c ->
                    let idx = index.Chunks |> Array.tryFindIndex (fun ch -> ch.FilePath = c.FilePath && ch.Heading = c.Heading)
                    match idx with
                    | Some i when i < index.Embeddings.Length && index.Embeddings.[i].Length > 0 ->
                        Some (Path.GetFileName c.FilePath, index.Embeddings.[i])
                    | _ -> None)

            if docEmbeddings.Length < 4 then
                [| mdict [ "suggestion", box "Not enough embedded docs to cluster."; "docs", box docEmbeddings.Length ] |]
            else
                let clusters = greedyCluster docEmbeddings threshold
                // Find common terms in each cluster for suggested folder names
                clusters
                |> Array.mapi (fun i members ->
                    let nameHint =
                        if members.Length = 1 then members.[0].Replace(".md", "")
                        else
                            // Find common prefix or common word
                            let words =
                                members
                                |> Array.collect (fun m -> m.Replace(".md", "").Replace("-", " ").Split(' '))
                                |> Array.countBy id
                                |> Array.sortByDescending snd
                                |> Array.truncate 2
                                |> Array.map fst
                            if words.Length > 0 then words |> String.concat "-"
                            else sprintf "group-%d" (i + 1)
                    mdict [ "suggestedFolder", box nameHint
                            "docs", box members.Length
                            "files", box (members |> String.concat ", ") ])

    // ── changed ──

    /// changed(gitRef) — find chunks in files that changed since a git ref.
    let changed (index: DocIndex) (session: QuerySession) (repoRoot: string) (gitRef: string) =
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
                        results.Add(mdict [
                            "id", box id; "heading", box c.Heading; "file", box (Path.GetFileName c.FilePath)
                            "path", box c.FilePath; "line", box c.StartLine; "summary", box c.Summary ])
                results.ToArray()
        with ex ->
            [| mdict [ "error", box (sprintf "git not available: %s" ex.Message) ] |]

    // ── explain ──

    /// explain(refId) — debug primitive showing index metadata and findSource diagnosis.
    let explain (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (refId: string) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found in session" refId) ]
        | Some idx when idx < 0 || idx >= index.Chunks.Length ->
            mdict [ "error", box (sprintf "ref %s points to chunk %d but index has %d chunks" refId idx index.Chunks.Length) ]
        | Some idx ->
            let c = index.Chunks.[idx]
            let cid = IndexStore.chunkId c.FilePath c.Heading c.StartLine
            let sourceMatch =
                match chunks with
                | None -> "source chunks not loaded"
                | Some chs ->
                    let cidMatch = chs |> Array.tryFind (fun ch ->
                        IndexStore.chunkId ch.FilePath ch.Heading ch.StartLine = cid)
                    match cidMatch with
                    | Some ch -> sprintf "CID match (%s), content length: %d" cid ch.Content.Length
                    | None ->
                        let tripleMatch = chs |> Array.tryFind (fun ch ->
                            ch.FilePath = c.FilePath && ch.Heading = c.Heading && ch.StartLine = c.StartLine)
                        match tripleMatch with
                        | Some ch -> sprintf "triple-key match (no CID), content length: %d" ch.Content.Length
                        | None ->
                            let pathMatch = chs |> Array.tryFind (fun ch -> normPath ch.FilePath = normPath c.FilePath && ch.Heading = c.Heading)
                            match pathMatch with
                            | Some ch -> sprintf "partial match (heading+path, line differs: source=%d vs index=%d), content length: %d" ch.StartLine c.StartLine ch.Content.Length
                            | None -> sprintf "NO MATCH — findSource will return None. CID=%s, FilePath=%s, Heading=%s, StartLine=%d" cid c.FilePath c.Heading c.StartLine
            mdict [
                "refId", box refId; "chunkIdx", box idx; "cid", box cid
                "filePath", box c.FilePath; "heading", box c.Heading
                "startLine", box c.StartLine; "endLine", box c.EndLine
                "summary", box c.Summary; "sourceMatch", box sourceMatch ]
