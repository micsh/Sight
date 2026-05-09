namespace AITeam.KnowledgeSight

open System
open System.Collections.Generic
open System.IO
open System.Net.Http
open System.Net.Sockets
open System.Numerics
open System.Security.Cryptography
open System.Text.RegularExpressions
open System.Text
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Jint
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
        |> Result.toOption
        |> Option.map (fun e -> e.[0])

    /// Normalize path separators for cross-platform comparison.
    let private normPath (p: string) = p.Replace('\\', '/')

    let private pathContainsPattern (path: string) (pattern: string) =
        String.IsNullOrWhiteSpace(pattern)
        || normPath(path).Contains(normPath(pattern), StringComparison.OrdinalIgnoreCase)

    /// Find source chunk matching an index entry. Uses stable chunk ID as primary key.
    let private findSource (chunks: DocChunk[] option) (c: ChunkEntry) =
        chunks |> Option.bind (fun chs ->
            let targetCid = IndexStore.chunkId c.FilePath c.Heading c.StartLine
            match chs |> Array.tryFind (fun ch -> IndexStore.chunkId ch.FilePath ch.Heading ch.StartLine = targetCid) with
            | Some _ as hit -> hit
            | None ->
                chs |> Array.tryFind (fun ch ->
                    normPath ch.FilePath = normPath c.FilePath && ch.Heading = c.Heading && ch.StartLine = c.StartLine))

    let private frontmatterForFile (index: DocIndex) (fileName: string) =
        index.Frontmatters
        |> Map.tryFind fileName
        |> Option.orElseWith (fun () ->
            index.Frontmatters
            |> Map.toSeq
            |> Seq.tryFind (fun (k, _) -> IndexStore.matchFile k fileName)
            |> Option.map snd)

    let private indexedFrontmatterPayload (frontmatter: Frontmatter option) =
        match frontmatter with
        | Some value -> "index", frontmatterToDict value
        | None -> "", mdict []

    let retrievalDefaultStatuses = [| "active" |]
    let orphansDefaultStatuses = [| "active"; "stale"; "superseded"; "deprecated" |]
    let brokenDefaultStatuses = [| "active"; "stale" |]
    let staleDefaultStatuses = [| "active" |]
    let prunePreviewStatuses = set [ "stale"; "superseded"; "deprecated" ]
    let pruneDefaultOlderThanDays = 30

    let private normalizeStatusValue (status: string) =
        if String.IsNullOrWhiteSpace(status) then ""
        else status.Trim().ToLowerInvariant()

    let private normalizeRequestedStatuses (statuses: string[]) =
        statuses
        |> Array.map normalizeStatusValue
        |> Array.filter (String.IsNullOrWhiteSpace >> not)
        |> Set.ofArray

    let private relativeRepoPath (repoRoot: string) (path: string) =
        let fullPath = Path.GetFullPath(path)
        let fullRoot = Path.GetFullPath(repoRoot)
        Path.GetRelativePath(fullRoot, fullPath).Replace("\\", "/")

    let private absoluteRepoPath (repoRoot: string) (relativePath: string) =
        Path.Combine(repoRoot, relativePath.Replace("/", string Path.DirectorySeparatorChar))

    let private inboxPrefix (cfg: KnowledgeSightConfig) =
        Config.resolveInboxDir cfg
        |> Result.map (fun inboxDir -> inboxDir.Trim('/').Replace("\\", "/") + "/")

    let private isInboxPath (cfg: KnowledgeSightConfig) (path: string) =
        let rel = relativeRepoPath cfg.RepoRoot path
        match inboxPrefix cfg with
        | Ok prefix -> rel.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
        | Error _ -> false

    let effectiveDocStatus (cfg: KnowledgeSightConfig) (index: DocIndex) (path: string) =
        let frontmatterStatus =
            frontmatterForFile index path
            |> Option.map (fun fm -> normalizeStatusValue fm.Status)
            |> Option.defaultValue ""

        if frontmatterStatus <> "" then frontmatterStatus
        elif isInboxPath cfg path then "pending"
        else "active"

    let matchesDocStatus (cfg: KnowledgeSightConfig) (index: DocIndex) (allowedStatuses: Set<string>) (path: string) =
        allowedStatuses.Contains(effectiveDocStatus cfg index path)

    // ── catalog (like modules in code-sight) ──

    let private catalogDirectoryKey (cfg: KnowledgeSightConfig) (filePath: string) =
        let relativePath =
            if Path.IsPathRooted(filePath) then relativeRepoPath cfg.RepoRoot filePath
            else normPath filePath

        let normalized = normPath relativePath
        let parts = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries)

        if parts.Length >= 2 then
            String.concat "/" parts.[0 .. parts.Length - 2]
        else
            "root"

    let catalog (cfg: KnowledgeSightConfig) (index: DocIndex) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        index.Chunks
        |> Array.filter (fun c -> matchesDocStatus cfg index allowedStatuses c.FilePath)
        |> Array.groupBy (fun c -> catalogDirectoryKey cfg c.FilePath)
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

    let private searchWithEmbedding (cfg: KnowledgeSightConfig) (index: DocIndex) (makeId: int -> ChunkEntry -> string)
                                    (embedding: float32[]) (limit: int) (tag: string) (filePattern: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        IndexStore.search index embedding index.Chunks.Length
        |> Array.filter (fun (i, _) ->
            let c = index.Chunks.[i]
            matchesDocStatus cfg index allowedStatuses c.FilePath &&
            (String.IsNullOrEmpty(tag) || c.Tags.Contains(tag, StringComparison.OrdinalIgnoreCase)) &&
            pathContainsPattern c.FilePath filePattern)
        |> Array.truncate limit
        |> Array.map (fun (i, sim) ->
            let c = index.Chunks.[i]
            let id = makeId i c
            mdict [ "id", box id; "score", box (Math.Round(float sim, 3))
                    "heading", box c.Heading; "headingPath", box c.HeadingPath
                    "file", box (Path.GetFileName c.FilePath); "path", box c.FilePath
                    "line", box c.StartLine; "summary", box c.Summary
                    "tags", box c.Tags; "links", box c.LinkCount; "words", box c.WordCount ])

    let search (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (embeddingUrl: string)
               (query: string) (limit: int) (tag: string) (filePattern: string) (statuses: string[]) =
        match embedQuery embeddingUrl query with
        | None -> [| mdict [ "error", box "embedding server not available — search requires embeddings" ] |]
        | Some qEmb ->
            searchWithEmbedding cfg index (fun i _ -> session.NextRef(i)) qEmb limit tag filePattern statuses

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
        let fm = frontmatterForFile index fileName
        let frontmatterSource, frontmatter = indexedFrontmatterPayload fm
        mdict [
            "file", box fileName
            "title", box (fm |> Option.map (fun f -> f.Title) |> Option.defaultValue "")
            "status", box (fm |> Option.map (fun f -> f.Status) |> Option.defaultValue "")
            "tags", box (fm |> Option.map (fun f -> f.Tags |> String.concat ", ") |> Option.defaultValue "")
            "related", box (fm |> Option.map (fun f -> f.Related |> String.concat ", ") |> Option.defaultValue "")
            "frontmatterSource", box frontmatterSource
            "frontmatter", box frontmatter
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

    let similar (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (refId: string) (limit: int) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        match session.GetRef(refId) with
        | None -> [| mdict [ "error", box (sprintf "ref %s not found" refId) ] |]
        | Some chunkIdx ->
            IndexStore.similar index chunkIdx index.Chunks.Length
            |> Array.filter (fun (i, _) -> matchesDocStatus cfg index allowedStatuses index.Chunks.[i].FilePath)
            |> Array.truncate limit
            |> Array.map (fun (i, sim) ->
                let c = index.Chunks.[i]
                let id = session.NextRef(i)
                mdict [ "id", box id; "score", box (Math.Round(float sim, 3))
                        "heading", box c.Heading; "file", box (Path.GetFileName c.FilePath)
                        "line", box c.StartLine; "summary", box c.Summary; "tags", box c.Tags ])

    // ── grep ──

    let grep (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (pattern: string) (limit: int) (filePattern: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks ->
            let regex = try Regex(pattern, RegexOptions.IgnoreCase ||| RegexOptions.Compiled) with _ -> Regex(Regex.Escape(pattern), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    if matchesDocStatus cfg index allowedStatuses c.FilePath
                       && pathContainsPattern c.FilePath filePattern then
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

    let mentions (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (term: string) (limit: int) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks ->
            let regex = Regex(sprintf @"\b%s\b" (Regex.Escape term), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
            let results = ResizeArray()
            for i in 0..index.Chunks.Length-1 do
                if results.Count < limit then
                    let c = index.Chunks.[i]
                    if matchesDocStatus cfg index allowedStatuses c.FilePath then
                        match findSource (Some allChunks) c with
                        | Some ch when regex.IsMatch(ch.Content) ->
                            let matchLine = ch.Content.Split('\n') |> Array.tryFind (fun l -> regex.IsMatch(l)) |> Option.map (fun l -> l.Trim()) |> Option.defaultValue ""
                            let count = regex.Matches(ch.Content).Count
                            let id = session.NextRef(i)
                            results.Add(mdict [ "id", box id; "heading", box c.Heading; "file", box (Path.GetFileName c.FilePath)
                                                "line", box c.StartLine; "matchLine", box matchLine; "count", box count
                                                "summary", box c.Summary; "tags", box c.Tags ])
                        | _ -> ()
                    else
                        ()
            results.ToArray()

    // ── files ──

    let files (index: DocIndex) (pattern: string) =
        index.Chunks |> Array.groupBy (fun c -> c.FilePath)
        |> Array.choose (fun (filePath, chunks) ->
            let fileName = Path.GetFileName(filePath)
            if String.IsNullOrEmpty(pattern) || fileName.Contains(pattern, StringComparison.OrdinalIgnoreCase) || pathContainsPattern filePath pattern then
                let fm = frontmatterForFile index filePath
                let frontmatterSource, frontmatter = indexedFrontmatterPayload fm
                let title = fm |> Option.map (fun f -> f.Title) |> Option.defaultValue ""
                let tags = fm |> Option.map (fun f -> f.Tags |> String.concat ",") |> Option.defaultValue ""
                let backlinks = IndexStore.backlinks index filePath
                Some (mdict [ "file", box fileName; "path", box filePath; "sections", box chunks.Length
                              "title", box title; "tags", box tags; "backlinks", box backlinks.Length
                              "words", box (chunks |> Array.sumBy (fun c -> c.WordCount))
                              "frontmatterSource", box frontmatterSource
                              "frontmatter", box frontmatter ])
            else None)
        |> Array.sortBy (fun d -> string d.["file"])

    // ── backlinks ──

    let backlinks (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (fileName: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        IndexStore.backlinks index fileName
        |> Array.filter (fun l -> matchesDocStatus cfg index allowedStatuses l.SourceFile)
        |> Array.map (fun l ->
            mdict [ "from", box (Path.GetFileName l.SourceFile); "section", box l.SourceHeading
                    "text", box l.LinkText; "line", box l.Line
                    "resolved", box (if l.TargetResolved <> "" then "✓" else "✗") ])

    // ── links (outgoing) ──

    let links (cfg: KnowledgeSightConfig) (index: DocIndex) (fileName: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        IndexStore.outlinks index fileName
        |> Array.filter (fun l -> matchesDocStatus cfg index allowedStatuses l.SourceFile)
        |> Array.map (fun l ->
            let resolved = if l.TargetResolved <> "" then Path.GetFileName l.TargetResolved else sprintf "⚠ %s" l.TargetPath
            mdict [ "to", box resolved; "text", box l.LinkText; "section", box l.SourceHeading; "line", box l.Line ])

    // ── pinned — ambient tier-filtered docs ──

    let private tryFindExtraScalarInsensitive (key: string) (frontmatter: Frontmatter) =
        frontmatter.Extra
        |> Map.toSeq
        |> Seq.tryPick (fun (extraKey, extraValue) ->
            if String.Equals(extraKey, key, StringComparison.OrdinalIgnoreCase) then
                match extraValue with
                | Scalar value when not (String.IsNullOrWhiteSpace(value)) -> Some value
                | _ -> None
            else
                None)

    let pinned (index: DocIndex) (session: QuerySession) (tier: string) =
        let requestedTier =
            if String.IsNullOrWhiteSpace(tier) then "grammar"
            else tier.Trim()

        index.Frontmatters
        |> Map.toArray
        |> Array.choose (fun (filePath, frontmatter) ->
            let status = frontmatter.Status.Trim().ToLowerInvariant()
            let tierValue =
                frontmatter
                |> tryFindExtraScalarInsensitive "tier"
                |> Option.defaultValue ""

            if (status = "active" || status = "stale")
               && String.Equals(tierValue, requestedTier, StringComparison.OrdinalIgnoreCase) then
                index.Chunks
                |> Array.indexed
                |> Array.filter (fun (_, chunk) -> String.Equals(chunk.FilePath, filePath, StringComparison.OrdinalIgnoreCase))
                |> Array.sortBy (fun (_, chunk) -> chunk.Level, chunk.StartLine)
                |> Array.tryHead
                |> Option.map (fun (chunkIndex, chunk) ->
                    let refId = session.NextRef(chunkIndex)
                    mdict [
                        "id", box refId
                        "file", box (Path.GetFileName filePath)
                        "path", box filePath
                        "title", box frontmatter.Title
                        "status", box frontmatter.Status
                        "tier", box tierValue
                        "summary", box chunk.Summary
                    ])
            else
                None)
        |> Array.sortBy (fun result -> string result.["path"])

    // ── orphans — docs with no incoming links ──

    let orphans (cfg: KnowledgeSightConfig) (index: DocIndex) (statuses: string[]) =
        let candidateStatuses = normalizeRequestedStatuses statuses
        let nonPendingStatuses = normalizeRequestedStatuses orphansDefaultStatuses
        let allFiles = index.Chunks |> Array.map (fun c -> c.FilePath) |> Array.distinct
        let linkedFiles =
            index.Links
            |> Array.filter (fun l -> l.TargetResolved <> "" && matchesDocStatus cfg index nonPendingStatuses l.SourceFile)
            |> Array.choose (fun l -> if l.TargetResolved <> "" then Some l.TargetResolved else None)
            |> Set.ofArray
        allFiles
        |> Array.filter (fun f ->
            matchesDocStatus cfg index candidateStatuses f
            && (effectiveDocStatus cfg index f = "pending" || not (linkedFiles.Contains f)))
        |> Array.map (fun f ->
            let fm = index.Frontmatters |> Map.tryFind f
            let title = fm |> Option.map (fun f -> f.Title) |> Option.defaultValue ""
            let sections = index.Chunks |> Array.filter (fun c -> c.FilePath = f)
            mdict [ "file", box (Path.GetFileName f); "path", box f; "title", box title; "sections", box sections.Length ])

    // ── broken — links pointing to nonexistent docs ──

    let broken (cfg: KnowledgeSightConfig) (index: DocIndex) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        index.Links
        |> Array.filter (fun l -> l.TargetResolved = "" && matchesDocStatus cfg index allowedStatuses l.SourceFile)
        |> Array.map (fun l ->
            mdict [ "from", box (Path.GetFileName l.SourceFile); "target", box l.TargetPath
                    "text", box l.LinkText; "section", box l.SourceHeading; "line", box l.Line ])

    // ── placement — where should new content go? ──

    let placement (cfg: KnowledgeSightConfig) (index: DocIndex) (embeddingUrl: string) (content: string) (limit: int) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses

        match embedQuery embeddingUrl content with
        | None -> [| mdict [ "error", box "embedding server not available — placement requires embeddings" ] |]
        | Some qEmb ->
            // Find most similar sections, then group by file to suggest placement
            let hits =
                IndexStore.search index qEmb index.Chunks.Length
                |> Array.filter (fun (i, _) -> matchesDocStatus cfg index allowedStatuses index.Chunks.[i].FilePath)
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

    let walk (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (startFile: string) (maxDepth: int) (direction: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        let visited = HashSet<string>()
        let results = ResizeArray<Dictionary<string, obj>>()

        let rec trace (file: string) (depth: int) (trail: string list) =
            if depth > maxDepth || visited.Contains(file) || results.Count >= maxDepth * 10 || not (matchesDocStatus cfg index allowedStatuses file) then ()
            else
                visited.Add(file) |> ignore
                let neighbors =
                    if direction = "in" then
                        IndexStore.backlinks index file |> Array.map (fun l -> l.SourceFile, l.LinkText)
                    else
                        IndexStore.outlinks index file |> Array.map (fun l -> l.TargetResolved, l.LinkText)
                    |> Array.filter (fun (f, _) -> f <> "" && not (visited.Contains f) && matchesDocStatus cfg index allowedStatuses f)
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
        if matchesDocStatus cfg index allowedStatuses startResolved then
            trace startResolved 1 [Path.GetFileName startResolved]
        results.ToArray()

    // ── novelty — what's new in this text vs existing knowledge? ──

    let private splitKnowledgeParagraphs (text: string) =
        text.Split([| "\n\n"; "\r\n\r\n" |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.map (fun paragraph -> paragraph.Trim())
        |> Array.filter (fun paragraph ->
            paragraph.Length > 30
            && not (paragraph.StartsWith("```"))
            && not (paragraph.StartsWith("|")))

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

    let private allowShortProposeClaim (para: string) =
        let lower = para.ToLowerInvariant()
        let prescriptive = [| " should "; " must "; " always "; " never "; " when "; " ensure "; " requires "; " depends on "; " means that " |]
        let declarative = [| " is a "; " are "; " defines "; " represents "; " consists of "; " handles "; " processes " |]
        let hedging = [| " maybe "; " perhaps "; " i wonder "; " not sure "; " might "; " could be "; " i think "; "?" |]
        let hasPattern (patterns: string[]) = patterns |> Array.exists lower.Contains

        para.Length < 50
        && not (hasPattern hedging)
        && (hasPattern prescriptive || hasPattern declarative)

    /// Split text into paragraphs, embed each, compare to index.
    /// Classifies each paragraph as: off-topic, musing, novel, or covered.
    let novelty (cfg: KnowledgeSightConfig) (index: DocIndex) (embeddingUrl: string) (text: string) (threshold: float) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        let paragraphs = splitKnowledgeParagraphs text

        if paragraphs.Length = 0 then [||]
        else
            let prefixed = paragraphs |> Array.map (fun p -> sprintf "search_query: %s" (p.Substring(0, min 200 p.Length)))
            let embeddings =
                match EmbeddingService.embed embeddingUrl prefixed |> Async.AwaitTask |> Async.RunSynchronously with
                | Ok embs -> embs
                | Error _ -> [||]

            if embeddings.Length = 0 then [||]
            else
                paragraphs |> Array.mapi (fun i para ->
                    if i >= embeddings.Length || embeddings.[i].Length = 0 then
                        mdict [ "paragraph", box (para.Substring(0, min 80 para.Length) + "..."); "status", box "error"; "score", box 0.0 ]
                    else
                        let hits =
                            IndexStore.search index embeddings.[i] index.Chunks.Length
                            |> Array.filter (fun (idx, _) -> matchesDocStatus cfg index allowedStatuses index.Chunks.[idx].FilePath)
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

    // ── minimal v1 bus loop — propose / triage / dispose ──

    let private isWriteCanonicalFile (cfg: KnowledgeSightConfig) (index: DocIndex) (path: string) =
        not (isInboxPath cfg path)
        && effectiveDocStatus cfg index path = "active"

    let private isPendingInboxFile (cfg: KnowledgeSightConfig) (index: DocIndex) (path: string) (frontmatter: Frontmatter) =
        isInboxPath cfg path
        && effectiveDocStatus cfg index path = "pending"
        && not (frontmatterFields frontmatter |> Map.containsKey "disposition")

    let private busTokens (text: string) =
        Regex.Matches(text.ToLowerInvariant(), @"[a-z0-9]{3,}")
        |> Seq.cast<Match>
        |> Seq.map (fun m -> m.Value)
        |> Set.ofSeq

    let private busJaccard (left: Set<string>) (right: Set<string>) =
        if Set.isEmpty left || Set.isEmpty right then 0.0
        else
            let overlap = Set.intersect left right |> Set.count
            let universe = Set.union left right |> Set.count
            if universe = 0 then 0.0 else float overlap / float universe

    let private cosineScore (query: float32[]) (candidate: float32[]) =
        if query.Length = 0 || candidate.Length = 0 || query.Length <> candidate.Length then
            None
        else
            let mutable dot = 0.0f
            let mutable queryNorm = 0.0f
            let mutable candidateNorm = 0.0f
            for i in 0 .. query.Length - 1 do
                dot <- dot + query.[i] * candidate.[i]
                queryNorm <- queryNorm + query.[i] * query.[i]
                candidateNorm <- candidateNorm + candidate.[i] * candidate.[i]
            if queryNorm = 0.0f || candidateNorm = 0.0f then None
            else Some (float (dot / (sqrt queryNorm * sqrt candidateNorm)))

    let private candidateText (chunks: DocChunk[] option) (entry: ChunkEntry) =
        match findSource chunks entry with
        | Some chunk -> String.concat "\n" [ entry.HeadingPath; entry.Summary; chunk.Content ]
        | None -> String.concat "\n" [ entry.HeadingPath; entry.Summary ]

    let private rankedSubsetMatches (index: DocIndex) (chunks: DocChunk[] option) (embeddingUrl: string) (isEligibleFile: string -> bool) (text: string) (limit: int) =
        let candidates =
            index.Chunks
            |> Array.indexed
            |> Array.filter (fun (_, chunk) -> isEligibleFile chunk.FilePath)

        let byEmbedding =
            match embedQuery embeddingUrl text with
            | Some queryEmbedding ->
                candidates
                |> Array.choose (fun (i, _) ->
                    if i < index.Embeddings.Length then
                        cosineScore queryEmbedding index.Embeddings.[i]
                        |> Option.map (fun score -> i, score)
                    else None)
            | None -> [||]

        let ranked =
            if byEmbedding.Length > 0 then
                byEmbedding
                |> Array.sortByDescending snd
            else
                let queryTokens = busTokens text
                candidates
                |> Array.map (fun (i, chunk) ->
                    let score = busJaccard queryTokens (busTokens (candidateText chunks chunk))
                    i, score)
                |> Array.sortByDescending snd

        ranked |> Array.truncate limit

    let private writePipelinePlacement (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option) (embeddingUrl: string) (text: string) (limit: int) =
        rankedSubsetMatches index chunks embeddingUrl (isWriteCanonicalFile cfg index) text (max 1 (limit * 3))
        |> Array.groupBy (fun (i, _) -> index.Chunks.[i].FilePath)
        |> Array.map (fun (filePath, matches) ->
            let bestIndex, bestScore = matches |> Array.maxBy snd
            let bestChunk = index.Chunks.[bestIndex]
            filePath, bestScore, bestChunk)
        |> Array.sortByDescending (fun (_, score, _) -> score)
        |> Array.truncate limit

    let private firstLine (text: string) =
        text.Split('\n')
        |> Array.map (fun line -> line.Trim())
        |> Array.tryFind (String.IsNullOrWhiteSpace >> not)
        |> Option.defaultValue "Untitled claim"

    let private titleFromText (text: string) =
        let line = firstLine text
        if line.Length > 80 then line.Substring(0, 80).TrimEnd() + "..."
        else line

    let private slugify (text: string) =
        let cleaned =
            text.ToLowerInvariant()
            |> fun value -> Regex.Replace(value, @"[^a-z0-9]+", "-")
            |> fun value -> value.Trim('-')
        if cleaned = "" then "claim"
        elif cleaned.Length > 48 then cleaned.Substring(0, 48).Trim('-')
        else cleaned

    let private normalizeTeam (team: string) =
        team.Trim()
        |> fun value -> Regex.Replace(value, @"[^A-Za-z0-9_-]+", "-")
        |> fun value -> value.Trim('-')

    let private cycleFormat = "yyyy-MM-dd'T'HH-mm-ss'Z'"

    type private CycleValue =
        | TimestampCycle of DateTime
        | IntegerCycle of BigInteger

    let private tryParseTimestampCycle (cycle: string) =
        match DateTime.TryParseExact(cycle, cycleFormat, Globalization.CultureInfo.InvariantCulture, Globalization.DateTimeStyles.AssumeUniversal ||| Globalization.DateTimeStyles.AdjustToUniversal) with
        | true, parsed -> Some parsed
        | _ -> None

    let private tryParseIntegerCycle (cycle: string) =
        let trimmed = cycle.Trim()

        if String.IsNullOrWhiteSpace(trimmed) then
            None
        elif Regex.IsMatch(trimmed, @"^[0-9]+$") then
            Some(BigInteger.Parse(trimmed, Globalization.CultureInfo.InvariantCulture))
        else
            None

    let private tryParseCycleValue (cycle: string) =
        let trimmed = cycle.Trim()

        match tryParseTimestampCycle trimmed with
        | Some parsed -> Some(TimestampCycle parsed)
        | None -> tryParseIntegerCycle trimmed |> Option.map IntegerCycle

    let private normalizeCycle (cycle: string) =
        let trimmed = cycle.Trim()

        match tryParseTimestampCycle trimmed with
        | Some parsed -> Some(parsed.ToString(cycleFormat, Globalization.CultureInfo.InvariantCulture))
        | None ->
            tryParseIntegerCycle trimmed
            |> Option.map (fun parsed -> parsed.ToString(Globalization.CultureInfo.InvariantCulture))

    let private isValidCycle (cycle: string) =
        normalizeCycle cycle |> Option.isSome

    let private compareCycleValues (left: string) (right: string) =
        let leftTrimmed = left.Trim()
        let rightTrimmed = right.Trim()

        match tryParseCycleValue leftTrimmed, tryParseCycleValue rightTrimmed with
        | Some(TimestampCycle leftCycle), Some(TimestampCycle rightCycle) -> compare leftCycle rightCycle
        | Some(IntegerCycle leftCycle), Some(IntegerCycle rightCycle) -> compare leftCycle rightCycle
        | Some(TimestampCycle _), Some(IntegerCycle _) -> -1
        | Some(IntegerCycle _), Some(TimestampCycle _) -> 1
        | Some _, None -> -1
        | None, Some _ -> 1
        | None, None -> String.Compare(leftTrimmed, rightTrimmed, StringComparison.OrdinalIgnoreCase)

    let private cycleFormName (cycle: string) =
        match tryParseCycleValue cycle with
        | Some(TimestampCycle _) -> Some "utc"
        | Some(IntegerCycle _) -> Some "integer"
        | None -> None

    let private cycleOnOrBefore (cycle: string) (before: string) =
        let cycleTrimmed = cycle.Trim()
        let beforeTrimmed = before.Trim()

        match tryParseCycleValue cycleTrimmed, tryParseCycleValue beforeTrimmed with
        | Some(TimestampCycle cycleValue), Some(TimestampCycle beforeValue) -> cycleValue <= beforeValue
        | Some(IntegerCycle cycleValue), Some(IntegerCycle beforeValue) -> cycleValue <= beforeValue
        | Some _, Some _ -> false
        | _ -> String.Compare(cycleTrimmed, beforeTrimmed, StringComparison.OrdinalIgnoreCase) <= 0

    let private humanizeAge (cycle: string) =
        match tryParseTimestampCycle cycle with
        | None -> ""
        | Some parsed ->
            let delta = DateTime.UtcNow - parsed
            if delta.TotalDays >= 1.0 then sprintf "%dd" (int delta.TotalDays)
            elif delta.TotalHours >= 1.0 then sprintf "%dh" (int delta.TotalHours)
            elif delta.TotalMinutes >= 1.0 then sprintf "%dm" (int delta.TotalMinutes)
            else sprintf "%ds" (max 0 (int delta.TotalSeconds))

    let private frontmatterFieldValue (frontmatter: Frontmatter) (key: string) =
        frontmatterFields frontmatter |> Map.tryFind key

    let private frontmatterScalar (frontmatter: Frontmatter) (key: string) =
        match frontmatterFieldValue frontmatter key with
        | Some (Scalar value) when not (String.IsNullOrWhiteSpace(value)) -> Some value
        | Some (StringList values) when values.Length > 0 -> Some values.[0]
        | _ -> None

    let private missingRequiredFields (cfg: KnowledgeSightConfig) (frontmatter: Frontmatter) =
        cfg.RequireFields
        |> Array.filter (fun field ->
            match frontmatterFieldValue frontmatter field with
            | Some (Scalar value) -> String.IsNullOrWhiteSpace(value)
            | Some (StringList values) -> values |> Array.forall String.IsNullOrWhiteSpace
            | None -> true)

    let private setFrontmatterField (key: string) (value: FrontmatterValue) (frontmatter: Frontmatter) =
        frontmatterFields frontmatter
        |> Map.add key value
        |> frontmatterFromFields

    let private setFrontmatterScalarIfAny (key: string) (value: string) (frontmatter: Frontmatter) =
        if String.IsNullOrWhiteSpace(value) then frontmatter
        else setFrontmatterField key (Scalar value) frontmatter

    let private removeFrontmatterFields (keys: string[]) (frontmatter: Frontmatter) =
        frontmatterFields frontmatter
        |> Map.filter (fun key _ -> keys |> Array.forall (fun removeKey -> not (String.Equals(key, removeKey, StringComparison.OrdinalIgnoreCase))))
        |> frontmatterFromFields

    let private decodeStoredUnicodeEscapes (value: string) =
        if String.IsNullOrWhiteSpace(value) then value
        else
            value
            |> fun raw ->
                let mutable collapsed = raw
                while collapsed.Contains("\\\\u") do
                    collapsed <- collapsed.Replace("\\\\u", "\\u")
                collapsed
            |> fun collapsed -> Regex.Replace(collapsed, @"\\u([0-9a-fA-F]{4})", fun m ->
                let codePoint = Convert.ToInt32(m.Groups.[1].Value, 16)
                string (char codePoint))

    let private decodeStoredVerifyExpression (expression: string) =
        decodeStoredUnicodeEscapes expression
        |> fun value -> value.Replace("\\/", "/").Replace("\\\"", "\"")

    let private decodeStoredJsonScalar (value: string) =
        if String.IsNullOrWhiteSpace(value) then value
        else
            let wrapped = "\"" + value.Replace("\"", "\\\"") + "\""
            use doc = JsonDocument.Parse(wrapped)
            doc.RootElement.GetString()

    let private verifyExpressionUsesSearch (expression: string) =
        decodeStoredVerifyExpression expression
        |> fun value -> Regex.IsMatch(value, @"(?<![A-Za-z0-9_$])search\s*\(", RegexOptions.IgnoreCase)

    let private verifyVisibleFrontmatterToDict (frontmatter: Frontmatter) =
        frontmatterFields frontmatter
        |> Map.filter (fun key _ ->
            not (String.Equals(key, "verify_snapshot", StringComparison.OrdinalIgnoreCase)) &&
            not (String.Equals(key, "verify_search_cache", StringComparison.OrdinalIgnoreCase)))
        |> Map.map (fun key value ->
            if String.Equals(key, "verify", StringComparison.OrdinalIgnoreCase) then
                match value with
                | Scalar scalar -> Scalar (decodeStoredVerifyExpression scalar)
                | _ -> value
            else value)
        |> frontmatterFromFields
        |> frontmatterToDict

    let private orderedFrontmatterFields (frontmatter: Frontmatter) =
        let preferredOrder =
            [|
                "id"; "title"; "status"; "tags"; "related"; "source"; "cycle"; "confidence"; "concept"; "verify"; "observable"; "forbids"
                "verify_snapshot"; "verify_search_cache"; "supersedes"; "superseded_by"; "reason"; "suggested_target"; "disposition"; "disposition_target"; "disposition_reason"
            |]

        let fieldMap = frontmatterFields frontmatter
        let preferred =
            preferredOrder
            |> Array.choose (fun key -> fieldMap |> Map.tryFind key |> Option.map (fun value -> key, value))

        let preferredSet = preferredOrder |> Set.ofArray
        let rest =
            fieldMap
            |> Map.toArray
            |> Array.filter (fun (key, _) -> not (preferredSet.Contains key))

        Array.append preferred rest

    let private yamlScalar (value: string) =
        System.Text.Json.JsonSerializer.Serialize(value)

    let private yamlVerifyScalar (value: string) =
        let decoded = decodeStoredVerifyExpression value
        if decoded.Contains('\n') || decoded.Contains('\r') then
            yamlScalar decoded
        else
            decoded

    let private frontmatterYaml (frontmatter: Frontmatter) =
        let lines = ResizeArray<string>()
        lines.Add("---")
        for (key, value) in orderedFrontmatterFields frontmatter do
            match value with
            | Scalar scalar when String.Equals(key, "verify", StringComparison.OrdinalIgnoreCase) ->
                lines.Add(sprintf "%s: %s" key (yamlVerifyScalar scalar))
            | Scalar scalar -> lines.Add(sprintf "%s: %s" key (yamlScalar scalar))
            | StringList values ->
                lines.Add(sprintf "%s:" key)
                for item in values do
                    lines.Add(sprintf "  - %s" (yamlScalar item))
        lines.Add("---")
        lines.ToArray() |> String.concat "\n"

    let private markdownBodyWithoutFrontmatter (path: string) =
        let lines = File.ReadAllLines(path)
        let _, contentStart = MarkdownChunker.parseFrontmatter lines
        if contentStart >= lines.Length then ""
        else String.concat "\n" lines.[contentStart..]

    let private renderMarkdownContent (frontmatter: Frontmatter option) (body: string) =
        let trimmedBody = body.Trim()
        match frontmatter with
        | Some fm ->
            if trimmedBody = "" then frontmatterYaml fm + "\n"
            else frontmatterYaml fm + "\n\n" + trimmedBody + "\n"
        | None ->
            if trimmedBody = "" then ""
            else trimmedBody + "\n"

    let private readMarkdownFile (path: string) =
        let content =
            if File.Exists path then File.ReadAllText(path)
            else ""

        let lines =
            if File.Exists path then File.ReadAllLines(path)
            else [||]

        let frontmatter, contentStart = MarkdownChunker.parseFrontmatter lines
        let body =
            if contentStart >= lines.Length then ""
            else String.concat "\n" lines.[contentStart..]

        content, frontmatter, body

    let private writeMarkdownFile (path: string) (frontmatter: Frontmatter) (body: string) =
        Directory.CreateDirectory(Path.GetDirectoryName(path)) |> ignore
        File.WriteAllText(path, renderMarkdownContent (Some frontmatter) body)

    let private captureFileSnapshot (path: string) =
        if File.Exists path then Some (File.ReadAllText(path))
        else None

    let private restoreFileSnapshot (path: string) (snapshot: string option) =
        match snapshot with
        | Some content ->
            Directory.CreateDirectory(Path.GetDirectoryName(path)) |> ignore
            File.WriteAllText(path, content)
        | None ->
            if File.Exists path then
                File.Delete(path)

    let private nextAvailablePath (path: string) =
        if not (File.Exists path) then path
        else
            let directory = Path.GetDirectoryName(path)
            let name = Path.GetFileNameWithoutExtension(path)
            let extension = Path.GetExtension(path)
            Seq.initInfinite ((+) 2)
            |> Seq.map (fun n -> Path.Combine(directory, sprintf "%s-%d%s" name n extension))
            |> Seq.find (File.Exists >> not)

    let private nextVersionSiblingPath (path: string) =
        let directory = Path.GetDirectoryName(path)
        let extension = Path.GetExtension(path)
        let stem = Path.GetFileNameWithoutExtension(path)
        let baseStem =
            let versionMatch = Regex.Match(stem, @"^(.*?)-v(\d+)$", RegexOptions.IgnoreCase)
            if versionMatch.Success then versionMatch.Groups.[1].Value
            else stem

        let namePattern = sprintf "^%s(?:-v(?<version>\\d+))?%s$" (Regex.Escape(baseStem)) (Regex.Escape(extension))
        let maxVersion =
            Directory.EnumerateFiles(directory, "*" + extension)
            |> Seq.choose (fun candidate ->
                let fileName = Path.GetFileName(candidate)
                let candidateMatch = Regex.Match(fileName, namePattern, RegexOptions.IgnoreCase)
                if not candidateMatch.Success then None
                elif candidateMatch.Groups.["version"].Success then
                    match Int32.TryParse(candidateMatch.Groups.["version"].Value) with
                    | true, version -> Some version
                    | _ -> None
                else
                    Some 1)
            |> Seq.append (Seq.singleton 1)
            |> Seq.max

        Path.Combine(directory, sprintf "%s-v%d%s" baseStem (maxVersion + 1) extension)

    let private fallbackCanonicalTitle (filePath: string) (chunk: ChunkEntry) (body: string) =
        let derived = titleFromText body
        if derived <> "Untitled claim" then derived
        elif not (String.IsNullOrWhiteSpace(chunk.Heading)) && chunk.Heading <> "(intro)" then chunk.Heading
        else Path.GetFileNameWithoutExtension(filePath)

    let private canonicalFrontmatterForFile (filePath: string) (chunk: ChunkEntry) (frontmatter: Frontmatter option) (body: string) =
        frontmatter
        |> Option.defaultWith (fun () ->
            {
                Id = ""
                Title = fallbackCanonicalTitle filePath chunk body
                Status = ""
                Tags = [||]
                Related = [||]
                Extra = Map.empty
            })
        |> fun fm ->
            if String.IsNullOrWhiteSpace(fm.Title) then
                { fm with Title = fallbackCanonicalTitle filePath chunk body }
            else
                fm

    let private replacementTitle (oldTitle: string) (newContent: string) =
        let derived = titleFromText newContent
        if derived <> "Untitled claim" then derived
        elif not (String.IsNullOrWhiteSpace(oldTitle)) then oldTitle
        else derived

    let private pathWithinRoot (rootPath: string) (candidatePath: string) =
        let fullRoot = Path.GetFullPath(rootPath).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
        let fullCandidate = Path.GetFullPath(candidatePath)
        String.Equals(fullCandidate, fullRoot, StringComparison.OrdinalIgnoreCase)
        || fullCandidate.StartsWith(fullRoot + string Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)

    let private resolveRepoRelativePath (cfg: KnowledgeSightConfig) (path: string) =
        let candidate =
            if Path.IsPathRooted(path) then path
            else Path.Combine(cfg.RepoRoot, path.Replace("/", Path.DirectorySeparatorChar.ToString()))

        let fullPath = Path.GetFullPath(candidate)
        if pathWithinRoot cfg.RepoRoot fullPath then Ok fullPath
        else Error (sprintf "path '%s' must stay under the repo root" path)

    let private normalizeConfigRelativeDir (path: string) =
        path.Replace("\\", "/").Trim().Trim('/')

    let private isUnderConfiguredDocDir (cfg: KnowledgeSightConfig) (path: string) =
        let relativePath = relativeRepoPath cfg.RepoRoot path
        let normalizedRelative = normalizeConfigRelativeDir relativePath
        cfg.DocDirs
        |> Array.map normalizeConfigRelativeDir
        |> Array.exists (fun dir ->
            dir = "."
            || String.Equals(normalizedRelative, dir, StringComparison.OrdinalIgnoreCase)
            || normalizedRelative.StartsWith(dir + "/", StringComparison.OrdinalIgnoreCase))

    let private isIndexedFile (index: DocIndex) (path: string) =
        index.Chunks
        |> Array.exists (fun chunk -> String.Equals(chunk.FilePath, path, StringComparison.OrdinalIgnoreCase))

    let private resolveMergeTargetPath (cfg: KnowledgeSightConfig) (index: DocIndex) (target: string) =
        if String.IsNullOrWhiteSpace(target) then
            Error "dispose(merge) requires an explicit target"
        else
            match resolveRepoRelativePath cfg target with
            | Error error -> Error error
            | Ok targetPath ->
                let relativeTarget = relativeRepoPath cfg.RepoRoot targetPath
                let effectiveStatus = effectiveDocStatus cfg index targetPath
                if not (File.Exists targetPath) then
                    Error (sprintf "dispose(merge) target '%s' does not exist" target)
                elif isInboxPath cfg targetPath then
                    Error (sprintf "dispose(merge) target '%s' must be an active canonical doc, not inbox content" relativeTarget)
                elif not (isUnderConfiguredDocDir cfg targetPath) then
                    Error (sprintf "dispose(merge) target '%s' must stay under a configured knowledge doc dir" target)
                elif not (isIndexedFile index targetPath) then
                    Error (sprintf "dispose(merge) target '%s' must resolve to an indexed canonical doc" relativeTarget)
                elif effectiveStatus <> "active" then
                    Error (sprintf "dispose(merge) target '%s' is not an active canonical doc (status: %s)" relativeTarget effectiveStatus)
                else
                    Ok targetPath

    type private CanonicalCommitOutcome =
        | CanonicalWritten
        | CanonicalConflict of string

    type private MergeCommitOutcome =
        | MergeWritten
        | MergeAlreadyPresent
        | MergeConflict of string

    let private mergeConflictMessage =
        "dispose(merge) target changed concurrently before commit; retry against the latest canonical doc"

    let private reverifyApplyConflictMessage =
        "reverify(apply:true) target changed concurrently before commit; retry against the latest canonical doc"

    let private snapshotHash (text: string) =
        let bytes = Encoding.UTF8.GetBytes(text)
        SHA256.HashData(bytes)
        |> Convert.ToHexString
        |> fun value -> value.ToLowerInvariant()

    let private mergeIdentity (relativeInboxPath: string) (source: string) (cycle: string) (body: string) =
        snapshotHash (String.concat "\n" [ relativeInboxPath.Trim(); source.Trim(); cycle.Trim(); body.Trim() ])

    let private mergeMarker (mergeId: string) = sprintf "<!-- ks-merge:%s -->" mergeId

    let private mergeHeading (source: string) (cycle: string) =
        let actualSource =
            if String.IsNullOrWhiteSpace(source) then "unknown"
            else source.Trim()

        if String.IsNullOrWhiteSpace(cycle) then
            sprintf "### Corroboration (%s)" actualSource
        else
            sprintf "### Corroboration (%s, cycle %s)" actualSource (cycle.Trim())

    let private mergeBlock (relativeInboxPath: string) (source: string) (cycle: string) (body: string) =
        let mergeId = mergeIdentity relativeInboxPath source cycle body
        let marker = mergeMarker mergeId
        let heading = mergeHeading source cycle
        let trimmedBody = body.Trim()
        let block =
            if String.IsNullOrWhiteSpace(trimmedBody) then
                String.concat "\n" [ marker; heading ]
            else
                String.concat "\n\n" [ String.concat "\n" [ marker; heading ]; trimmedBody ]
        mergeId, marker, block

    let private appendMergeBlock (canonicalBody: string) (block: string) =
        let trimmedCanonical = canonicalBody.Trim()
        if String.IsNullOrWhiteSpace(trimmedCanonical) then block
        else trimmedCanonical + "\n\n" + block

    let private readLockedFileContent (stream: FileStream) =
        stream.Position <- 0L
        use reader = new StreamReader(stream, Encoding.UTF8, true, 1024, true)
        reader.ReadToEnd()

    let private writeLockedFileContent (stream: FileStream) (content: string) =
        stream.SetLength(0L)
        stream.Position <- 0L
        use writer = new StreamWriter(stream, new UTF8Encoding(false), 1024, true)
        writer.Write(content)
        writer.Flush()
        stream.Flush(true)

    let private commitCanonicalContent (path: string) (baselineContent: string) (updatedContent: string) (conflictMessage: string) =
        try
            use lockedStream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
            let currentContent = readLockedFileContent lockedStream
            if currentContent = baselineContent then
                writeLockedFileContent lockedStream updatedContent
                CanonicalWritten
            else
                CanonicalConflict conflictMessage
        with
        | :? IOException ->
            CanonicalConflict conflictMessage
        | ex ->
            CanonicalConflict ex.Message

    let private rollbackWriteSeamFailure (cfg: KnowledgeSightConfig) (refreshState: DocIndex -> DocChunk[] option -> unit)
                                         (baselineIndex: DocIndex) (baselineChunks: DocChunk[] option)
                                         (restore: unit -> unit) (error: string) =
        try
            restore()
            match IndexingWorkflow.rebuild cfg with
            | Ok (restoredIndex, restoredChunks) ->
                refreshState restoredIndex restoredChunks
                mdict [ "error", box error ]
            | Error rollbackError ->
                refreshState baselineIndex baselineChunks
                mdict [ "error", box (sprintf "%s (rollback rebuild failed: %s)" error rollbackError) ]
        with ex ->
            refreshState baselineIndex baselineChunks
            mdict [ "error", box (sprintf "%s (rollback failed: %s)" error ex.Message) ]

    let private commitMergeContent (path: string) (baselineContent: string) (updatedContent: string) (marker: string) =
        if baselineContent.Contains(marker, StringComparison.Ordinal) then
            MergeAlreadyPresent
        else
            try
                use lockedStream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
                let currentContent = readLockedFileContent lockedStream
                if currentContent = baselineContent then
                    writeLockedFileContent lockedStream updatedContent
                    MergeWritten
                elif currentContent.Contains(marker, StringComparison.Ordinal) then
                    MergeAlreadyPresent
                else
                    MergeConflict mergeConflictMessage
            with
            | :? IOException ->
                MergeConflict mergeConflictMessage
            | ex ->
                MergeConflict ex.Message

    let private primaryChunkIndexForFile (index: DocIndex) (filePath: string) =
        index.Chunks
        |> Array.indexed
        |> Array.filter (fun (_, chunk) -> String.Equals(chunk.FilePath, filePath, StringComparison.OrdinalIgnoreCase))
        |> Array.sortBy (fun (_, chunk) -> chunk.Level, chunk.StartLine)
        |> Array.tryHead
        |> Option.map fst

    let private conflictAnchorChunkIndexForFile (index: DocIndex) (filePath: string) =
        index.Chunks
        |> Array.indexed
        |> Array.filter (fun (_, chunk) -> String.Equals(chunk.FilePath, filePath, StringComparison.OrdinalIgnoreCase))
        |> Array.sortBy (fun (_, chunk) -> chunk.Level, chunk.StartLine)
        |> Array.tryFind (fun (chunkIndex, _) ->
            chunkIndex < index.Embeddings.Length
            && index.Embeddings.[chunkIndex].Length > 0)
        |> Option.map fst

    let private stableRepoPath (cfg: KnowledgeSightConfig) (path: string) =
        if String.IsNullOrWhiteSpace(path) then ""
        elif Path.IsPathRooted(path) then relativeRepoPath cfg.RepoRoot path
        else path.Replace("\\", "/")

    let private normalizeScopeSelector (selector: string) =
        let normalized =
            if isNull selector then ""
            else selector.Trim().Replace("\\", "/")
        if normalized.StartsWith("./", StringComparison.Ordinal) then normalized.Substring(2)
        else normalized

    let private scopeSelectorHasWildcard (selector: string) =
        selector.IndexOfAny([| '*'; '?' |]) >= 0

    let private scopeSelectorRoot (selector: string) =
        let normalized = normalizeScopeSelector selector
        let wildcardIndexes =
            [| normalized.IndexOf('*'); normalized.IndexOf('?') |]
            |> Array.filter (fun index -> index >= 0)

        let root =
            if wildcardIndexes.Length = 0 then normalized
            else normalized.Substring(0, Array.min wildcardIndexes)

        root.TrimEnd('/')

    let private isSelectorUnderAllowedDirs (allowedDirs: string[]) (selector: string) =
        let normalizedSelector = normalizeConfigRelativeDir selector
        not (String.IsNullOrWhiteSpace(normalizedSelector))
        && (allowedDirs
            |> Array.map normalizeConfigRelativeDir
            |> Array.exists (fun dir ->
                dir = "."
                || String.Equals(normalizedSelector, dir, StringComparison.OrdinalIgnoreCase)
                || normalizedSelector.StartsWith(dir + "/", StringComparison.OrdinalIgnoreCase)))

    let private isSelectorUnderConfiguredDocDir (cfg: KnowledgeSightConfig) (selector: string) =
        isSelectorUnderAllowedDirs cfg.DocDirs selector

    let private scopeSelectorRegex (selector: string) =
        let pattern = normalizeScopeSelector selector
        let builder = StringBuilder("^")
        let mutable i = 0

        while i < pattern.Length do
            match pattern.[i] with
            | '*' when i + 1 < pattern.Length && pattern.[i + 1] = '*' ->
                if i + 2 < pattern.Length && pattern.[i + 2] = '/' then
                    builder.Append("(?:.*/)?") |> ignore
                    i <- i + 3
                else
                    builder.Append(".*") |> ignore
                    i <- i + 2
            | '*' ->
                builder.Append("[^/]*") |> ignore
                i <- i + 1
            | '?' ->
                builder.Append("[^/]") |> ignore
                i <- i + 1
            | ch ->
                builder.Append(Regex.Escape(string ch)) |> ignore
                i <- i + 1

        builder.Append("$") |> ignore
        Regex(builder.ToString(), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)

    let private verifyContentHash (content: string) =
        snapshotHash (if isNull content then "" else content)

    let private verifyChunkDict (cfg: KnowledgeSightConfig) (path: string) (heading: string) (startLine: int) (content: string) =
        mdict [
            "path", box (stableRepoPath cfg path)
            "heading", box heading
            "startLine", box startLine
            "contentHash", box (verifyContentHash content)
        ]

    let private verifyJsStr (opts: Jint.Native.JsValue) (key: string) (def: string) =
        if isNull (box opts) || opts.IsUndefined() || opts.IsNull() then def
        elif opts.IsObject() then
            let prop = opts.AsObject().Get(key)
            if prop.IsUndefined() || prop.IsNull() then def else prop.AsString()
        else def

    let private verifyJsInt (opts: Jint.Native.JsValue) (key: string) (def: int) =
        if isNull (box opts) || opts.IsUndefined() || opts.IsNull() then def
        elif opts.IsObject() then
            let prop = opts.AsObject().Get(key)
            if prop.IsUndefined() || prop.IsNull() then def else int (prop.AsNumber())
        else def

    let private verifyJsStrArray (opts: Jint.Native.JsValue) (key: string) (def: string[]) =
        if isNull (box opts) || opts.IsUndefined() || opts.IsNull() then def
        elif opts.IsObject() then
            let prop = opts.AsObject().Get(key)
            if prop.IsUndefined() || prop.IsNull() then def
            elif prop.IsArray() then
                match prop.ToObject() with
                | :? (obj array) as values -> values |> Array.map string
                | :? System.Collections.IEnumerable as values -> values |> Seq.cast<obj> |> Seq.map string |> Seq.toArray
                | _ -> def
            else
                [| prop.AsString() |]
        else def

    let private ensureVerifySourceChunks (chunks: DocChunk[] option) =
        match chunks with
        | Some loaded -> loaded
        | None -> invalidOp "verify sandbox requires cached source chunks; run 'knowledge-sight index' first"

    let private verifyFiles (cfg: KnowledgeSightConfig) (index: DocIndex) (pattern: string) =
        index.Chunks
        |> Array.groupBy (fun c -> c.FilePath)
        |> Array.choose (fun (filePath, _) ->
            let fileName = Path.GetFileName(filePath)
            if String.IsNullOrWhiteSpace(pattern)
               || fileName.Contains(pattern, StringComparison.OrdinalIgnoreCase)
               || pathContainsPattern filePath pattern then
                Some (mdict [ "path", box (stableRepoPath cfg filePath) ])
            else
                None)
        |> Array.sortBy (fun item -> string item.["path"])

    let private verifyGrep (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option) (pattern: string) (limit: int) (filePattern: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        let sourceChunks = ensureVerifySourceChunks chunks
        let regex =
            try Regex(pattern, RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
            with _ -> Regex(Regex.Escape(pattern), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
        let results = ResizeArray<Dictionary<string, obj>>()
        for i in 0 .. index.Chunks.Length - 1 do
            if results.Count < limit then
                let entry = index.Chunks.[i]
                if matchesDocStatus cfg index allowedStatuses entry.FilePath
                   && pathContainsPattern entry.FilePath filePattern then
                    match findSource (Some sourceChunks) entry with
                    | Some chunk when regex.IsMatch(chunk.Content) ->
                        results.Add(verifyChunkDict cfg entry.FilePath entry.Heading entry.StartLine chunk.Content)
                    | _ -> ()
        results.ToArray()

    let private verifyMentions (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option) (term: string) (limit: int) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        let sourceChunks = ensureVerifySourceChunks chunks
        let regex = Regex(sprintf @"\b%s\b" (Regex.Escape term), RegexOptions.IgnoreCase ||| RegexOptions.Compiled)
        let results = ResizeArray<Dictionary<string, obj>>()
        for i in 0 .. index.Chunks.Length - 1 do
            if results.Count < limit then
                let entry = index.Chunks.[i]
                if matchesDocStatus cfg index allowedStatuses entry.FilePath then
                    match findSource (Some sourceChunks) entry with
                    | Some chunk when regex.IsMatch(chunk.Content) ->
                        results.Add(verifyChunkDict cfg entry.FilePath entry.Heading entry.StartLine chunk.Content)
                    | _ -> ()
        results.ToArray()

    let private verifyBacklinks (cfg: KnowledgeSightConfig) (index: DocIndex) (fileName: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        IndexStore.backlinks index fileName
        |> Array.filter (fun link -> matchesDocStatus cfg index allowedStatuses link.SourceFile)
        |> Array.map (fun link ->
            mdict [
                "path", box (stableRepoPath cfg link.SourceFile)
                "heading", box link.SourceHeading
                "startLine", box link.Line
            ])

    let private verifyLinks (cfg: KnowledgeSightConfig) (index: DocIndex) (fileName: string) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        IndexStore.outlinks index fileName
        |> Array.filter (fun link -> matchesDocStatus cfg index allowedStatuses link.SourceFile)
        |> Array.map (fun link ->
            mdict [
                "path", box (if link.TargetResolved <> "" then stableRepoPath cfg link.TargetResolved else link.TargetPath.Replace("\\", "/"))
                "heading", box link.SourceHeading
                "startLine", box link.Line
            ])

    let private verifyContext (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option) (fileName: string) =
        let sourceChunks = ensureVerifySourceChunks chunks
        let matchingFiles =
            index.Chunks
            |> Array.map (fun c -> c.FilePath.Replace("\\", "/"))
            |> Array.distinct
            |> Array.filter (fun fp -> IndexStore.matchFile fp fileName)
        if matchingFiles.Length > 1 then
            invalidOp (sprintf "verify sandbox context('%s') is ambiguous; use a more specific path" fileName)
        elif matchingFiles.Length = 0 then
            invalidOp (sprintf "verify sandbox context('%s') did not match any file" fileName)
        else
            let resolvedFile = matchingFiles.[0]
            let fileChunks = IndexStore.fileChunks index resolvedFile
            let backlinks = IndexStore.backlinks index resolvedFile
            let outlinks = IndexStore.outlinks index resolvedFile
            let fm = frontmatterForFile index resolvedFile
            mdict [
                "path", box (stableRepoPath cfg resolvedFile)
                "title", box (fm |> Option.map (fun f -> f.Title) |> Option.defaultValue "")
                "status", box (effectiveDocStatus cfg index resolvedFile)
                "tags", box (fm |> Option.map (fun f -> f.Tags) |> Option.defaultValue [||])
                "related", box (fm |> Option.map (fun f -> f.Related |> Array.map (stableRepoPath cfg)) |> Option.defaultValue [||])
                "frontmatter", box (fm |> Option.map verifyVisibleFrontmatterToDict |> Option.defaultValue (mdict []))
                "sections", box (fileChunks |> Array.choose (fun (_, chunk) ->
                    findSource (Some sourceChunks) chunk
                    |> Option.map (fun source -> verifyChunkDict cfg chunk.FilePath chunk.Heading chunk.StartLine source.Content)))
                "backlinks", box (backlinks |> Array.map (fun link ->
                    mdict [
                        "path", box (stableRepoPath cfg link.SourceFile)
                        "heading", box link.SourceHeading
                        "startLine", box link.Line
                    ]))
                "outlinks", box (outlinks |> Array.map (fun link ->
                    mdict [
                        "path", box (if link.TargetResolved <> "" then stableRepoPath cfg link.TargetResolved else link.TargetPath.Replace("\\", "/"))
                        "heading", box link.SourceHeading
                        "startLine", box link.Line
                    ]))
            ]

    let private unstableVerifyKeys = Set.ofList [ "id"; "score"; "__ks_source__" ]

    let rec private canonicalizeVerifyNode (node: JsonNode) =
        match node with
        | null -> null
        | :? JsonObject as obj ->
            let canonical = JsonObject()
            obj
            |> Seq.filter (fun kv -> not (unstableVerifyKeys.Contains kv.Key))
            |> Seq.sortBy (fun kv -> kv.Key)
            |> Seq.iter (fun kv -> canonical.Add(kv.Key, canonicalizeVerifyNode kv.Value))
            canonical :> JsonNode
        | :? JsonArray as arr ->
            let canonical = JsonArray()
            arr
            |> Seq.map canonicalizeVerifyNode
            |> Seq.sortBy (fun value -> if isNull value then "null" else value.ToJsonString())
            |> Seq.iter canonical.Add
            canonical :> JsonNode
        | _ -> node.DeepClone()

    let private canonicalizeVerifyValue (value: obj) =
        let node =
            if isNull value then null
            else JsonSerializer.SerializeToNode(value)
        let canonical = canonicalizeVerifyNode node
        if isNull canonical then "null"
        else canonical.ToJsonString()

    let private verifySearchCacheField = "verify_search_cache"

    type private VerifySearchEvaluation = {
        Snapshot: string
        UsedSearch: bool
        SearchCacheEntries: (string * float32[])[]
    }

    let private canonicalVerifySearchStatuses (statuses: string[]) =
        normalizeRequestedStatuses statuses
        |> Set.toArray
        |> Array.sort

    let private verifySearchCacheKey (query: string) (limit: int) (tag: string) (filePattern: string) (statuses: string[]) =
        JsonSerializer.Serialize(
            {|
                query = if isNull query then "" else query
                limit = limit
                tag = if isNull tag then "" else tag
                file = if isNull filePattern then "" else filePattern
                statuses = canonicalVerifySearchStatuses statuses
            |})

    let private readVerifySearchCache (frontmatter: Frontmatter option) =
        let empty () = Dictionary<string, float32[]>(StringComparer.Ordinal)

        match frontmatter |> Option.bind (fun value -> frontmatterScalar value verifySearchCacheField) with
        | None -> Ok (empty ())
        | Some raw ->
            try
                use doc = JsonDocument.Parse(decodeStoredJsonScalar raw)
                if doc.RootElement.ValueKind <> JsonValueKind.Array then
                    Error "verify_search_cache must be a JSON array"
                else
                    let cache = empty ()
                    for entry in doc.RootElement.EnumerateArray() do
                        let key =
                            match entry.TryGetProperty("key") with
                            | true, value when value.ValueKind = JsonValueKind.String ->
                                value.GetString()
                            | _ ->
                                raise (FormatException("verify_search_cache entries require a string key"))

                        if String.IsNullOrWhiteSpace(key) then
                            raise (FormatException("verify_search_cache entries require a non-empty key"))

                        let embedding =
                            match entry.TryGetProperty("embedding") with
                            | true, value when value.ValueKind = JsonValueKind.Array ->
                                value.EnumerateArray()
                                |> Seq.map (fun number ->
                                    let mutable actual = 0.0f
                                    if number.TryGetSingle(&actual) then actual
                                    else raise (FormatException("verify_search_cache embeddings must contain only numbers")))
                                |> Seq.toArray
                            | _ ->
                                raise (FormatException("verify_search_cache entries require an embedding array"))

                        if embedding.Length = 0 then
                            raise (FormatException("verify_search_cache embeddings must be non-empty"))

                        cache.[key] <- embedding

                    Ok cache
            with ex ->
                Error (sprintf "verify_search_cache is invalid: %s" ex.Message)

    let private serializeVerifySearchCache (entries: (string * float32[])[]) =
        entries
        |> Array.sortBy fst
        |> Array.map (fun (key, embedding) -> {| key = key; embedding = embedding |})
        |> JsonSerializer.Serialize

    let private applyVerifySearchCache (evaluation: VerifySearchEvaluation) (frontmatter: Frontmatter) =
        if evaluation.UsedSearch then
            frontmatter
            |> setFrontmatterField verifySearchCacheField (Scalar (serializeVerifySearchCache evaluation.SearchCacheEntries))
        else
            frontmatter
            |> removeFrontmatterFields [| verifySearchCacheField |]

    let private verifySearch (cfg: KnowledgeSightConfig) (index: DocIndex) (cache: Dictionary<string, float32[]>)
                             (allowPopulate: bool) (markUsed: string * float32[] -> unit)
                             (query: string) (limit: int) (tag: string) (filePattern: string) (statuses: string[]) =
        let actualQuery = if isNull query then "" else query
        let key = verifySearchCacheKey actualQuery limit tag filePattern statuses

        let embedding =
            match cache.TryGetValue(key) with
            | true, persisted ->
                markUsed (key, persisted)
                persisted
            | _ when allowPopulate ->
                match embedQuery cfg.EmbeddingUrl actualQuery with
                | Some generated ->
                    cache.[key] <- generated
                    markUsed (key, generated)
                    generated
                | None ->
                    raise (InvalidOperationException("embedding server not available — search() in verify cannot capture deterministic query embeddings"))
            | _ ->
                raise (InvalidOperationException("search() in verify requires persisted deterministic query embeddings; refresh the doc through promote/merge/supersede before reverify"))

        searchWithEmbedding
            cfg
            index
            (fun _ chunk -> IndexStore.chunkId chunk.FilePath chunk.Heading chunk.StartLine)
            embedding
            limit
            tag
            filePattern
            statuses

    let private createVerifyEngine (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option)
                                   (cache: Dictionary<string, float32[]>) (allowPopulateSearchCache: bool)
                                   (markSearchUsed: string * float32[] -> unit) =
        let engine = new Engine()
        engine.Execute(QueryHelpers.compositionHelpersJs) |> ignore
        engine.Execute("globalThis.Date = undefined; Math.random = function(){ throw new Error('Math.random is not allowed in verify'); };") |> ignore

        engine.SetValue("grep", Func<string, Jint.Native.JsValue, obj>(fun pattern opts ->
            let limit = verifyJsInt opts "limit" 10
            let file = verifyJsStr opts "file" ""
            let statuses = verifyJsStrArray opts "status" retrievalDefaultStatuses
            box (verifyGrep cfg index chunks pattern limit file statuses))) |> ignore

        engine.SetValue("mentions", Func<string, Jint.Native.JsValue, obj>(fun term opts ->
            let limit = verifyJsInt opts "limit" 20
            let statuses = verifyJsStrArray opts "status" retrievalDefaultStatuses
            box (verifyMentions cfg index chunks term limit statuses))) |> ignore

        engine.SetValue("files", Func<string, obj>(fun pattern ->
            let actualPattern = if isNull pattern then "" else pattern
            box (verifyFiles cfg index actualPattern))) |> ignore

        engine.SetValue("backlinks", Func<string, Jint.Native.JsValue, obj>(fun file opts ->
            let statuses = verifyJsStrArray opts "status" retrievalDefaultStatuses
            box (verifyBacklinks cfg index file statuses))) |> ignore

        engine.SetValue("links", Func<string, Jint.Native.JsValue, obj>(fun file opts ->
            let statuses = verifyJsStrArray opts "status" retrievalDefaultStatuses
            box (verifyLinks cfg index file statuses))) |> ignore

        engine.SetValue("context", Func<string, obj>(fun file ->
            box (verifyContext cfg index chunks file))) |> ignore

        engine.SetValue("search", Func<string, Jint.Native.JsValue, obj>(fun query opts ->
            let limit = verifyJsInt opts "limit" 10
            let tag = verifyJsStr opts "tag" ""
            let file = verifyJsStr opts "file" ""
            let statuses = verifyJsStrArray opts "status" retrievalDefaultStatuses
            box (verifySearch cfg index cache allowPopulateSearchCache markSearchUsed query limit tag file statuses))) |> ignore

        engine

    let private evaluateVerifyExpression (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option)
                                         (frontmatter: Frontmatter option) (allowPopulateSearchCache: bool) (expression: string) =
        match readVerifySearchCache frontmatter with
        | Error error -> Error error
        | Ok cache ->
            try
                let usedEntries = Dictionary<string, float32[]>(StringComparer.Ordinal)
                let mutable usedSearch = false
                let markSearchUsed (key, embedding) =
                    usedSearch <- true
                    usedEntries.[key] <- embedding

                let engine = createVerifyEngine cfg index chunks cache allowPopulateSearchCache markSearchUsed
                let wrapped = QueryHelpers.wrapIIFE (decodeStoredVerifyExpression expression)
                let value = engine.Evaluate(wrapped).ToObject()
                let payload = canonicalizeVerifyValue value
                Ok {
                    Snapshot = snapshotHash payload
                    UsedSearch = usedSearch
                    SearchCacheEntries = usedEntries |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toArray
                }
            with ex ->
                Error ex.Message

    let private persistVerifyMetadata (cfg: KnowledgeSightConfig) (path: string) (frontmatter: Frontmatter) =
        let body =
            if File.Exists path then markdownBodyWithoutFrontmatter path
            else ""

        writeMarkdownFile path frontmatter body
        IndexingWorkflow.rebuild cfg

    let private maybePersistVerifySnapshot (cfg: KnowledgeSightConfig) (index: DocIndex) (chunks: DocChunk[] option) (path: string) =
        match frontmatterForFile index path with
        | None -> Ok (index, chunks)
        | Some frontmatter when effectiveDocStatus cfg index path <> "active" -> Ok (index, chunks)
        | Some frontmatter ->
            match frontmatterScalar frontmatter "verify" with
            | None
            | Some "" -> Ok (index, chunks)
            | Some verifyExpr ->
                let requiresDeterministicSearch = verifyExpressionUsesSearch verifyExpr
                match evaluateVerifyExpression cfg index chunks (Some frontmatter) true verifyExpr with
                | Error error when requiresDeterministicSearch -> Error error
                | Error _ -> Ok (index, chunks)
                | Ok evaluation ->
                    let updatedFrontmatter =
                        frontmatter
                        |> applyVerifySearchCache evaluation
                        |> setFrontmatterField "verify_snapshot" (Scalar evaluation.Snapshot)

                    if updatedFrontmatter = frontmatter then Ok (index, chunks)
                    else
                        match persistVerifyMetadata cfg path updatedFrontmatter with
                        | Error error -> Error error
                        | Ok (updatedIndex, updatedChunks) ->
                            let currentFrontmatter = frontmatterForFile updatedIndex path
                            match evaluateVerifyExpression cfg updatedIndex updatedChunks currentFrontmatter false verifyExpr with
                            | Error error when requiresDeterministicSearch -> Error error
                            | Error _ -> Ok (updatedIndex, updatedChunks)
                            | Ok stabilizedEvaluation ->
                                match currentFrontmatter with
                                | None -> Ok (updatedIndex, updatedChunks)
                                | Some persistedFrontmatter ->
                                    let stabilizedFrontmatter =
                                        persistedFrontmatter
                                        |> applyVerifySearchCache stabilizedEvaluation
                                        |> setFrontmatterField "verify_snapshot" (Scalar stabilizedEvaluation.Snapshot)

                                    if stabilizedFrontmatter = persistedFrontmatter then
                                        Ok (updatedIndex, updatedChunks)
                                    else
                                        match persistVerifyMetadata cfg path stabilizedFrontmatter with
                                        | Error error -> Error error
                                        | Ok (finalIndex, finalChunks) -> Ok (finalIndex, finalChunks)

    let private applyReverifyStaleStatus (cfg: KnowledgeSightConfig) (path: string) =
        try
            let baselineContent, baselineFrontmatter, baselineBody = readMarkdownFile path
            match baselineFrontmatter with
            | None ->
                Error "reverify(apply:true) target is missing frontmatter"
            | Some frontmatter ->
                let updatedContent =
                    frontmatter
                    |> setFrontmatterField "status" (Scalar "stale")
                    |> Some
                    |> fun updatedFrontmatter -> renderMarkdownContent updatedFrontmatter baselineBody

                match commitCanonicalContent path baselineContent updatedContent reverifyApplyConflictMessage with
                | CanonicalConflict error -> Error error
                | CanonicalWritten -> IndexingWorkflow.rebuild cfg
        with
        | :? IOException ->
            Error reverifyApplyConflictMessage

    let reverify (cfg: KnowledgeSightConfig) (session: QuerySession) (refreshState: DocIndex -> DocChunk[] option -> unit) (index: DocIndex) (chunks: DocChunk[] option) (scope: string[]) (apply: bool) =
        let mkResult refId path outcome =
            mdict [ "ref", box refId; "path", box path; "outcome", box outcome ]

        let clearTransientFields (result: Dictionary<string, obj>) =
            for key in [| "__verifyExpr"; "__snapshot"; "__path" |] do
                result.Remove(key) |> ignore
            result

        let allScopedDocs =
            index.Frontmatters
            |> Map.toArray
            |> Array.choose (fun (path, frontmatter) ->
                if isUnderConfiguredDocDir cfg path then
                    Some (
                        path,
                        stableRepoPath cfg path,
                        frontmatter,
                        frontmatterScalar frontmatter "verify",
                        frontmatterScalar frontmatter "verify_snapshot" |> Option.defaultValue ""
                    )
                else
                    None)
            |> Array.sortBy (fun (_, relativePath, _, _, _) -> relativePath)

        let mkPendingTarget path verifyExpr snapshot =
            let refId =
                primaryChunkIndexForFile index path
                |> Option.map session.NextRef
                |> Option.defaultValue ""
            let result = mkResult refId (stableRepoPath cfg path) "pending"
            result.["__verifyExpr"] <- box verifyExpr
            result.["__snapshot"] <- box snapshot
            result.["__path"] <- box path
            result

        let supportedTargets =
            allScopedDocs
            |> Array.choose (fun (path, _, _, verifyExpr, snapshot) ->
                match verifyExpr with
                | Some value when effectiveDocStatus cfg index path = "active" && not (String.IsNullOrWhiteSpace(value)) ->
                    Some (path, value, snapshot)
                | _ -> None)

        let defaultTargets =
            supportedTargets
            |> Array.map (fun (path, verifyExpr, snapshot) -> mkPendingTarget path verifyExpr snapshot)

        let explicitTargets =
            let selectorErrors = ResizeArray<Dictionary<string, obj>>()
            let selectedTargets = Dictionary<string, string * string>(StringComparer.OrdinalIgnoreCase)

            for requested in scope do
                let rawSelector = if isNull requested then "" else requested
                let trimmed = rawSelector.Trim()
                let normalized = normalizeScopeSelector trimmed

                let error message =
                    selectorErrors.Add(mdict [ "ref", box rawSelector; "outcome", box "error"; "error", box message ])

                if trimmed = "" then
                    error "scope selectors must be non-empty repo-relative canonical paths or globs in this wave"
                elif Path.IsPathRooted(trimmed) then
                    error "scope selectors must be repo-relative canonical paths or globs in this wave"
                elif Regex.IsMatch(normalized, @"^R\d+$") then
                    error "scope selectors must be stable repo-relative canonical paths or globs in this wave"
                else
                    let selectorRoot = scopeSelectorRoot normalized
                    if not (isSelectorUnderConfiguredDocDir cfg selectorRoot) then
                        error "scope selectors must stay under configured knowledge doc dirs in this wave"
                    else
                        let matches =
                            if scopeSelectorHasWildcard normalized then
                                let matcher = scopeSelectorRegex normalized
                                allScopedDocs
                                |> Array.filter (fun (_, relativePath, _, _, _) -> matcher.IsMatch(relativePath))
                            else
                                let normalizedExact = normalized.TrimEnd('/')
                                let exactMatches =
                                    allScopedDocs
                                    |> Array.filter (fun (_, relativePath, _, _, _) ->
                                        String.Equals(relativePath, normalizedExact, StringComparison.OrdinalIgnoreCase))
                                if exactMatches.Length > 0 then exactMatches
                                else
                                    allScopedDocs
                                    |> Array.filter (fun (_, relativePath, _, _, _) ->
                                        relativePath.StartsWith(normalizedExact + "/", StringComparison.OrdinalIgnoreCase))

                        let supportedMatches =
                            matches
                            |> Array.choose (fun (path, _, _, verifyExpr, snapshot) ->
                                match verifyExpr with
                                | Some value when effectiveDocStatus cfg index path = "active" && not (String.IsNullOrWhiteSpace(value)) ->
                                    Some (path, value, snapshot)
                                | _ -> None)

                        if supportedMatches.Length = 0 then
                            error (sprintf "scope selector '%s' did not match any active canonical docs with verify in this wave" normalized)
                        else
                            supportedMatches
                            |> Array.iter (fun (path, verifyExpr, snapshot) ->
                                if not (selectedTargets.ContainsKey(path)) then
                                    selectedTargets.[path] <- (verifyExpr, snapshot))

            let targetResults =
                selectedTargets
                |> Seq.map (fun kv -> kv.Key, fst kv.Value, snd kv.Value)
                |> Seq.sortBy (fun (path, _, _) -> stableRepoPath cfg path)
                |> Seq.map (fun (path, verifyExpr, snapshot) -> mkPendingTarget path verifyExpr snapshot)
                |> Seq.toArray

            Array.append (selectorErrors.ToArray()) targetResults

        let targets =
            if scope.Length = 0 then
                defaultTargets
            else
                explicitTargets

        let mutable latestIndex = index
        let mutable latestChunks = chunks
        let mutable stateChanged = false

        let results =
            targets
            |> Array.map (fun result ->
                match result.TryGetValue("__verifyExpr") with
                | true, verifyObj ->
                    let verifyExpr = string verifyObj
                    let path =
                        match result.TryGetValue("__path") with
                        | true, value when not (isNull value) -> string value
                        | _ -> ""
                    let storedSnapshot =
                        match result.TryGetValue("__snapshot") with
                        | true, value when not (isNull value) -> string value
                        | _ -> ""
                    let verifyFrontmatter = frontmatterForFile index path
                    match evaluateVerifyExpression cfg index chunks verifyFrontmatter false verifyExpr with
                    | Error error ->
                        clearTransientFields result |> ignore
                        result.["outcome"] <- box "error"
                        result.["error"] <- box error
                        result
                    | Ok evaluation ->
                        let currentSnapshot = evaluation.Snapshot
                        if String.IsNullOrWhiteSpace(storedSnapshot) then
                            clearTransientFields result |> ignore
                            result.["outcome"] <- box "no_snapshot"
                            result.["now"] <- box currentSnapshot
                            result
                        elif String.Equals(storedSnapshot, currentSnapshot, StringComparison.OrdinalIgnoreCase) then
                            clearTransientFields result |> ignore
                            result.["outcome"] <- box "ok"
                            result.["was"] <- box storedSnapshot
                            result.["now"] <- box currentSnapshot
                            result
                        else
                            result.["outcome"] <- box "drift"
                            result.["was"] <- box storedSnapshot
                            result.["now"] <- box currentSnapshot
                            if apply then
                                match applyReverifyStaleStatus cfg path with
                                | Ok (updatedIndex, updatedChunks) ->
                                    latestIndex <- updatedIndex
                                    latestChunks <- updatedChunks
                                    stateChanged <- true
                                    result.["applied"] <- box "stale"
                                | Error error ->
                                    result.["error"] <- box error
                            clearTransientFields result |> ignore
                            result
                | _ -> result)

        if stateChanged then
            refreshState latestIndex latestChunks

        results

    let prune (cfg: KnowledgeSightConfig) (refreshState: DocIndex -> DocChunk[] option -> unit) (index: DocIndex) (scope: string[]) (olderThanDays: int) (apply: bool) =
        if olderThanDays < 0 then
            [| mdict [ "error", box "prune() olderThanDays must be >= 0 in this wave" ] |]
        else
            let now = DateTime.UtcNow

            let allScopedDocs =
                index.Frontmatters
                |> Map.toArray
                |> Array.choose (fun (path, _) ->
                    if isUnderConfiguredDocDir cfg path then
                        let status = effectiveDocStatus cfg index path
                        if not (isInboxPath cfg path) && prunePreviewStatuses.Contains(status) then
                            let relativePath = stableRepoPath cfg path
                            Some (path, relativePath, normalizeScopeSelector relativePath, status)
                        else
                            None
                    else
                        None)
                |> Array.sortBy (fun (_, relativePath, _, _) -> relativePath)

            let selectorErrors = ResizeArray<Dictionary<string, obj>>()

            let selectedDocs =
                if scope.Length = 0 then
                    allScopedDocs
                else
                    let selected = Dictionary<string, string * string * string>(StringComparer.OrdinalIgnoreCase)

                    for requested in scope do
                        let rawSelector = if isNull requested then "" else requested
                        let trimmed = rawSelector.Trim()
                        let normalized = normalizeScopeSelector trimmed

                        let error message =
                            selectorErrors.Add(mdict [ "ref", box rawSelector; "outcome", box "error"; "error", box message ])

                        if trimmed = "" then
                            error "prune() scope selectors must be non-empty repo-relative canonical paths or globs in this wave"
                        elif Path.IsPathRooted(trimmed) then
                            error "prune() scope selectors must be repo-relative canonical paths or globs in this wave"
                        elif Regex.IsMatch(normalized, @"^R\d+$") then
                            error "prune() scope selectors must be stable repo-relative canonical paths or globs in this wave"
                        else
                            let selectorRoot = scopeSelectorRoot normalized

                            if not (isSelectorUnderConfiguredDocDir cfg selectorRoot) then
                                error "prune() scope selectors must stay under configured knowledge doc dirs in this wave"
                            else
                                let matches =
                                    if scopeSelectorHasWildcard normalized then
                                        let matcher = scopeSelectorRegex normalized
                                        allScopedDocs
                                        |> Array.filter (fun (_, _, scopePath, _) -> matcher.IsMatch(scopePath))
                                    else
                                        let normalizedExact = normalized.TrimEnd('/')
                                        let exactMatches =
                                            allScopedDocs
                                            |> Array.filter (fun (_, _, scopePath, _) ->
                                                String.Equals(scopePath, normalizedExact, StringComparison.OrdinalIgnoreCase))
                                        if exactMatches.Length > 0 then
                                            exactMatches
                                        else
                                            allScopedDocs
                                            |> Array.filter (fun (_, _, scopePath, _) ->
                                                scopePath.StartsWith(normalizedExact + "/", StringComparison.OrdinalIgnoreCase))

                                if matches.Length = 0 then
                                    error (sprintf "prune() scope selector '%s' did not match any non-live canonical docs in this wave" normalized)
                                else
                                    matches
                                    |> Array.iter (fun (path, relativePath, scopePath, status) ->
                                        if not (selected.ContainsKey(scopePath)) then
                                            selected.[scopePath] <- (path, relativePath, status))

                    selected
                    |> Seq.map (fun kv ->
                        let path, relativePath, status = kv.Value
                        path, relativePath, kv.Key, status)
                    |> Seq.sortBy (fun (_, relativePath, _, _) -> relativePath)
                    |> Seq.toArray

            let evaluations =
                selectedDocs
                |> Array.map (fun (path, relativePath, _, status) ->
                    let fullPath =
                        if Path.IsPathRooted(path) then path
                        else Path.Combine(cfg.RepoRoot, path.Replace("/", Path.DirectorySeparatorChar.ToString()))

                    let lastModifiedUtc =
                        try
                            if File.Exists(fullPath) then Some (File.GetLastWriteTimeUtc(fullPath)) else None
                        with _ ->
                            None

                    let ageDays =
                        lastModifiedUtc
                        |> Option.map (fun timestamp -> max 0 (int (Math.Floor((now - timestamp).TotalDays))))

                    let ageGuardPassed =
                        match lastModifiedUtc with
                        | Some timestamp -> (now - timestamp).TotalDays >= float olderThanDays
                        | None -> false

                    let backlinkSources =
                        IndexStore.backlinks index path
                        |> Array.map (fun link -> stableRepoPath cfg link.SourceFile)
                        |> Array.filter (fun source ->
                            not (String.IsNullOrWhiteSpace(source))
                            && not (String.Equals(source, relativePath, StringComparison.OrdinalIgnoreCase)))
                        |> Array.distinct
                        |> Array.sort

                    let backlinkGuardPassed = backlinkSources.Length = 0
                    let blockers = ResizeArray<string>()

                    if not ageGuardPassed then
                        match lastModifiedUtc with
                        | Some _ -> blockers.Add(sprintf "age guard requires at least %d day(s) since last modification" olderThanDays)
                        | None -> blockers.Add("age guard could not be evaluated because the indexed file is missing on disk")

                    if not backlinkGuardPassed then
                        blockers.Add(sprintf "backlink guard protects docs with incoming links (%d found)" backlinkSources.Length)

                    let previewOutcome = if blockers.Count = 0 then "candidate" else "blocked"
                    let result =
                        mdict [
                            "path", box relativePath
                            "status", box status
                            "preview", box true
                            "eligible", box (blockers.Count = 0)
                            "outcome", box previewOutcome
                            "olderThanDays", box olderThanDays
                            "lastModifiedUtc", box (lastModifiedUtc |> Option.map (fun timestamp -> timestamp.ToString("O")) |> Option.defaultValue "")
                            "ageDays", box (ageDays |> Option.defaultValue -1)
                            "ageGuardPassed", box ageGuardPassed
                            "backlinkCount", box backlinkSources.Length
                            "backlinks", box backlinkSources
                            "backlinkGuardPassed", box backlinkGuardPassed
                            "blockers", box (blockers.ToArray())
                        ]
                    fullPath, (blockers.Count = 0), result
                )

            let previewResults = evaluations |> Array.map (fun (_, _, result) -> result)

            if not apply then
                let combined = Array.append (selectorErrors.ToArray()) previewResults
                if combined.Length = 0 then
                    [| mdict [ "note", box "No non-live canonical docs matched prune() preview in this wave."; "preview", box true; "docs", box 0 ] |]
                else
                    combined
            else
                let mutable deletedCount = 0

                let appliedResults =
                    evaluations
                    |> Array.map (fun (fullPath, eligible, result) ->
                        let previewOutcome = string result.["outcome"]
                        result.["previewOutcome"] <- box previewOutcome
                        result.["apply"] <- box true

                        if not eligible then
                            result.["outcome"] <- box "blocked"
                            result
                        else
                            try
                                if not (File.Exists(fullPath)) then
                                    result.["outcome"] <- box "error"
                                    result.["error"] <- box "prune(apply:true) target missing before delete"
                                    result
                                else
                                    File.Delete(fullPath)
                                    deletedCount <- deletedCount + 1
                                    result.["outcome"] <- box "deleted"
                                    result.["applied"] <- box "deleted"
                                    result
                            with ex ->
                                result.["outcome"] <- box "error"
                                result.["error"] <- box (sprintf "prune(apply:true) could not delete target: %s" ex.Message)
                                result)

                let refreshResults =
                    if deletedCount = 0 then
                        [||]
                    else
                        match IndexingWorkflow.rebuild cfg with
                        | Ok (updatedIndex, updatedChunks) ->
                            refreshState updatedIndex updatedChunks
                            [||]
                        | Error error ->
                            [| mdict [ "outcome", box "error"; "apply", box true; "error", box (sprintf "prune(apply:true) deleted %d doc(s) but failed to refresh index: %s" deletedCount error) ] |]

                let combined = Array.concat [| selectorErrors.ToArray(); appliedResults; refreshResults |]
                if combined.Length = 0 then
                    [| mdict [ "note", box "No non-live canonical docs matched prune() apply in this wave."; "preview", box true; "apply", box true; "docs", box 0 ] |]
                else
                    combined

    let private claimPhrase (text: string) =
        let tokens =
            Regex.Matches(text, @"[A-Za-z0-9]{4,}")
            |> Seq.cast<Match>
            |> Seq.map (fun m -> m.Value)
            |> Seq.truncate 6
            |> Seq.toArray
        if tokens.Length = 0 then ""
        else String.concat " " tokens

    let private synthesizeVerify (index: DocIndex) (claimText: string) (placementPaths: string[]) =
        let phrase = claimPhrase claimText
        if phrase = "" then None
        else
            let escapedPhrase = phrase.Replace("'", "\\'")
            placementPaths
            |> Array.tryPick (fun path ->
                match frontmatterForFile index path with
                | Some frontmatter when frontmatter.Related.Length > 0 ->
                    let relatedPath = frontmatter.Related.[0].Replace("\\", "/")
                    Some (sprintf "grep('%s', {file:'%s'}).length > 0" escapedPhrase relatedPath)
                | _ -> None)

    let private createInboxFrontmatter (title: string) (team: string) (cycle: string) (confidence: string) (concept: string) (verify: string) (observable: string) (forbids: string) (suggestedTarget: string) =
        let baseFrontmatter =
            {
                Id = ""
                Title = title
                Status = "pending"
                Tags = [||]
                Related = [||]
                Extra = Map.empty
            }
            |> setFrontmatterField "source" (Scalar team)
            |> setFrontmatterField "cycle" (Scalar cycle)

        baseFrontmatter
        |> setFrontmatterScalarIfAny "confidence" confidence
        |> setFrontmatterScalarIfAny "concept" concept
        |> setFrontmatterScalarIfAny "verify" verify
        |> setFrontmatterScalarIfAny "observable" observable
        |> setFrontmatterScalarIfAny "forbids" forbids
        |> setFrontmatterScalarIfAny "suggested_target" suggestedTarget

    let propose (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (cfg: KnowledgeSightConfig) (refreshState: DocIndex -> DocChunk[] option -> unit)
                (text: string) (team: string) (cycle: string) (concept: string) (confidence: string) (verify: string) (observable: string) (forbids: string)
                (threshold: float) (dryRun: bool) =
        let normalizedTeam = normalizeTeam team
        let normalizedCycle = normalizeCycle cycle
        let paragraphs = splitKnowledgeParagraphs text
        let requireFieldsError =
            String.Equals((cfg.RequireFieldsMode |> Option.ofObj |> Option.defaultValue "").Trim(), "error", StringComparison.OrdinalIgnoreCase)

        if String.IsNullOrWhiteSpace(normalizedTeam) then
            [| mdict [ "status", box "blocked"; "score", box 0.0; "error", box "team is required" ] |]
        elif not (isValidCycle cycle) then
            [| mdict [ "status", box "blocked"; "score", box 0.0; "error", box "cycle must be a non-negative integer id or filename-safe UTC form yyyy-MM-ddTHH-mm-ssZ" ] |]
        elif paragraphs.Length = 0 then
            [| mdict [ "status", box "blocked"; "score", box 0.0; "error", box "no knowledge-like paragraphs found" ] |]
        else
            let cycle = normalizedCycle |> Option.defaultValue cycle
            match Config.resolveInboxDir cfg with
            | Error error ->
                [| mdict [ "status", box "blocked"; "score", box 0.0; "error", box error ] |]
            | Ok inboxDir ->
                let plannedWrites = ResizeArray<int * string * Frontmatter * string * string option * string[]>()
                let results = ResizeArray<Dictionary<string, obj>>()

                for paragraph in paragraphs do
                    let signal = knowledgeSignal paragraph index
                    let placement = writePipelinePlacement cfg index chunks cfg.EmbeddingUrl paragraph 3
                    let placementPaths = placement |> Array.map (fun (filePath, _, _) -> filePath)
                    let suggestedPath = placement |> Array.tryHead |> Option.map (fun (filePath, _, _) -> relativeRepoPath cfg.RepoRoot filePath)
                    let nearest = rankedSubsetMatches index chunks cfg.EmbeddingUrl (isWriteCanonicalFile cfg index) paragraph 1
                    let nearestIndex, nearestScore =
                        if nearest.Length > 0 then
                            let i, score = nearest.[0]
                            Some i, score
                        else
                            None, 0.0

                    let allowShortClaim = signal < 1 && allowShortProposeClaim paragraph

                    if signal < 1 && not allowShortClaim then
                        results.Add(mdict [
                            "status", box "blocked"
                            "score", box (Math.Round(nearestScore, 3))
                            "paragraph", box (titleFromText paragraph)
                            "reason", box "paragraph does not look knowledge-like enough to file"
                        ])
                    elif nearestScore >= threshold then
                        let nearestRef =
                            nearestIndex
                            |> Option.map session.NextRef
                            |> Option.defaultValue ""
                        results.Add(mdict [
                            "status", box "known"
                            "score", box (Math.Round(nearestScore, 3))
                            "nearest", box nearestRef
                            "suggestedTarget", box (suggestedPath |> Option.defaultValue "")
                            "paragraph", box (titleFromText paragraph)
                        ])
                    else
                        let title = titleFromText paragraph
                        let slug = slugify title
                        let inboxDirectory = Path.Combine(cfg.RepoRoot, inboxDir, normalizedTeam)
                        let plannedPath = nextAvailablePath (Path.Combine(inboxDirectory, sprintf "%s-%s.md" cycle slug))
                        let suggestedVerify =
                            if String.IsNullOrWhiteSpace(verify) then synthesizeVerify index paragraph placementPaths
                            else None
                        let frontmatter = createInboxFrontmatter title normalizedTeam cycle confidence concept verify observable forbids (suggestedPath |> Option.defaultValue "")
                        let missingFields = missingRequiredFields cfg frontmatter
                        let warnings = missingFields |> Array.map (fun field -> "no_" + field)

                        if requireFieldsError && missingFields.Length > 0 then
                            let blockedResult =
                                mdict [
                                    "status", box "blocked"
                                    "score", box (Math.Round(nearestScore, 3))
                                    "suggestedTarget", box (suggestedPath |> Option.defaultValue "")
                                    "warnings", box warnings
                                    "missing", box missingFields
                                    "paragraph", box title
                                    "reason", box (sprintf "missing required fields: %s" (String.concat ", " missingFields))
                                ]
                            suggestedVerify |> Option.iter (fun value -> blockedResult.["suggestedVerify"] <- box value)
                            results.Add(blockedResult)
                        elif dryRun then
                            let dryRunResult =
                                mdict [
                                    "status", box "filed"
                                    "score", box (Math.Round(nearestScore, 3))
                                    "inboxPath", box (relativeRepoPath cfg.RepoRoot plannedPath)
                                    "suggestedTarget", box (suggestedPath |> Option.defaultValue "")
                                    "warnings", box warnings
                                    "paragraph", box title
                                ]
                            suggestedVerify |> Option.iter (fun value -> dryRunResult.["suggestedVerify"] <- box value)
                            results.Add(dryRunResult)
                        else
                            let resultIndex = results.Count
                            plannedWrites.Add(resultIndex, plannedPath, frontmatter, paragraph, suggestedVerify, warnings)
                            results.Add(mdict [
                                "status", box "filed"
                                "score", box (Math.Round(nearestScore, 3))
                                "inboxPath", box (relativeRepoPath cfg.RepoRoot plannedPath)
                                "suggestedTarget", box (suggestedPath |> Option.defaultValue "")
                                "warnings", box warnings
                                "paragraph", box title
                            ])

                if not dryRun && plannedWrites.Count > 0 then
                    for (_, path, frontmatter, body, _, _) in plannedWrites do
                        writeMarkdownFile path frontmatter body

                    match IndexingWorkflow.rebuild cfg with
                    | Ok (updatedIndex, updatedChunks) ->
                        refreshState updatedIndex updatedChunks
                        for i in 0 .. plannedWrites.Count - 1 do
                            let resultIndex, path, _, _, suggestedVerify, warnings = plannedWrites.[i]
                            let result = results.[resultIndex]
                            match primaryChunkIndexForFile updatedIndex path with
                            | Some chunkIndex -> result.["ref"] <- box (session.NextRef(chunkIndex))
                            | None -> ()
                            result.["inboxPath"] <- box (relativeRepoPath cfg.RepoRoot path)
                            result.["warnings"] <- box warnings
                            suggestedVerify |> Option.iter (fun value -> result.["suggestedVerify"] <- box value)
                    | Error error ->
                        for (resultIndex, _, _, _, _, _) in plannedWrites do
                            results.[resultIndex].["status"] <- box "blocked"
                            results.[resultIndex].["error"] <- box error

                results.ToArray()

    let triage (index: DocIndex) (session: QuerySession) (cfg: KnowledgeSightConfig) (team: string) (before: string) (limit: int) =
        match Config.resolveInboxDir cfg with
        | Error error -> [| mdict [ "error", box error ] |]
        | Ok _ ->
            let requestedTeam = normalizeTeam team
            let pendingRows =
                index.Frontmatters
                |> Map.toArray
                |> Array.choose (fun (path, frontmatter) ->
                    if not (isPendingInboxFile cfg index path frontmatter) then None
                    else
                        let source = frontmatterScalar frontmatter "source" |> Option.defaultValue ""
                        let includeTeam = requestedTeam = "" || source.Equals(requestedTeam, StringComparison.OrdinalIgnoreCase)
                        if not includeTeam then None
                        else
                            let cycle = frontmatterScalar frontmatter "cycle" |> Option.defaultValue ""
                            Some(path, frontmatter, source, cycle))

            let cycleForms =
                pendingRows
                |> Array.choose (fun (_, _, _, cycle) -> cycleFormName cycle)
                |> Set.ofArray

            if before <> "" && cycleForms.Contains "utc" && cycleForms.Contains "integer" then
                [| mdict [ "error", box "triage({before}) cannot page mixed UTC-string and integer cycle inbox items honestly in this wave; normalize the inbox to one cycle form or omit before" ] |]
            else
                pendingRows
                |> Array.choose (fun (path, frontmatter, source, cycle) ->
                    let includeBefore = before = "" || cycleOnOrBefore cycle before
                    if not includeBefore then None
                    else
                        let missing = missingRequiredFields cfg frontmatter
                        primaryChunkIndexForFile index path
                        |> Option.map (fun chunkIndex ->
                            let refId = session.NextRef(chunkIndex)
                            let relativePath = relativeRepoPath cfg.RepoRoot path
                            let suggestedTarget = frontmatterScalar frontmatter "suggested_target" |> Option.defaultValue ""
                            mdict [
                                "id", box refId
                                "file", box (Path.GetFileName path)
                                "path", box relativePath
                                "title", box frontmatter.Title
                                "cycle", box cycle
                                "source", box source
                                "suggestedTarget", box suggestedTarget
                                "age", box (humanizeAge cycle)
                                "missing", box missing
                            ]))
                |> Array.sortWith (fun left right ->
                    let cycleComparison = compareCycleValues (string left.["cycle"]) (string right.["cycle"])
                    if cycleComparison <> 0 then cycleComparison
                    else String.Compare(string left.["path"], string right.["path"], StringComparison.OrdinalIgnoreCase))
                |> Array.truncate (max 1 limit)

    let dispose (index: DocIndex) (session: QuerySession) (cfg: KnowledgeSightConfig) (refreshState: DocIndex -> DocChunk[] option -> unit)
                (inboxRef: string) (action: string) (target: string) (verify: string) (concept: string) (observable: string) (forbids: string) (reason: string) (archive: bool) =
        match Config.resolveInboxDir cfg with
        | Error error -> mdict [ "error", box error ]
        | Ok inboxDir ->
            match session.GetRef(inboxRef) with
            | None -> mdict [ "error", box (sprintf "ref %s not found" inboxRef) ]
            | Some chunkIndex ->
                let sourceChunk = index.Chunks.[chunkIndex]
                let inboxPath = sourceChunk.FilePath
                match frontmatterForFile index inboxPath with
                | None -> mdict [ "error", box (sprintf "frontmatter not found for %s" inboxPath) ]
                | Some inboxFrontmatter when not (isPendingInboxFile cfg index inboxPath inboxFrontmatter) ->
                    mdict [ "error", box (sprintf "%s is not an inbox item awaiting disposition" (relativeRepoPath cfg.RepoRoot inboxPath)) ]
                | Some inboxFrontmatter ->
                    let inboxBaselineContent, _, inboxBody = readMarkdownFile inboxPath

                    let cycle = frontmatterScalar inboxFrontmatter "cycle" |> Option.defaultValue ""
                    let source = frontmatterScalar inboxFrontmatter "source" |> Option.defaultValue ""
                    let relativeInboxPath = relativeRepoPath cfg.RepoRoot inboxPath

                    let archiveInbox (updatedFrontmatter: Frontmatter) =
                        if archive then
                            let destinationDir = Path.Combine(cfg.RepoRoot, inboxDir, "_processed", cycle)
                            let destinationPath = nextAvailablePath (Path.Combine(destinationDir, Path.GetFileName(inboxPath)))
                            writeMarkdownFile destinationPath updatedFrontmatter inboxBody
                            if File.Exists inboxPath then File.Delete(inboxPath)
                            Ok (relativeRepoPath cfg.RepoRoot destinationPath)
                        else
                            if File.Exists inboxPath then File.Delete(inboxPath)
                            Ok ""

                    match action.Trim().ToLowerInvariant() with
                    | "promote" ->
                        if String.IsNullOrWhiteSpace(target) then
                            mdict [ "error", box "dispose(promote) requires an explicit target" ]
                        else
                            match resolveRepoRelativePath cfg target with
                            | Error error -> mdict [ "error", box error ]
                            | Ok targetPath ->
                                let actualTarget =
                                    if File.Exists targetPath then
                                        match cfg.PromoteCollision.Trim().ToLowerInvariant() with
                                        | "error" -> ""
                                        | _ -> nextAvailablePath targetPath
                                    else targetPath

                                if actualTarget = "" then
                                    mdict [ "error", box (sprintf "target '%s' already exists and promoteCollision is 'error'" target) ]
                                else
                                    let targetSnapshot = captureFileSnapshot actualTarget
                                    let canonicalFrontmatter =
                                        inboxFrontmatter
                                        |> removeFrontmatterFields [| "suggested_target"; "disposition"; "disposition_target"; "disposition_reason"; "verify_snapshot"; verifySearchCacheField |]
                                        |> fun fm -> { fm with Status = "active" }
                                        |> setFrontmatterScalarIfAny "verify" verify
                                        |> setFrontmatterScalarIfAny "concept" concept
                                        |> setFrontmatterScalarIfAny "observable" observable
                                        |> setFrontmatterScalarIfAny "forbids" forbids
                                        |> fun fm -> if String.IsNullOrWhiteSpace(fm.Title) then { fm with Title = titleFromText inboxBody } else fm

                                    writeMarkdownFile actualTarget canonicalFrontmatter inboxBody

                                    let updatedInboxFrontmatter =
                                        inboxFrontmatter
                                        |> setFrontmatterField "disposition" (Scalar "promoted")
                                        |> setFrontmatterField "disposition_target" (Scalar (relativeRepoPath cfg.RepoRoot actualTarget))

                                    match archiveInbox updatedInboxFrontmatter with
                                    | Error error -> mdict [ "error", box error ]
                                    | Ok archivedPath ->
                                        match IndexingWorkflow.rebuild cfg with
                                        | Error error -> mdict [ "error", box error ]
                                        | Ok (updatedIndex, updatedChunks) ->
                                            match maybePersistVerifySnapshot cfg updatedIndex updatedChunks actualTarget with
                                            | Error error ->
                                                rollbackWriteSeamFailure cfg refreshState index None (fun () ->
                                                    restoreFileSnapshot actualTarget targetSnapshot
                                                    restoreFileSnapshot inboxPath (Some inboxBaselineContent)
                                                    if archivedPath <> "" then
                                                        restoreFileSnapshot (absoluteRepoPath cfg.RepoRoot archivedPath) None) error
                                            | Ok (finalIndex, finalChunks) ->
                                                refreshState finalIndex finalChunks
                                                let canonicalRef =
                                                    primaryChunkIndexForFile finalIndex actualTarget
                                                    |> Option.map session.NextRef
                                                    |> Option.defaultValue ""
                                                mdict [
                                                    "ref", box inboxRef
                                                    "action", box "promote"
                                                    "source", box source
                                                    "path", box relativeInboxPath
                                                    "target", box (relativeRepoPath cfg.RepoRoot actualTarget)
                                                    "canonicalRef", box canonicalRef
                                                    "archivedPath", box archivedPath
                                                ]
                    | "merge" ->
                        match resolveMergeTargetPath cfg index target with
                        | Error error -> mdict [ "error", box error ]
                        | Ok targetPath ->
                            try
                                let baselineContent, targetFrontmatter, targetBody = readMarkdownFile targetPath
                                let targetSnapshot = Some baselineContent
                                let _, marker, block = mergeBlock relativeInboxPath source cycle inboxBody
                                let updatedBody = appendMergeBlock targetBody block
                                let updatedContent = renderMarkdownContent targetFrontmatter updatedBody

                                match commitMergeContent targetPath baselineContent updatedContent marker with
                                | MergeConflict error ->
                                    mdict [ "error", box error ]
                                | commitOutcome ->
                                    let updatedInboxFrontmatter =
                                        inboxFrontmatter
                                        |> setFrontmatterField "disposition" (Scalar "merged")
                                        |> setFrontmatterField "disposition_target" (Scalar (relativeRepoPath cfg.RepoRoot targetPath))

                                    match archiveInbox updatedInboxFrontmatter with
                                    | Error error -> mdict [ "error", box error ]
                                    | Ok archivedPath ->
                                        match IndexingWorkflow.rebuild cfg with
                                        | Error error -> mdict [ "error", box error ]
                                        | Ok (updatedIndex, updatedChunks) ->
                                            match maybePersistVerifySnapshot cfg updatedIndex updatedChunks targetPath with
                                            | Error error ->
                                                rollbackWriteSeamFailure cfg refreshState index None (fun () ->
                                                    restoreFileSnapshot targetPath targetSnapshot
                                                    restoreFileSnapshot inboxPath (Some inboxBaselineContent)
                                                    if archivedPath <> "" then
                                                        restoreFileSnapshot (absoluteRepoPath cfg.RepoRoot archivedPath) None) error
                                            | Ok (finalIndex, finalChunks) ->
                                                refreshState finalIndex finalChunks
                                                let canonicalRef =
                                                    primaryChunkIndexForFile finalIndex targetPath
                                                    |> Option.map session.NextRef
                                                    |> Option.defaultValue ""
                                                mdict [
                                                    "ref", box inboxRef
                                                    "action", box "merge"
                                                    "source", box source
                                                    "path", box relativeInboxPath
                                                    "target", box (relativeRepoPath cfg.RepoRoot targetPath)
                                                    "canonicalRef", box canonicalRef
                                                    "archivedPath", box archivedPath
                                                    "deduped", box (match commitOutcome with | MergeAlreadyPresent -> true | _ -> false)
                                                ]
                            with
                            | :? IOException ->
                                mdict [ "error", box "dispose(merge) target changed concurrently before commit; retry against the latest canonical doc" ]
                    | "reject" ->
                        if String.IsNullOrWhiteSpace(reason) then
                            mdict [ "error", box "dispose(reject) requires a reason" ]
                        else
                            let updatedInboxFrontmatter =
                                inboxFrontmatter
                                |> setFrontmatterField "disposition" (Scalar "rejected")
                                |> setFrontmatterField "disposition_reason" (Scalar reason)

                            match archiveInbox updatedInboxFrontmatter with
                            | Error error -> mdict [ "error", box error ]
                            | Ok archivedPath ->
                                match IndexingWorkflow.rebuild cfg with
                                | Error error -> mdict [ "error", box error ]
                                | Ok (updatedIndex, updatedChunks) ->
                                    refreshState updatedIndex updatedChunks
                                    mdict [
                                        "ref", box inboxRef
                                        "action", box "reject"
                                        "source", box source
                                        "path", box relativeInboxPath
                                        "archivedPath", box archivedPath
                                    ]
                    | _ ->
                        mdict [ "error", box "dispose action must be 'promote', 'merge', or 'reject'" ]

    let supersede (index: DocIndex) (session: QuerySession) (cfg: KnowledgeSightConfig) (refreshState: DocIndex -> DocChunk[] option -> unit)
                  (oldRef: string) (newContent: string) (reason: string) (by: string) (verify: string) =
        if String.IsNullOrWhiteSpace(reason) then
            mdict [ "error", box "supersede() requires a reason" ]
        elif String.IsNullOrWhiteSpace(newContent) then
            mdict [ "error", box "supersede() requires replacement content" ]
        else
            match session.GetRef(oldRef) with
            | None -> mdict [ "error", box (sprintf "ref %s not found" oldRef) ]
            | Some chunkIndex ->
                let sourceChunk = index.Chunks.[chunkIndex]
                let oldPath = sourceChunk.FilePath
                let oldRelativePath = relativeRepoPath cfg.RepoRoot oldPath
                let effectiveStatus = effectiveDocStatus cfg index oldPath

                if isInboxPath cfg oldPath || effectiveStatus <> "active" then
                    mdict [ "error", box (sprintf "%s is not an active canonical doc (status: %s)" oldRelativePath effectiveStatus) ]
                else
                    let oldBaselineContent, _, oldBody = readMarkdownFile oldPath

                    let oldFrontmatter =
                        canonicalFrontmatterForFile oldPath sourceChunk (frontmatterForFile index oldPath) oldBody

                    let newPath = nextVersionSiblingPath oldPath
                    let newPathSnapshot = captureFileSnapshot newPath
                    let newRelativePath = relativeRepoPath cfg.RepoRoot newPath
                    let newFrontmatter =
                        oldFrontmatter
                        |> removeFrontmatterFields [| "id"; "supersedes"; "superseded_by"; "reason"; "verify_snapshot"; verifySearchCacheField |]
                        |> fun fm -> { fm with Status = "active"; Title = replacementTitle oldFrontmatter.Title newContent }
                        |> setFrontmatterField "supersedes" (Scalar oldRelativePath)
                        |> setFrontmatterScalarIfAny "source" by
                        |> setFrontmatterScalarIfAny "verify" verify

                    let supersededFrontmatter =
                        oldFrontmatter
                        |> fun fm -> { fm with Status = "superseded" }
                        |> setFrontmatterField "superseded_by" (Scalar newRelativePath)
                        |> setFrontmatterField "reason" (Scalar reason)

                    writeMarkdownFile newPath newFrontmatter newContent
                    writeMarkdownFile oldPath supersededFrontmatter oldBody

                    match IndexingWorkflow.rebuild cfg with
                    | Error error -> mdict [ "error", box error ]
                    | Ok (updatedIndex, updatedChunks) ->
                        match maybePersistVerifySnapshot cfg updatedIndex updatedChunks newPath with
                        | Error error ->
                            rollbackWriteSeamFailure cfg refreshState index None (fun () ->
                                restoreFileSnapshot newPath newPathSnapshot
                                restoreFileSnapshot oldPath (Some oldBaselineContent)) error
                        | Ok (finalIndex, finalChunks) ->
                            refreshState finalIndex finalChunks
                            let updatedOldRef =
                                primaryChunkIndexForFile finalIndex oldPath
                                |> Option.map session.NextRef
                                |> Option.defaultValue ""
                            let updatedNewRef =
                                primaryChunkIndexForFile finalIndex newPath
                                |> Option.map session.NextRef
                                |> Option.defaultValue ""

                            mdict [
                                "action", box "supersede"
                                "oldRef", box updatedOldRef
                                "newRef", box updatedNewRef
                                "oldPath", box oldRelativePath
                                "newPath", box newRelativePath
                                "reason", box reason
                            ]

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

    type private ConflictScopeDoc = {
        FilePath: string
        RelativePath: string
        ScopePath: string
        Title: string
        Status: string
        Source: string
        IsSupported: bool
        JudgeExcerpt: string
        AnchorChunkIndex: int option
    }

    type private ConflictDoc = {
        RefId: string
        FilePath: string
        RelativePath: string
        ScopePath: string
        Title: string
        Status: string
        Source: string
        Embedding: float32[]
        IsSupported: bool
        JudgeExcerpt: string
    }

    type private ConflictPair = {
        Left: ConflictDoc
        Right: ConflictDoc
        Similarity: float
    }

    type private ConflictCandidateData = {
        FirstPath: string
        Members: ConflictDoc[]
        Similarity: float
    }

    let private conflictJudgeRequestTimeout = TimeSpan.FromSeconds(5.0)
    let private conflictJudgeExplanationMaxChars = 200
    let private conflictJudgeExcerptMaxChars = 1200
    let private allowedConflictVerdicts = set [ "conflict"; "duplicate"; "compatible" ]
    let private allowedConflictProfiles = set [ "duplicateOnly"; "compatibleOnly"; "conflictOnly"; "noConflictMixed"; "mixedWithConflict" ]

    let private conflictJudgeClient =
        let httpClient = new HttpClient()
        httpClient.Timeout <- Timeout.InfiniteTimeSpan
        httpClient

    let private conflictTrimForMessage (text: string) =
        if String.IsNullOrWhiteSpace(text) then ""
        else
            let compact = text.Replace("\r", " ").Replace("\n", " ").Trim()
            if compact.Length > 200 then compact.Substring(0, 200) + "…" else compact

    let private conflictFormatTimeout (timeout: TimeSpan) =
        if timeout.TotalMilliseconds < 1000.0 then sprintf "%.0fms" timeout.TotalMilliseconds
        elif abs (timeout.TotalSeconds - Math.Round(timeout.TotalSeconds)) < 0.0001 then sprintf "%.0fs" timeout.TotalSeconds
        else sprintf "%.1fs" timeout.TotalSeconds

    let private conflictTryFindSocketException (ex: exn) =
        let rec loop (current: exn) =
            match current with
            | null -> None
            | :? SocketException as socket -> Some socket
            | _ -> loop current.InnerException
        loop ex

    let private conflictClassifyHttpRequestException (url: string) (ex: HttpRequestException) =
        match conflictTryFindSocketException ex with
        | Some socket when socket.SocketErrorCode = SocketError.ConnectionRefused ->
            sprintf "conflicts() judge request connect-refused for %s" url
        | Some socket when socket.SocketErrorCode = SocketError.HostNotFound
                           || socket.SocketErrorCode = SocketError.NoData
                           || socket.SocketErrorCode = SocketError.TryAgain ->
            sprintf "conflicts() judge request DNS/NXDOMAIN failure for %s" url
        | _ ->
            match ex.HttpRequestError with
            | HttpRequestError.NameResolutionError ->
                sprintf "conflicts() judge request DNS/NXDOMAIN failure for %s" url
            | _ ->
                sprintf "conflicts() judge request failed for %s: %s" url (conflictTrimForMessage ex.Message)

    let private conflictJudgeContentExcerpt (cfg: KnowledgeSightConfig) (path: string) =
        let relativePath = stableRepoPath cfg path
        let fullPath =
            if Path.IsPathRooted(path) then path
            else Path.Combine(cfg.RepoRoot, path)

        let fallback = sprintf "(content unavailable for %s)" relativePath

        try
            if not (File.Exists(fullPath)) then fallback
            else
                let normalized = File.ReadAllText(fullPath).Replace("\r\n", "\n").Replace("\r", "\n")
                let lines = normalized.Split('\n')
                let body =
                    if lines.Length >= 2 && lines.[0].Trim() = "---" then
                        match lines |> Array.skip 1 |> Array.tryFindIndex (fun line -> line.Trim() = "---") with
                        | Some closingOffset ->
                            let bodyStart = closingOffset + 2
                            if bodyStart < lines.Length then String.Join("\n", lines.[bodyStart..]) else ""
                        | None -> normalized
                    else normalized

                let trimmed = body.Trim()
                if String.IsNullOrWhiteSpace(trimmed) then fallback
                elif trimmed.Length > conflictJudgeExcerptMaxChars then trimmed.Substring(0, conflictJudgeExcerptMaxChars) + "\n…"
                else trimmed
        with _ ->
            fallback

    let private conflictsSemanticUnavailableError =
        "conflicts() requires usable persisted semantic anchors in this wave; the indexed candidate surface has none"

    let private conflictsScopeSemanticUnavailableError (selector: string) =
        sprintf
            "conflicts() scope selector '%s' matched indexed/supported docs but none have usable persisted semantic anchors in this wave"
            selector

    let private conflictScopeDocs (cfg: KnowledgeSightConfig) (index: DocIndex) =
        index.Frontmatters
        |> Map.toArray
        |> Array.map (fun (path, frontmatter) ->
            let status = effectiveDocStatus cfg index path
            let isSupported =
                (status = "pending" && isInboxPath cfg path)
                || (status = "active" && not (isInboxPath cfg path))

            let relativePath = stableRepoPath cfg path
            let scopePath = normalizeScopeSelector relativePath
            let title =
                if String.IsNullOrWhiteSpace(frontmatter.Title) then Path.GetFileNameWithoutExtension(relativePath)
                else frontmatter.Title

            {
                FilePath = path
                RelativePath = relativePath
                ScopePath = scopePath
                Title = title
                Status = status
                Source = frontmatterScalar frontmatter "source" |> Option.defaultValue ""
                IsSupported = isSupported
                JudgeExcerpt = conflictJudgeContentExcerpt cfg path
                AnchorChunkIndex = conflictAnchorChunkIndexForFile index path
            })
        |> Array.sortBy (fun doc -> doc.RelativePath)

    let private conflictCandidateDocFromScopeDoc (index: DocIndex) (session: QuerySession) (doc: ConflictScopeDoc) =
        doc.AnchorChunkIndex
        |> Option.map (fun chunkIndex ->
            {
                RefId = session.NextRef(chunkIndex)
                FilePath = doc.FilePath
                RelativePath = doc.RelativePath
                ScopePath = doc.ScopePath
                Title = doc.Title
                Status = doc.Status
                Source = doc.Source
                Embedding = index.Embeddings.[chunkIndex]
                IsSupported = doc.IsSupported
                JudgeExcerpt = doc.JudgeExcerpt
            })

    let private conflictCandidateDocs (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) =
        conflictScopeDocs cfg index
        |> Array.choose (conflictCandidateDocFromScopeDoc index session)
        |> Array.sortBy (fun doc -> doc.RelativePath)

    let private conflictClusterSimilarity (members: ConflictDoc[]) =
        if members.Length < 2 then 0.0
        else
            [|
                for i in 0 .. members.Length - 2 do
                    for j in i + 1 .. members.Length - 1 do
                        cosine members.[i].Embedding members.[j].Embedding |> float
            |]
            |> Array.min

    let private conflictDocItem (doc: ConflictDoc) =
        mdict [
            "id", box doc.RefId
            "path", box doc.RelativePath
            "title", box doc.Title
            "status", box doc.Status
            "source", box doc.Source
        ]

    let private conflictPairs (members: ConflictDoc[]) =
        [|
            for i in 0 .. members.Length - 2 do
                for j in i + 1 .. members.Length - 1 do
                    let left = members.[i]
                    let right = members.[j]
                    yield {
                        Left = left
                        Right = right
                        Similarity = Math.Round(cosine left.Embedding right.Embedding |> float, 3)
                    }
        |]

    let private conflictPairItem (pair: ConflictPair) =
        mdict [
            "refs", box [| pair.Left.RefId; pair.Right.RefId |]
            "similarity", box pair.Similarity
            "items", box [| conflictDocItem pair.Left; conflictDocItem pair.Right |]
        ]

    let private normalizeConflictVerdictFilters (verdicts: string[]) =
        let normalized =
            verdicts
            |> Array.map (fun verdict -> if isNull verdict then "" else verdict.Trim().ToLowerInvariant())

        let invalid =
            normalized
            |> Array.filter (fun verdict -> String.IsNullOrWhiteSpace(verdict) || not (Set.contains verdict allowedConflictVerdicts))
            |> Array.distinct

        if invalid.Length > 0 then
            let renderedInvalid =
                invalid
                |> Array.map (fun verdict -> if verdict = "" then "<blank>" else verdict)
                |> String.concat ", "
            Error (sprintf "conflicts() verdicts must contain only conflict|duplicate|compatible; invalid: %s" renderedInvalid)
        else
            Ok (normalized |> Array.distinct)

    let private isExactRawConflictVerdictArray (expected: string[]) (rawVerdicts: obj option) =
        let tryEntries (value: obj) =
            match value with
            | null -> None
            | :? string -> None
            | :? (obj array) as entries -> Some entries
            | :? System.Collections.IEnumerable as items ->
                items
                |> Seq.cast<obj>
                |> Seq.toArray
                |> Some
            | _ -> None

        match rawVerdicts with
        | Some value ->
            match tryEntries value with
            | Some entries when entries.Length = expected.Length ->
                Array.forall2
                    (fun (entry: obj) (expectedValue: string) ->
                        match entry with
                        | :? string as rawEntry -> rawEntry = expectedValue
                        | _ -> false)
                    entries
                    expected
            | _ -> false
        | None ->
            false

    let private parseConflictProfileFilter (profile: string option) =
        match profile with
        | None ->
            Ok None
        | Some requested when Set.contains requested allowedConflictProfiles ->
            Ok (Some requested)
        | Some requested ->
            let renderedInvalid =
                if requested = "" then "<blank>"
                else sprintf "'%s'" requested
            Error (sprintf "conflicts() profile must be exactly one of duplicateOnly|compatibleOnly|conflictOnly|noConflictMixed|mixedWithConflict; invalid: %s" renderedInvalid)

    let private parseConflictProfileFilters (profiles: obj option) =
        let invalidProfilesMessage (invalidEntries: string[]) (duplicateEntries: string[]) =
            let invalidPart =
                if invalidEntries.Length = 0 then None
                else Some (sprintf "invalid: %s" (String.concat ", " invalidEntries))

            let duplicatePart =
                if duplicateEntries.Length = 0 then None
                else Some (sprintf "duplicates: %s" (String.concat ", " duplicateEntries))

            [ invalidPart; duplicatePart ]
            |> List.choose id
            |> String.concat "; "

        let validateEntries (entries: obj[]) =
            if entries.Length = 0 then
                Error "conflicts() profiles must be a non-empty array of distinct exact labels duplicateOnly|compatibleOnly|conflictOnly|noConflictMixed|mixedWithConflict; invalid: <empty array>"
            else
                let normalized = ResizeArray<string>()
                let invalidEntries = ResizeArray<string>()

                for entry in entries do
                    match entry with
                    | null ->
                        invalidEntries.Add("<null>")
                    | :? string as label when label = "" ->
                        invalidEntries.Add("<blank>")
                    | :? string as label when Set.contains label allowedConflictProfiles ->
                        normalized.Add(label)
                    | :? string as label ->
                        invalidEntries.Add(sprintf "'%s'" label)
                    | _ ->
                        invalidEntries.Add(sprintf "'%O'" entry)

                let duplicateEntries =
                    normalized
                    |> Seq.countBy id
                    |> Seq.filter (fun (_, count) -> count > 1)
                    |> Seq.map (fun (label, _) -> sprintf "'%s'" label)
                    |> Seq.toArray

                if invalidEntries.Count > 0 || duplicateEntries.Length > 0 then
                    Error (
                        sprintf
                            "conflicts() profiles must be a non-empty array of distinct exact labels duplicateOnly|compatibleOnly|conflictOnly|noConflictMixed|mixedWithConflict; %s"
                            (invalidProfilesMessage (invalidEntries |> Seq.toArray) duplicateEntries))
                else
                    Ok (normalized.ToArray())

        match profiles with
        | None ->
            Ok None
        | Some null ->
            Error "conflicts() profiles must be a non-empty array of distinct exact labels duplicateOnly|compatibleOnly|conflictOnly|noConflictMixed|mixedWithConflict; invalid: <null>"
        | Some (:? (obj array) as entries) ->
            validateEntries entries |> Result.map Some
        | Some (:? string as label) ->
            Error (sprintf "conflicts() profiles must be a non-empty array of distinct exact labels duplicateOnly|compatibleOnly|conflictOnly|noConflictMixed|mixedWithConflict; invalid: '%s'" label)
        | Some (:? System.Collections.IEnumerable as items) ->
            items
            |> Seq.cast<obj>
            |> Seq.toArray
            |> validateEntries
            |> Result.map Some
        | Some value ->
            Error (sprintf "conflicts() profiles must be a non-empty array of distinct exact labels duplicateOnly|compatibleOnly|conflictOnly|noConflictMixed|mixedWithConflict; invalid: '%O'" value)

    let private conflictPairVerdict (pairItem: Dictionary<string, obj>) =
        match pairItem.TryGetValue("verdict") with
        | true, (:? string as verdict) ->
            let normalized = verdict.Trim().ToLowerInvariant()
            if String.IsNullOrWhiteSpace(normalized) then None else Some normalized
        | _ -> None

    let private filterConflictPairItems (verdictFilters: string[]) (pairItems: Dictionary<string, obj>[]) =
        if verdictFilters.Length = 0 then pairItems
        else
            let allowed = Set.ofArray verdictFilters
            pairItems
            |> Array.filter (fun pairItem ->
                match conflictPairVerdict pairItem with
                | Some verdict -> Set.contains verdict allowed
                | None -> false)

    let private normalizeConflictJudgePayload (payload: string) =
        let trimmed = if isNull payload then "" else payload.Trim()
        if trimmed.StartsWith("```", StringComparison.Ordinal) then
            let normalized = trimmed.Replace("\r\n", "\n")
            let lines = normalized.Split('\n')
            if lines.Length >= 2 && lines.[lines.Length - 1].Trim() = "```" then
                String.Join("\n", lines.[1 .. lines.Length - 2]).Trim()
            else
                trimmed
        else
            trimmed

    let private malformedConflictJudgeResponse detail =
        Error (sprintf "conflicts() judge response was malformed: %s" detail)

    let private parseConflictJudgeOutcome (content: string) =
        try
            let payload = normalizeConflictJudgePayload content
            use doc = JsonDocument.Parse(payload)
            let root = doc.RootElement

            let verdict =
                match root.TryGetProperty("verdict") with
                | true, value when value.ValueKind = JsonValueKind.String ->
                    value.GetString().Trim().ToLowerInvariant()
                | true, _ ->
                    invalidOp "property 'verdict' was not a string"
                | _ ->
                    invalidOp "missing property 'verdict'"

            if verdict <> "conflict" && verdict <> "duplicate" && verdict <> "compatible" then
                invalidOp "property 'verdict' must be one of conflict|duplicate|compatible"

            let explanation =
                match root.TryGetProperty("explanation") with
                | true, value when value.ValueKind = JsonValueKind.String ->
                    value.GetString().Trim()
                | true, _ ->
                    invalidOp "property 'explanation' was not a string"
                | _ ->
                    invalidOp "missing property 'explanation'"

            if String.IsNullOrWhiteSpace(explanation) then
                invalidOp "property 'explanation' was empty"
            elif explanation.Length > conflictJudgeExplanationMaxChars then
                invalidOp (sprintf "property 'explanation' exceeded %d characters" conflictJudgeExplanationMaxChars)

            Ok (verdict, explanation)
        with
        | :? JsonException as ex ->
            malformedConflictJudgeResponse (conflictTrimForMessage ex.Message)
        | :? InvalidOperationException as ex ->
            malformedConflictJudgeResponse (conflictTrimForMessage ex.Message)
        | :? KeyNotFoundException as ex ->
            malformedConflictJudgeResponse (conflictTrimForMessage ex.Message)

    let private parseConflictJudgeResponse (json: string) =
        try
            use doc = JsonDocument.Parse(json)
            let choices =
                match doc.RootElement.TryGetProperty("choices") with
                | true, value when value.ValueKind = JsonValueKind.Array -> value
                | true, _ -> invalidOp "property 'choices' was not an array"
                | _ -> invalidOp "missing property 'choices'"

            let firstChoice =
                choices.EnumerateArray()
                |> Seq.tryHead
                |> Option.defaultWith (fun () -> invalidOp "property 'choices' was empty")

            let message =
                match firstChoice.TryGetProperty("message") with
                | true, value when value.ValueKind = JsonValueKind.Object -> value
                | true, _ -> invalidOp "property 'message' was not an object"
                | _ -> invalidOp "missing property 'message'"

            let content =
                match message.TryGetProperty("content") with
                | true, value when value.ValueKind = JsonValueKind.String ->
                    value.GetString()
                | true, value when value.ValueKind = JsonValueKind.Array ->
                    value.EnumerateArray()
                    |> Seq.choose (fun part ->
                        match part.TryGetProperty("text") with
                        | true, text when text.ValueKind = JsonValueKind.String -> Some(text.GetString())
                        | _ -> None)
                    |> String.concat ""
                | true, _ ->
                    invalidOp "property 'content' was neither a string nor a text-part array"
                | _ ->
                    invalidOp "missing property 'content'"

            parseConflictJudgeOutcome content
        with
        | :? JsonException as ex ->
            malformedConflictJudgeResponse (conflictTrimForMessage ex.Message)
        | :? InvalidOperationException as ex ->
            malformedConflictJudgeResponse (conflictTrimForMessage ex.Message)
        | :? KeyNotFoundException as ex ->
            malformedConflictJudgeResponse (conflictTrimForMessage ex.Message)

    let private conflictJudgePrompt (pair: ConflictPair) =
        String.concat "\n" [|
            "Judge whether these two knowledge claims conflict, duplicate, or are compatible."
            sprintf "Return JSON only: {\"verdict\":\"conflict|duplicate|compatible\",\"explanation\":\"<=%d chars\"}." conflictJudgeExplanationMaxChars
            ""
            "Claim A:"
            sprintf "path: %s" pair.Left.RelativePath
            sprintf "title: %s" pair.Left.Title
            pair.Left.JudgeExcerpt
            ""
            "Claim B:"
            sprintf "path: %s" pair.Right.RelativePath
            sprintf "title: %s" pair.Right.Title
            pair.Right.JudgeExcerpt
        |]

    let private judgeConflictPair (cfg: KnowledgeSightConfig) (pair: ConflictPair) = task {
        let body =
            JsonSerializer.Serialize(
                {| model = cfg.ConflictJudgeModel
                   temperature = 0
                   messages =
                    [|
                        {| role = "system"
                           content = sprintf "You judge whether two knowledge claims conflict, duplicate, or are compatible. Return JSON only with keys verdict and explanation. Keep explanation at or under %d characters." conflictJudgeExplanationMaxChars |}
                        {| role = "user"; content = conflictJudgePrompt pair |}
                    |] |})

        use content = new StringContent(body, Encoding.UTF8, "application/json")
        use cts = new CancellationTokenSource(conflictJudgeRequestTimeout)
        try
            let! response = conflictJudgeClient.PostAsync(cfg.CompletionUrl, content, cts.Token)
            if not response.IsSuccessStatusCode then
                let! errorBody = response.Content.ReadAsStringAsync(cts.Token)
                let detail = conflictTrimForMessage errorBody
                let suffix = if detail = "" then "" else sprintf ": %s" detail
                return Error (sprintf "conflicts() judge request failed with HTTP %d %s%s" (int response.StatusCode) response.ReasonPhrase suffix)
            else
                let! json = response.Content.ReadAsStringAsync(cts.Token)
                return parseConflictJudgeResponse json
        with
        | :? TaskCanceledException when cts.IsCancellationRequested ->
            return Error (sprintf "conflicts() judge request timed out after %s for %s" (conflictFormatTimeout conflictJudgeRequestTimeout) cfg.CompletionUrl)
        | :? HttpRequestException as ex ->
            return Error (conflictClassifyHttpRequestException cfg.CompletionUrl ex)
        | ex ->
            return Error (sprintf "conflicts() judge request threw %s: %s" (ex.GetType().Name) (conflictTrimForMessage ex.Message))
    }

    let private judgedConflictPairItems (cfg: KnowledgeSightConfig) (pairs: ConflictPair[]) =
        if String.IsNullOrWhiteSpace(cfg.CompletionUrl) || String.IsNullOrWhiteSpace(cfg.ConflictJudgeModel) then
            Error "conflicts({pairs:true, judge:true}) requires knowledge-sight.json completionUrl and conflictJudgeModel in this wave"
        else
            let judged = ResizeArray<Dictionary<string, obj>>()
            let mutable failure = None

            for pair in pairs do
                if failure.IsNone then
                    match judgeConflictPair cfg pair |> Async.AwaitTask |> Async.RunSynchronously with
                    | Ok (verdict, explanation) ->
                        let item = conflictPairItem pair
                        item.Add("verdict", box verdict)
                        item.Add("explanation", box explanation)
                        judged.Add(item)
                    | Error message ->
                        failure <- Some (sprintf "conflicts() judge failed for %s vs %s: %s" pair.Left.RelativePath pair.Right.RelativePath message)

            match failure with
            | Some message -> Error message
            | None -> Ok (judged.ToArray())

    let private conflictRollupItem (pairItems: Dictionary<string, obj>[]) =
        let verdicts =
            pairItems
            |> Array.choose (fun pair ->
                match pair.TryGetValue("verdict") with
                | true, (:? string as verdict) -> Some (verdict.Trim().ToLowerInvariant())
                | _ -> None)

        let count verdict =
            verdicts |> Array.filter ((=) verdict) |> Array.length

        let conflictCount = count "conflict"
        let duplicateCount = count "duplicate"
        let compatibleCount = count "compatible"

        let verdictCounts =
            mdict [
                "conflict", box conflictCount
                "duplicate", box duplicateCount
                "compatible", box compatibleCount
            ]

        let nonZeroCategories =
            [| conflictCount; duplicateCount; compatibleCount |]
            |> Array.filter (fun value -> value > 0)

        let duplicateOnly = conflictCount = 0 && compatibleCount = 0 && duplicateCount > 0
        let hasConflict = conflictCount > 0
        let compatibleOnly = conflictCount = 0 && duplicateCount = 0 && compatibleCount > 0
        let conflictOnly = conflictCount > 0 && duplicateCount = 0 && compatibleCount = 0
        let noConflict = conflictCount = 0 && (duplicateCount > 0 || compatibleCount > 0)
        let profile =
            if duplicateOnly then "duplicateOnly"
            elif compatibleOnly then "compatibleOnly"
            elif conflictOnly then "conflictOnly"
            elif noConflict then "noConflictMixed"
            else "mixedWithConflict"

        mdict [
            "judgedPairs", box verdicts.Length
            "verdictCounts", box verdictCounts
            "mixedVerdicts", box (nonZeroCategories.Length > 1)
            "duplicateOnly", box duplicateOnly
            "hasConflict", box hasConflict
            "compatibleOnly", box compatibleOnly
            "conflictOnly", box conflictOnly
            "noConflict", box noConflict
            "profile", box profile
        ]

    type private VisibleConflictSurface = {
        PairItems: Dictionary<string, obj>[]
        Rollup: Dictionary<string, obj> option
    }

    let private tryBuildVisibleConflictSurface (rollup: bool) (fullRollup: Dictionary<string, obj> option) (verdictFilters: string[]) (profileFilter: string option) (pairItems: Dictionary<string, obj>[]) =
        let visiblePairItems = filterConflictPairItems verdictFilters pairItems

        if verdictFilters.Length > 0 && visiblePairItems.Length = 0 then
            None
        else
            let visibleRollup =
                if rollup then
                    match fullRollup, verdictFilters.Length, profileFilter with
                    | Some existing, 0, None -> Some existing
                    | _ -> Some (conflictRollupItem visiblePairItems)
                else
                    None

            Some {
                PairItems = visiblePairItems
                Rollup = visibleRollup
            }

    let private tryGetConflictRollupBool (rollup: Dictionary<string, obj>) (key: string) =
        match rollup.TryGetValue(key) with
        | true, (:? bool as value) -> Some value
        | _ -> None

    let private tryGetConflictRollupInt (source: Dictionary<string, obj>) (key: string) =
        match source.TryGetValue(key) with
        | true, (:? int as value) -> Some value
        | true, (:? int64 as value) -> Some (int value)
        | true, (:? float as value) -> Some (int value)
        | true, (:? float32 as value) -> Some (int value)
        | true, (:? decimal as value) -> Some (int value)
        | _ -> None

    let private tryGetConflictRollupVerdictCounts (rollup: Dictionary<string, obj>) =
        match rollup.TryGetValue("verdictCounts") with
        | true, (:? Dictionary<string, obj> as counts) ->
            Some (
                tryGetConflictRollupInt counts "conflict" |> Option.defaultValue 0,
                tryGetConflictRollupInt counts "duplicate" |> Option.defaultValue 0,
                tryGetConflictRollupInt counts "compatible" |> Option.defaultValue 0)
        | _ -> None

    let private isDuplicateOnlyConflictRollup (rollup: Dictionary<string, obj>) =
        match tryGetConflictRollupBool rollup "mixedVerdicts", tryGetConflictRollupVerdictCounts rollup with
        | Some false, Some (conflictCount, duplicateCount, compatibleCount) ->
            conflictCount = 0 && compatibleCount = 0 && duplicateCount > 0
        | _ -> false

    let private isConflictBearingConflictRollup (rollup: Dictionary<string, obj>) =
        match tryGetConflictRollupVerdictCounts rollup with
        | Some (conflictCount, _, _) -> conflictCount > 0
        | None -> false

    let private isMixedVerdictsConflictRollup (rollup: Dictionary<string, obj>) =
        tryGetConflictRollupBool rollup "mixedVerdicts" |> Option.defaultValue false

    let private isCompatibleOnlyConflictRollup (rollup: Dictionary<string, obj>) =
        match tryGetConflictRollupBool rollup "mixedVerdicts", tryGetConflictRollupVerdictCounts rollup with
        | Some false, Some (conflictCount, duplicateCount, compatibleCount) ->
            conflictCount = 0 && duplicateCount = 0 && compatibleCount > 0
        | _ -> false

    let private isConflictOnlyConflictRollup (rollup: Dictionary<string, obj>) =
        match tryGetConflictRollupBool rollup "mixedVerdicts", tryGetConflictRollupVerdictCounts rollup with
        | Some false, Some (conflictCount, duplicateCount, compatibleCount) ->
            conflictCount > 0 && duplicateCount = 0 && compatibleCount = 0
        | _ -> false

    let private isNoConflictConflictRollup (rollup: Dictionary<string, obj>) =
        match tryGetConflictRollupVerdictCounts rollup with
        | Some (conflictCount, duplicateCount, compatibleCount) ->
            conflictCount = 0 && (duplicateCount > 0 || compatibleCount > 0)
        | None -> false

    let private isExactConflictProfile (rollup: Dictionary<string, obj>) (profile: string) =
        match rollup.TryGetValue("profile") with
        | true, (:? string as currentProfile) -> currentProfile = profile
        | _ -> false

    let private conflictCandidateItem (cfg: KnowledgeSightConfig) (pairs: bool) (judge: bool) (rollup: bool) (verdictFilters: string[]) (profileFilter: string option) (profileFilters: string[] option) (duplicatesOnly: bool) (hasConflict: bool) (mixedVerdicts: bool) (compatibleOnly: bool) (conflictOnly: bool) (noConflict: bool) (candidateData: ConflictCandidateData) =
        let candidate =
            mdict [
                "refs", box (candidateData.Members |> Array.map (fun doc -> doc.RefId))
                "similarity", box (Math.Round(candidateData.Similarity, 3))
                "docs", box candidateData.Members.Length
                "items", box (candidateData.Members |> Array.map conflictDocItem)
            ]

        if pairs then
            let pairData = conflictPairs candidateData.Members
            let pairItems =
                if judge then judgedConflictPairItems cfg pairData
                else Ok (pairData |> Array.map conflictPairItem)

            match pairItems with
            | Ok items ->
                let fullRollup =
                    if (duplicatesOnly || hasConflict || mixedVerdicts || compatibleOnly || conflictOnly || noConflict) && judge && rollup then Some (conflictRollupItem items) else None

                let filteredByCandidateGate =
                    (duplicatesOnly && (fullRollup |> Option.exists isDuplicateOnlyConflictRollup |> not))
                    || (hasConflict && (fullRollup |> Option.exists isConflictBearingConflictRollup |> not))
                    || (mixedVerdicts && (fullRollup |> Option.exists isMixedVerdictsConflictRollup |> not))
                    || (compatibleOnly && (fullRollup |> Option.exists isCompatibleOnlyConflictRollup |> not))
                    || (conflictOnly && (fullRollup |> Option.exists isConflictOnlyConflictRollup |> not))
                    || (noConflict && (fullRollup |> Option.exists isNoConflictConflictRollup |> not))

                if filteredByCandidateGate then
                    Ok None
                else
                    let visibleSurface =
                        if judge then
                            tryBuildVisibleConflictSurface rollup fullRollup verdictFilters profileFilter items
                        else
                            Some {
                                PairItems = items
                                Rollup = None
                            }

                    match visibleSurface with
                    | None ->
                        Ok None
                    | Some visibleSurface ->
                        let filteredByProfile =
                            match profileFilter, visibleSurface.Rollup with
                            | Some requested, Some rollupItem -> not (isExactConflictProfile rollupItem requested)
                            | _ -> false

                        let filteredByProfiles =
                            match profileFilters, visibleSurface.Rollup with
                            | Some requested, Some rollupItem ->
                                let allowed = Set.ofArray requested
                                match rollupItem.TryGetValue("profile") with
                                | true, (:? string as currentProfile) -> not (Set.contains currentProfile allowed)
                                | _ -> true
                            | _ -> false

                        if filteredByProfile || filteredByProfiles then
                            Ok None
                        else
                            candidate.Add("pairs", box visibleSurface.PairItems)
                            visibleSurface.Rollup |> Option.iter (fun rollupItem -> candidate.Add("rollup", box rollupItem))
                            Ok (Some candidate)
            | Error message ->
                Error message
        else
            Ok (Some candidate)

    let conflicts (cfg: KnowledgeSightConfig) (index: DocIndex) (session: QuerySession) (threshold: float) (scope: string[]) (judge: bool) (pairs: bool) (verdicts: string[]) (rawVerdicts: obj option) (rollup: bool) (profile: string option) (profiles: obj option) (duplicatesOnly: bool) (hasConflict: bool) (mixedVerdicts: bool) (compatibleOnly: bool) (conflictOnly: bool) (noConflict: bool) =
        match parseConflictProfileFilter profile with
        | Error message ->
            [| mdict [ "error", box message ] |]
        | Ok profileFilter ->
            match parseConflictProfileFilters profiles with
            | Error message ->
                [| mdict [ "error", box message ] |]
            | Ok profileFilters when profileFilter.IsSome && profileFilters.IsSome ->
                [| mdict [ "error", box "conflicts() in this wave hard-rejects every profile:'...' plus profiles:[...] combination so visible-profile input precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && duplicatesOnly ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave hard-rejects every duplicatesOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && hasConflict ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave hard-rejects every hasConflict:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && mixedVerdicts ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave hard-rejects every mixedVerdicts:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && compatibleOnly ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave hard-rejects every compatibleOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && conflictOnly ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave hard-rejects every conflictOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && noConflict ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave hard-rejects every noConflict:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when profileFilters.IsSome && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({profiles:[...]}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when profileFilter.IsSome && duplicatesOnly && profileFilter <> Some "duplicateOnly" ->
                [| mdict [ "error", box "conflicts({duplicatesOnly:true, profile:'...'}) in this wave allows profile:'...' only for the exact profile:'duplicateOnly' path; compatible-only, conflict-only, no-conflict, and mixed conflict-bearing profile labels stay rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && duplicatesOnly && rawVerdicts.IsSome ->
                [| mdict [ "error", box "conflicts({duplicatesOnly:true, profile:'duplicateOnly'}) in this wave allows the exact profile path only when the verdicts key is absent so verdict/profile composition stays rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && hasConflict && profileFilter <> Some "mixedWithConflict" ->
                [| mdict [ "error", box "conflicts({hasConflict:true, profile:'...'}) in this wave allows profile:'...' only for the exact profile:'mixedWithConflict' path; conflict-only and no-conflict profile labels stay rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && hasConflict && rawVerdicts.IsSome ->
                [| mdict [ "error", box "conflicts({hasConflict:true, profile:'mixedWithConflict'}) in this wave allows the exact profile path only when the verdicts key is absent so verdict/profile composition stays rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && mixedVerdicts && hasConflict ->
                [| mdict [ "error", box "conflicts({mixedVerdicts:true, profile:'...'}) in this wave hard-rejects every hasConflict:true combination so gate-intersection behavior stays explicit" ] |]
            | Ok profileFilters when profileFilter.IsSome && mixedVerdicts && noConflict ->
                [| mdict [ "error", box "conflicts({mixedVerdicts:true, profile:'...'}) in this wave hard-rejects every noConflict:true combination so gate-intersection behavior stays explicit" ] |]
            | Ok profileFilters when profileFilter.IsSome && mixedVerdicts && profileFilter <> Some "mixedWithConflict" ->
                [| mdict [ "error", box "conflicts({mixedVerdicts:true, profile:'...'}) in this wave allows profile:'...' only for the exact profile:'mixedWithConflict' path; no-conflict and singleton profile labels stay rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && mixedVerdicts && rawVerdicts.IsSome ->
                [| mdict [ "error", box "conflicts({mixedVerdicts:true, profile:'mixedWithConflict'}) in this wave allows the exact profile path only when the verdicts key is absent so verdict/profile composition stays rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && compatibleOnly && profileFilter <> Some "compatibleOnly" ->
                [| mdict [ "error", box "conflicts({compatibleOnly:true, profile:'...'}) in this wave allows profile:'...' only for the exact profile:'compatibleOnly' path; duplicate-only, conflict-only, no-conflict, and mixed conflict-bearing profile labels stay rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && compatibleOnly && rawVerdicts.IsSome ->
                [| mdict [ "error", box "conflicts({compatibleOnly:true, profile:'compatibleOnly'}) in this wave allows the exact profile path only when the verdicts key is absent so verdict/profile composition stays rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && conflictOnly && profileFilter <> Some "conflictOnly" ->
                [| mdict [ "error", box "conflicts({conflictOnly:true, profile:'...'}) in this wave allows profile:'...' only for the exact profile:'conflictOnly' path; duplicate-only, compatible-only, no-conflict, and mixed conflict-bearing profile labels stay rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && conflictOnly && rawVerdicts.IsSome ->
                [| mdict [ "error", box "conflicts({conflictOnly:true, profile:'conflictOnly'}) in this wave allows the exact profile path only when the verdicts key is absent so verdict/profile composition stays rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && noConflict && profileFilter <> Some "noConflictMixed" ->
                [| mdict [ "error", box "conflicts({noConflict:true, profile:'...'}) in this wave allows profile:'...' only for the exact profile:'noConflictMixed' path; unanimous and conflict-bearing profile labels stay rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && noConflict && rawVerdicts.IsSome ->
                [| mdict [ "error", box "conflicts({noConflict:true, profile:'noConflictMixed'}) in this wave allows the exact profile path only when the verdicts key is absent so verdict/profile composition stays rejected" ] |]
            | Ok profileFilters when profileFilter.IsSome && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({profile:'...'}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when noConflict && duplicatesOnly ->
                [| mdict [ "error", box "conflicts({noConflict:true}) in this wave hard-rejects every duplicatesOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when noConflict && hasConflict ->
                [| mdict [ "error", box "conflicts({noConflict:true}) in this wave hard-rejects every hasConflict:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when noConflict && compatibleOnly ->
                [| mdict [ "error", box "conflicts({noConflict:true}) in this wave hard-rejects every compatibleOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when noConflict && conflictOnly ->
                [| mdict [ "error", box "conflicts({noConflict:true}) in this wave hard-rejects every conflictOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when noConflict && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({noConflict:true}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when conflictOnly && rawVerdicts.IsSome && not (isExactRawConflictVerdictArray [| "conflict" |] rawVerdicts) ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shape ['conflict']; empty, repeated-entry, and broader raw-present verdicts stay rejected" ] |]
            | Ok profileFilters when conflictOnly && verdicts.Length > 0 && not (verdicts.Length = 1 && verdicts.[0] = "conflict") ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave allows verdicts:[...] only for the exact ['conflict'] path so broader visible conflict-only filtering stays rejected" ] |]
            | Ok profileFilters when conflictOnly && duplicatesOnly ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave hard-rejects every duplicatesOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when conflictOnly && hasConflict ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave hard-rejects every hasConflict:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when conflictOnly && mixedVerdicts ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave hard-rejects every mixedVerdicts:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when conflictOnly && compatibleOnly ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave hard-rejects every compatibleOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when conflictOnly && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({conflictOnly:true}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when compatibleOnly && duplicatesOnly ->
                [| mdict [ "error", box "conflicts({compatibleOnly:true}) in this wave hard-rejects every duplicatesOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when compatibleOnly && hasConflict ->
                [| mdict [ "error", box "conflicts({compatibleOnly:true}) in this wave hard-rejects every hasConflict:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when compatibleOnly && mixedVerdicts ->
                [| mdict [ "error", box "conflicts({compatibleOnly:true}) in this wave hard-rejects every mixedVerdicts:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when compatibleOnly && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({compatibleOnly:true}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when mixedVerdicts && duplicatesOnly ->
                [| mdict [ "error", box "conflicts({mixedVerdicts:true}) in this wave hard-rejects every duplicatesOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when mixedVerdicts && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({mixedVerdicts:true}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when hasConflict && duplicatesOnly ->
                [| mdict [ "error", box "conflicts({hasConflict:true}) in this wave hard-rejects every duplicatesOnly:true combination so candidate-gate precedence stays explicit" ] |]
            | Ok profileFilters when hasConflict && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({hasConflict:true}) in this wave requires pairs:true, judge:true, and rollup:true so there is no hidden implicit rollup path" ] |]
            | Ok profileFilters when rollup && (not pairs || not judge) ->
                [| mdict [ "error", box "conflicts({rollup:true}) in this wave requires pairs:true and judge:true so rollup stays on the shipped judged-pair surface" ] |]
            | Ok profileFilters when duplicatesOnly && (not pairs || not judge || not rollup) ->
                [| mdict [ "error", box "conflicts({duplicatesOnly:true}) in this wave requires pairs:true, judge:true, and rollup:true so duplicate-only retention stays on the shipped judged rollup surface" ] |]
            | Ok profileFilters when verdicts.Length > 0 && (not pairs || not judge) ->
                [| mdict [ "error", box "conflicts({verdicts:[...]}) in this wave requires pairs:true and judge:true so filtering stays on the shipped judged-pair surface" ] |]
            | Ok profileFilters when judge && not pairs ->
                [| mdict [ "error", box "conflicts({judge:true}) in this wave requires pairs:true to avoid ambiguous cluster-level verdicts" ] |]
            | Ok profileFilters when Double.IsNaN(threshold) || Double.IsInfinity(threshold) || threshold <= 0.0 || threshold > 1.0 ->
                [| mdict [ "error", box "conflicts() threshold must be > 0 and <= 1" ] |]
            | Ok profileFilters ->
                let verdictsPresent = rawVerdicts.IsSome
                let exactRawConflictVerdicts = isExactRawConflictVerdictArray [| "conflict" |] rawVerdicts
                let exactRawDuplicateVerdicts = isExactRawConflictVerdictArray [| "duplicate" |] rawVerdicts
                let exactRawCompatibleVerdicts = isExactRawConflictVerdictArray [| "compatible" |] rawVerdicts
                let exactRawConflictDuplicateVerdicts = isExactRawConflictVerdictArray [| "conflict"; "duplicate" |] rawVerdicts
                let exactRawDuplicateConflictVerdicts = isExactRawConflictVerdictArray [| "duplicate"; "conflict" |] rawVerdicts
                let exactRawConflictCompatibleVerdicts = isExactRawConflictVerdictArray [| "conflict"; "compatible" |] rawVerdicts
                let exactRawCompatibleConflictVerdicts = isExactRawConflictVerdictArray [| "compatible"; "conflict" |] rawVerdicts
                let exactRawDuplicateCompatibleVerdicts = isExactRawConflictVerdictArray [| "duplicate"; "compatible" |] rawVerdicts
                let exactRawCompatibleDuplicateVerdicts = isExactRawConflictVerdictArray [| "compatible"; "duplicate" |] rawVerdicts
                let exactRawConflictDuplicateCompatibleVerdicts = isExactRawConflictVerdictArray [| "conflict"; "duplicate"; "compatible" |] rawVerdicts
                match normalizeConflictVerdictFilters verdicts with
                | Error message ->
                    [| mdict [ "error", box message ] |]
                | Ok verdictFilters when mixedVerdicts && hasConflict && verdictsPresent && not (exactRawConflictVerdicts || exactRawDuplicateVerdicts || exactRawCompatibleVerdicts || exactRawConflictDuplicateVerdicts || exactRawDuplicateConflictVerdicts || exactRawConflictCompatibleVerdicts || exactRawCompatibleConflictVerdicts || exactRawDuplicateCompatibleVerdicts || exactRawCompatibleDuplicateVerdicts || exactRawConflictDuplicateCompatibleVerdicts) ->
                    [| mdict [ "error", box "conflicts({hasConflict:true, mixedVerdicts:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shapes ['conflict'], ['duplicate'], ['compatible'], ['conflict','duplicate'], ['duplicate','conflict'], ['conflict','compatible'], ['compatible','conflict'], ['duplicate','compatible'], ['compatible','duplicate'], and ['conflict','duplicate','compatible']; the shipped bare path still requires the verdicts key absent and every other raw-present verdicts input stays rejected" ] |]
                | Ok verdictFilters when compatibleOnly && verdictsPresent && not exactRawCompatibleVerdicts ->
                    [| mdict [ "error", box "conflicts({compatibleOnly:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shape ['compatible']; empty, repeated-entry, and broader raw-present verdicts stay rejected" ] |]
                | Ok verdictFilters when compatibleOnly && verdictFilters.Length > 0 && not (verdictFilters.Length = 1 && verdictFilters.[0] = "compatible") ->
                    [| mdict [ "error", box "conflicts({compatibleOnly:true}) in this wave allows verdicts:[...] only for the exact ['compatible'] path so broader visible compatible-only filtering stays rejected" ] |]
                | Ok verdictFilters when duplicatesOnly && verdictsPresent && not exactRawDuplicateVerdicts ->
                    [| mdict [ "error", box "conflicts({duplicatesOnly:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shape ['duplicate']; empty, repeated-entry, and broader raw-present verdicts stay rejected" ] |]
                | Ok verdictFilters when duplicatesOnly && verdictFilters.Length > 0 && not (verdictFilters.Length = 1 && verdictFilters.[0] = "duplicate") ->
                    [| mdict [ "error", box "conflicts({duplicatesOnly:true}) in this wave allows verdicts:[...] only for the exact ['duplicate'] path so broader visible duplicate-only filtering stays rejected" ] |]
                | Ok verdictFilters when mixedVerdicts && verdictsPresent && not hasConflict && not noConflict && not (exactRawConflictVerdicts || exactRawDuplicateVerdicts || exactRawCompatibleVerdicts || exactRawConflictDuplicateVerdicts || exactRawDuplicateConflictVerdicts || exactRawConflictCompatibleVerdicts || exactRawCompatibleConflictVerdicts || exactRawDuplicateCompatibleVerdicts || exactRawCompatibleDuplicateVerdicts) ->
                    [| mdict [ "error", box "conflicts({mixedVerdicts:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shapes ['conflict'], ['duplicate'], ['compatible'], ['conflict','duplicate'], ['duplicate','conflict'], ['conflict','compatible'], ['compatible','conflict'], ['duplicate','compatible'], or ['compatible','duplicate']; repeated-entry and broader raw-present verdicts stay rejected" ] |]
                | Ok verdictFilters when mixedVerdicts && noConflict && verdictsPresent && not (exactRawDuplicateVerdicts || exactRawCompatibleVerdicts || exactRawDuplicateCompatibleVerdicts || exactRawCompatibleDuplicateVerdicts) ->
                    [| mdict [ "error", box "conflicts({noConflict:true, mixedVerdicts:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shapes ['duplicate'], ['compatible'], ['duplicate','compatible'], or ['compatible','duplicate']; the bare path still requires the verdicts key absent and every other raw-present verdicts input stays rejected" ] |]
                | Ok verdictFilters when mixedVerdicts && verdictFilters.Length > 0 && not (((verdictFilters.Length = 1 && (verdictFilters.[0] = "conflict" || verdictFilters.[0] = "duplicate" || verdictFilters.[0] = "compatible")) && not noConflict) || (verdictFilters.Length = 2 && ((verdictFilters.[0] = "conflict" && verdictFilters.[1] = "duplicate") || (verdictFilters.[0] = "duplicate" && verdictFilters.[1] = "conflict") || (verdictFilters.[0] = "conflict" && verdictFilters.[1] = "compatible") || (verdictFilters.[0] = "compatible" && verdictFilters.[1] = "conflict") || (verdictFilters.[0] = "duplicate" && verdictFilters.[1] = "compatible") || (verdictFilters.[0] = "compatible" && verdictFilters.[1] = "duplicate"))) || (hasConflict && verdictFilters.Length = 3 && verdictFilters.[0] = "conflict" && verdictFilters.[1] = "duplicate" && verdictFilters.[2] = "compatible") || (noConflict && ((verdictFilters.Length = 1 && (verdictFilters.[0] = "duplicate" || verdictFilters.[0] = "compatible")) || (verdictFilters.Length = 2 && ((verdictFilters.[0] = "duplicate" && verdictFilters.[1] = "compatible") || (verdictFilters.[0] = "compatible" && verdictFilters.[1] = "duplicate")))))) ->
                    [| mdict [ "error", box "conflicts({mixedVerdicts:true}) in this wave allows verdicts:[...] only for the exact ['conflict'], exact raw ['duplicate'], and exact raw ['compatible'] plain-seam paths, the exact raw ['conflict','duplicate'], ['duplicate','conflict'], ['conflict','compatible'], ['compatible','conflict'], ['duplicate','compatible'], and ['compatible','duplicate'] plain-seam paths, the exact raw ['conflict','duplicate','compatible'] path when paired with hasConflict:true, plus the exact raw ['duplicate'], ['compatible'], ['duplicate','compatible'], and ['compatible','duplicate'] paths when paired with noConflict:true, so broader visible mixed-verdict filtering stays rejected" ] |]
                | Ok verdictFilters when hasConflict && verdictsPresent && not (exactRawConflictVerdicts || exactRawDuplicateVerdicts || exactRawCompatibleVerdicts || exactRawConflictDuplicateVerdicts || exactRawDuplicateConflictVerdicts || exactRawConflictCompatibleVerdicts || exactRawCompatibleConflictVerdicts || exactRawDuplicateCompatibleVerdicts || exactRawCompatibleDuplicateVerdicts || (mixedVerdicts && exactRawConflictDuplicateCompatibleVerdicts)) ->
                    [| mdict [ "error", box "conflicts({hasConflict:true}) in this wave allows raw-present verdicts:[...] only for the exact raw verdict shapes ['conflict'], ['duplicate'], ['compatible'], ['conflict','duplicate'], ['duplicate','conflict'], ['conflict','compatible'], ['compatible','conflict'], ['duplicate','compatible'], or ['compatible','duplicate'], plus ['conflict','duplicate','compatible'] only when paired with mixedVerdicts:true; repeated-entry, empty, and broader raw-present verdicts stay rejected" ] |]
                | Ok verdictFilters when hasConflict && verdictFilters.Length > 0 && not ((verdictFilters.Length = 1 && (verdictFilters.[0] = "conflict" || verdictFilters.[0] = "duplicate" || verdictFilters.[0] = "compatible")) || (verdictFilters.Length = 2 && ((verdictFilters.[0] = "conflict" && verdictFilters.[1] = "duplicate") || (verdictFilters.[0] = "duplicate" && verdictFilters.[1] = "conflict") || (verdictFilters.[0] = "conflict" && verdictFilters.[1] = "compatible") || (verdictFilters.[0] = "compatible" && verdictFilters.[1] = "conflict") || (verdictFilters.[0] = "duplicate" && verdictFilters.[1] = "compatible") || (verdictFilters.[0] = "compatible" && verdictFilters.[1] = "duplicate"))) || (mixedVerdicts && verdictFilters.Length = 3 && verdictFilters.[0] = "conflict" && verdictFilters.[1] = "duplicate" && verdictFilters.[2] = "compatible")) ->
                    [| mdict [ "error", box "conflicts({hasConflict:true}) in this wave allows verdicts:[...] only for the exact ['conflict'], exact ['duplicate'], and exact ['compatible'] paths plus the exact raw ['conflict','duplicate'], ['duplicate','conflict'], ['conflict','compatible'], ['compatible','conflict'], ['duplicate','compatible'], and ['compatible','duplicate'] paths, with exact raw ['conflict','duplicate','compatible'] added only on the hasConflict:true + mixedVerdicts:true intersection, so repeated and broader visible conflict-bearing filtering stays rejected" ] |]
                | Ok verdictFilters when noConflict && Array.contains "conflict" verdictFilters ->
                    [| mdict [ "error", box "conflicts({noConflict:true}) in this wave allows verdicts:[...] only within duplicate|compatible so conflict-bearing visible filtering stays rejected" ] |]
                | Ok verdictFilters ->
                    match Config.scanDocDirs cfg with
                    | Error error ->
                        [| mdict [ "error", box error ] |]
                    | Ok allowedScopeRoots ->
                        let allConflictDocs = conflictScopeDocs cfg index
                        let docs = allConflictDocs |> Array.choose (conflictCandidateDocFromScopeDoc index session)
                        let docsByPath = Dictionary<string, ConflictDoc>(StringComparer.OrdinalIgnoreCase)
                        for doc in docs do
                            docsByPath.[doc.RelativePath] <- doc
                        let supportedDocs = docs |> Array.filter (fun doc -> doc.IsSupported)
                        let supportedScopeDocs = allConflictDocs |> Array.filter (fun doc -> doc.IsSupported)

                        let resolvedDocsOrErrors =
                            if scope.Length = 0 then
                                if supportedScopeDocs.Length > 0 && supportedDocs.Length = 0 then
                                    Error [| mdict [ "error", box conflictsSemanticUnavailableError ] |]
                                else
                                    Ok supportedDocs
                            else
                                let selectorErrors = ResizeArray<Dictionary<string, obj>>()
                                let selectedDocs = Dictionary<string, ConflictDoc>(StringComparer.OrdinalIgnoreCase)

                                for requested in scope do
                                    let rawSelector = if isNull requested then "" else requested
                                    let trimmed = rawSelector.Trim()
                                    let normalized = normalizeScopeSelector trimmed

                                    let error message =
                                        selectorErrors.Add(mdict [ "selector", box rawSelector; "error", box message ])

                                    if trimmed = "" then
                                        error "conflicts() scope selectors must be non-empty repo-relative paths or globs in this wave"
                                    elif Path.IsPathRooted(trimmed) then
                                        error "conflicts() scope selectors must be repo-relative paths or globs in this wave"
                                    elif Regex.IsMatch(normalized, @"^R\d+$") then
                                        error "conflicts() scope selectors must not use session-scoped R# refs in this wave"
                                    else
                                        let selectorRoot = scopeSelectorRoot normalized
                                        if not (isSelectorUnderAllowedDirs allowedScopeRoots selectorRoot) then
                                            error "conflicts() scope selectors must stay under configured knowledge doc dirs or inbox in this wave"
                                        else
                                            let matches =
                                                if scopeSelectorHasWildcard normalized then
                                                    let matcher = scopeSelectorRegex normalized
                                                    allConflictDocs
                                                    |> Array.filter (fun doc -> matcher.IsMatch(doc.ScopePath))
                                                else
                                                    let normalizedExact = normalized.TrimEnd('/')
                                                    let exactMatches =
                                                        allConflictDocs
                                                        |> Array.filter (fun doc ->
                                                            String.Equals(doc.ScopePath, normalizedExact, StringComparison.OrdinalIgnoreCase))
                                                    if exactMatches.Length > 0 then exactMatches
                                                    else
                                                        allConflictDocs
                                                        |> Array.filter (fun doc ->
                                                            doc.ScopePath.StartsWith(normalizedExact + "/", StringComparison.OrdinalIgnoreCase))

                                            if matches.Length = 0 then
                                                error (sprintf "conflicts() scope selector '%s' did not match any indexed docs in this wave" normalized)
                                            else
                                                let supportedMatches = matches |> Array.filter (fun doc -> doc.IsSupported)
                                                let unsupportedMatches = matches |> Array.filter (fun doc -> not doc.IsSupported)

                                                if supportedMatches.Length = 0 then
                                                    error (sprintf "conflicts() scope selector '%s' did not match any supported pending inbox or active canonical docs in this wave" normalized)
                                                elif unsupportedMatches.Length > 0 then
                                                    error (sprintf "conflicts() scope selector '%s' matched unsupported docs in this wave" normalized)
                                                else
                                                    let supportedAnchoredMatches =
                                                        supportedMatches
                                                        |> Array.choose (fun doc ->
                                                            match docsByPath.TryGetValue(doc.RelativePath) with
                                                            | true, anchoredDoc -> Some anchoredDoc
                                                            | _ -> None)

                                                    if supportedAnchoredMatches.Length = 0 then
                                                        error (conflictsScopeSemanticUnavailableError normalized)
                                                    else
                                                        supportedAnchoredMatches
                                                        |> Array.iter (fun doc ->
                                                            if not (selectedDocs.ContainsKey(doc.ScopePath)) then
                                                                selectedDocs.[doc.ScopePath] <- doc)

                                if selectorErrors.Count > 0 then Error (selectorErrors.ToArray())
                                else
                                    Ok (
                                        selectedDocs.Values
                                        |> Seq.sortBy (fun doc -> doc.RelativePath)
                                        |> Seq.toArray)

                        match resolvedDocsOrErrors with
                        | Error errors -> errors
                        | Ok resolvedDocs when resolvedDocs.Length < 2 ->
                            [||]
                        | Ok resolvedDocs ->
                            let candidates =
                                resolvedDocs
                                |> Array.map (fun doc -> doc.RelativePath, doc.Embedding)
                                |> fun items -> greedyCluster items threshold
                                |> Array.choose (fun memberPaths ->
                                    let members =
                                        memberPaths
                                        |> Array.choose (fun relativePath ->
                                            match docsByPath.TryGetValue(relativePath) with
                                            | true, doc -> Some doc
                                            | _ -> None)
                                        |> Array.sortBy (fun doc -> doc.RelativePath)

                                    if members.Length < 2 then None
                                    else
                                        let similarity = conflictClusterSimilarity members
                                        if similarity < threshold then None
                                        else
                                            Some {
                                                FirstPath = members.[0].RelativePath
                                                Members = members
                                                Similarity = similarity
                                            })
                                |> Array.sortBy (fun candidateData -> candidateData.FirstPath)

                            let rendered = ResizeArray<Dictionary<string, obj>>()
                            let mutable failure = None

                            for candidateData in candidates do
                                if failure.IsNone then
                                    match conflictCandidateItem cfg pairs judge rollup verdictFilters profileFilter profileFilters duplicatesOnly hasConflict mixedVerdicts compatibleOnly conflictOnly noConflict candidateData with
                                    | Ok (Some candidate) -> rendered.Add(candidate)
                                    | Ok None -> ()
                                    | Error message -> failure <- Some message

                            match failure with
                            | Some message -> [| mdict [ "error", box message ] |]
                            | None -> rendered.ToArray()

    /// Suggest subfolder groupings for docs in a directory.
    /// Uses embeddings to cluster docs by semantic similarity.
    let cluster (cfg: KnowledgeSightConfig) (index: DocIndex) (dir: string) (threshold: float) (statuses: string[]) =
        let allowedStatuses = normalizeRequestedStatuses statuses
        let normDir = dir.Replace("\\", "/").TrimEnd('/')
        // Find docs in the target directory
        let docsInDir =
            index.Chunks
            |> Array.filter (fun c -> c.Level <= 1) // top-level sections only (one per doc)
            |> Array.filter (fun c -> matchesDocStatus cfg index allowedStatuses c.FilePath)
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

    // ── hygiene — role-aware, report-first maintenance workflow ──

    type private DocProfile = {
        FilePath: string
        RelativePath: string
        Title: string
        Tags: string[]
        Backlinks: int
        Sections: DocChunk[]
        Role: string
        RoleConfidence: float
        RoleEvidence: string[]
    }

    let private clamp01 (value: float) = Math.Max(0.0, Math.Min(1.0, value))

    let private lower (text: string) = if isNull text then "" else text.ToLowerInvariant()

    let private containsAny (text: string) (terms: string[]) =
        let haystack = lower text
        terms |> Array.exists haystack.Contains

    let private firstSnippet (text: string) =
        text.Split('\n')
        |> Array.map (fun line -> line.Trim())
        |> Array.tryFind (fun line -> line <> "")
        |> Option.defaultValue ""
        |> fun snippet ->
            if snippet.Length > 120 then snippet.Substring(0, 117) + "..."
            else snippet

    let private normalizedTokens (text: string) =
        Regex.Matches(lower text, @"[a-z0-9]{3,}")
        |> Seq.cast<Match>
        |> Seq.map (fun m -> m.Value)
        |> Set.ofSeq

    let private jaccard (left: Set<string>) (right: Set<string>) =
        if Set.isEmpty left || Set.isEmpty right then 0.0
        else
            let overlap = Set.intersect left right |> Set.count
            let universe = Set.union left right |> Set.count
            if universe = 0 then 0.0 else float overlap / float universe

    let private normalizeStatusText (text: string) =
        text.ToLowerInvariant()
        |> fun value -> Regex.Replace(value, @"\b\d+\b", "#")
        |> fun value -> Regex.Replace(value, @"[^\w\s#]", " ")
        |> fun value -> Regex.Replace(value, @"\s+", " ").Trim()

    let private relativePath (repoRoot: string) (filePath: string) =
        let normalizedRepoRoot = Path.GetFullPath(repoRoot)
        if Path.IsPathRooted filePath then Path.GetRelativePath(normalizedRepoRoot, filePath).Replace("\\", "/")
        else filePath.Replace("\\", "/")

    let private classifyDocRole (filePath: string) (title: string) (tags: string[]) (sections: DocChunk[]) (backlinks: int) =
        let scores = Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        let reasons = Dictionary<string, ResizeArray<string>>(StringComparer.OrdinalIgnoreCase)

        let add role points reason =
            let current = if scores.ContainsKey(role) then scores.[role] else 0
            scores.[role] <- current + points
            if not (reasons.ContainsKey(role)) then reasons.[role] <- ResizeArray<string>()
            reasons.[role].Add(reason)

        let titleLower = lower title
        let pathLower = lower filePath
        let textLower =
            sections
            |> Array.collect (fun section -> [| section.Heading; section.Content |])
            |> String.concat "\n"
            |> lower
        let tagLower = tags |> Array.map lower
        let tagsContain (terms: string[]) =
            terms
            |> Array.exists (fun (term: string) ->
                tagLower |> Array.exists (fun (tag: string) -> tag.Contains(term)))

        if containsAny pathLower [| "/status/"; "current-status"; "progress"; "status-report" |]
           || containsAny titleLower [| "current status"; "status"; "progress" |]
           || tagsContain [| "status"; "current" |] then
            add "canonical_live_status_owner" 3 "Title/path/tags indicate a live-status owner."
        if containsAny textLower [| "canonical owner"; "canonical status"; "other docs should point here" |] then
            add "canonical_live_status_owner" 3 "Content explicitly claims canonical live-status ownership."
        if backlinks > 0 && containsAny textLower [| "current status"; "active wave"; "tests baseline"; "delivered" |] then
            add "canonical_live_status_owner" (min 2 backlinks) (sprintf "Linked from %d other document(s)." backlinks)

        if containsAny pathLower [| "/archive/"; "history"; "historical"; "retro"; "review" |]
           || containsAny titleLower [| "archive"; "history"; "historical"; "review"; "retro" |]
           || tagsContain [| "archive"; "history"; "review" |] then
            add "historical_archive" 4 "File/title/tags look archival or historical."
        if containsAny textLower [| "historical snapshot"; "written at the time"; "past status"; "archive handling" |] then
            add "historical_archive" 2 "Content describes historical or archival status."

        if containsAny pathLower [| "spec"; "format"; "schema" |]
           || containsAny titleLower [| "spec"; "format"; "schema" |]
           || tagsContain [| "spec"; "schema"; "format" |] then
            add "implemented_spec" 2 "File/title/tags look specification-oriented."
        if containsAny textLower [| "api sketch"; "request"; "response"; "json"; "payload"; "endpoint"; "test baseline"; "fixtures" |] then
            add "implemented_spec" 3 "Content contains implementation scaffolding or API/test material."

        if containsAny pathLower [| "vision"; "design"; "analysis"; "deep-dive"; "deep_dive" |]
           || containsAny titleLower [| "vision"; "design"; "analysis"; "deep dive" |]
           || tagsContain [| "vision"; "design"; "analysis"; "deep-dive" |] then
            add "research_deep_dive" 2 "File/title/tags look like a design or deep-dive document."
        if containsAny textLower [| "core idea"; "invariant"; "deterministic"; "why this stays separate"; "tradeoff"; "must preserve" |] then
            add "research_deep_dive" 3 "Content contains deep-dive or invariant-oriented language."

        if containsAny pathLower [| "/research/"; "research-note"; "research-notes"; "experiment-log"; "notebook"; "hypothesis" |]
           || containsAny titleLower [| "research"; "experiment"; "hypothesis"; "notebook"; "negative result"; "open problem"; "known fragile" |]
           || tagsContain [| "research"; "experiment"; "hypothesis"; "notebook"; "fragile" |] then
            add "research_note" 3 "File/title/tags look like a research notebook or experiment note."
        if containsAny textLower [| "research note"; "experiment log"; "negative result"; "failed experiment"; "known fragile"; "open problem"; "hypothesis"; "artifact missing"; "evidence gap" |] then
            add "research_note" 3 "Content contains research-memory or experiment-log language."

        if containsAny pathLower [| "/decision-index/"; "decision-index"; "decision-register"; "decision-hub"; "decision-catalog"; "decision-registry" |]
           || containsAny titleLower [| "decision index"; "decision register"; "decision hub"; "decision catalog"; "decision registry"; "derived decision index" |]
           || tagsContain [| "decision-index"; "decision-register"; "decision-registry"; "derived-index" |] then
            add "decision_index" 4 "File/title/tags look like a decision index or register."
        if containsAny textLower [| "decision index"; "decision register"; "register entry"; "registry entry"; "derived index"; "historical snapshot"; "artifact id"; "canonical register" |] then
            add "decision_index" 3 "Content contains decision-index or register language."

        if containsAny pathLower [| "roadmap"; "scope"; "plan"; "brief"; "context"; "release"; "runbook"; "handoff"; "incident" |]
           || containsAny titleLower [| "roadmap"; "scope"; "plan"; "brief"; "context"; "release"; "runbook"; "handoff"; "incident" |]
           || tagsContain [| "roadmap"; "scope"; "plan"; "brief"; "context"; "release"; "runbook"; "incident"; "operations" |] then
            add "product_or_control_plane_doc" 3 "File/title/tags look roadmap or planning oriented."
        if containsAny textLower [| "bounded summary"; "summary only"; "non-authoritative"; "not the canonical owner"; "planning context"; "customer-facing summary"; "release note"; "runbook context"; "operator handoff"; "incident context" |] then
            add "product_or_control_plane_doc" 3 "Content explicitly marks the status note as bounded/non-authoritative."

        if containsAny pathLower [| "readme"; "index"; "overview" |]
           || containsAny titleLower [| "readme"; "index"; "overview" |] then
            add "entrypoint_or_index_doc" 3 "File/title looks like an entrypoint or index."

        if containsAny pathLower [| "adr"; "decision"; "proposal"; "review" |]
           || containsAny titleLower [| "adr"; "decision"; "proposal"; "review" |]
           || tagsContain [| "decision"; "proposal"; "review" |] then
            add "review_or_decision_record" 3 "File/title/tags look like review or decision material."
        if containsAny textLower [| "accepted"; "rejected"; "decision"; "proposal" |] then
            add "review_or_decision_record" 2 "Content uses review/decision terminology."

        let ranked =
            scores
            |> Seq.map (fun kv ->
                let evidence =
                    if reasons.ContainsKey(kv.Key) then reasons.[kv.Key] |> Seq.distinct |> Seq.toArray
                    else [||]
                kv.Key, kv.Value, evidence)
            |> Seq.sortByDescending (fun (_, score, _) -> score)
            |> Seq.toArray

        let topScore =
            if ranked.Length = 0 then 0
            else
                let _, score, _ = ranked.[0]
                score

        if ranked.Length = 0 || topScore < 3 then
            "unknown", 0.25, [| "No strong role signals were found; keeping the role honest as unknown." |]
        elif ranked.Length > 1 then
            let topRole, topScore, topEvidence = ranked.[0]
            let secondRole, secondScore, secondEvidence = ranked.[1]
            if secondScore >= topScore - 1 && secondScore >= 3 then
                let evidence =
                    Array.append
                        [| sprintf "Competing role signals: %s (%d) vs %s (%d)." topRole topScore secondRole secondScore |]
                        (Array.append topEvidence secondEvidence |> Array.distinct)
                "mixed", 0.55, evidence
            else
                let confidence = clamp01 (0.45 + (float topScore / 10.0) - (float secondScore / 20.0))
                topRole, confidence, topEvidence
        else
            let topRole, topScore, topEvidence = ranked.[0]
            let confidence = clamp01 (0.45 + (float topScore / 10.0))
            topRole, confidence, topEvidence

    let private buildDocProfiles (index: DocIndex) (repoRoot: string) (allChunks: DocChunk[]) =
        let backlinkCounts =
            index.Links
            |> Array.fold (fun (counts: Dictionary<string, int>) link ->
                if String.IsNullOrWhiteSpace(link.TargetResolved) then
                    counts
                else
                    let key = normPath link.TargetResolved
                    let current =
                        match counts.TryGetValue(key) with
                        | true, count -> count
                        | _ -> 0
                    counts.[key] <- current + 1
                    counts) (Dictionary<string, int>(StringComparer.OrdinalIgnoreCase))

        allChunks
        |> Array.groupBy (fun chunk -> normPath chunk.FilePath)
        |> Array.map (fun (_, sections) ->
            let filePath = sections.[0].FilePath
            let fm = index.Frontmatters |> Map.tryFind filePath
            let title =
                fm
                |> Option.map (fun frontmatter ->
                    if String.IsNullOrWhiteSpace(frontmatter.Title) then Path.GetFileNameWithoutExtension(filePath)
                    else frontmatter.Title)
                |> Option.defaultValue (Path.GetFileNameWithoutExtension(filePath))
            let tags = fm |> Option.map (fun frontmatter -> frontmatter.Tags) |> Option.defaultValue [||]
            let backlinks =
                match backlinkCounts.TryGetValue(normPath filePath) with
                | true, count -> count
                | _ -> 0
            let role, roleConfidence, roleEvidence = classifyDocRole filePath title tags sections backlinks
            {
                FilePath = filePath
                RelativePath = relativePath repoRoot filePath
                Title = title
                Tags = tags
                Backlinks = backlinks
                Sections = sections |> Array.sortBy (fun section -> section.StartLine)
                Role = role
                RoleConfidence = roleConfidence
                RoleEvidence = roleEvidence
            })

    let private statusSectionScore (section: DocChunk) =
        let headingLower = lower section.Heading
        let textLower = lower (section.Heading + "\n" + section.Content)
        let mutable score = 0
        if containsAny headingLower [| "status"; "progress"; "current"; "wave"; "gate"; "baseline"; "platform note"; "context snapshot" |] then score <- score + 2
        if containsAny textLower [| "current status"; "current gate"; "active wave"; "delivered"; "tests baseline"; "current platform note"; "currently" |] then score <- score + 2
        if containsAny textLower [| "tests"; "passing"; "baseline"; "wave"; "gate" |] then score <- score + 1
        score

    let private statusPhraseHits (text: string) =
        [| "current status"; "current gate"; "active wave"; "tests baseline"; "delivered"; "current platform note"; "currently" |]
        |> Array.filter (fun phrase -> lower text |> fun haystack -> haystack.Contains(phrase))

    let private statusSimilarity (left: DocChunk) (right: DocChunk) =
        let leftTokens = normalizedTokens (normalizeStatusText (left.Heading + "\n" + left.Content))
        let rightTokens = normalizedTokens (normalizeStatusText (right.Heading + "\n" + right.Content))
        jaccard leftTokens rightTokens

    let private sectionSignals (section: DocChunk) =
        let textLower = lower (section.Heading + "\n" + section.Content)
        let uniqueHits =
            [| "must"; "invariant"; "deterministic"; "tradeoff"; "why"; "core idea"; "preserve"; "separate" |]
            |> Array.filter (fun term -> textLower.Contains(term))
        let genericHits =
            [| "api"; "endpoint"; "json"; "payload"; "request"; "response"; "schema"; "test baseline"; "fixtures"; "example" |]
            |> Array.filter (fun term -> textLower.Contains(term))
        uniqueHits, genericHits

    let private decisionArtifactReferenceCount (doc: DocProfile) =
        doc.Sections
        |> Array.sumBy (fun section -> Regex.Matches(section.Content, @"\ba-[a-z0-9]{8}\b", RegexOptions.IgnoreCase).Count)

    let private decisionSectionSignals (section: DocChunk) =
        let headingLower = lower section.Heading
        let textLower = lower (section.Heading + "\n" + section.Content)
        let decisionHits =
            [|
                "decision"; "rationale"; "consequences"; "status"; "accepted"; "rejected"; "withdrawn"; "promoted";
                "strategic bet"; "candidate"; "considered"; "artifact"; "adr"; "proposal"
            |]
            |> Array.filter (fun term -> headingLower.Contains(term) || textLower.Contains(term))
        decisionHits

    let private researchSectionSignals (section: DocChunk) =
        let headingLower = lower section.Heading
        let textLower = lower (section.Heading + "\n" + section.Content)
        let researchHits =
            [|
                "research note"; "research notebook"; "research log"; "experiment"; "experiment log"; "finding";
                "observation"; "hypothesis"; "negative result"; "failed experiment"; "disproven"; "known fragile";
                "open problem"; "candidate memory"; "probe"; "investigation"; "evidence gap"; "artifact missing"
            |]
            |> Array.filter (fun term -> headingLower.Contains(term) || textLower.Contains(term))
        let conflictHits =
            [|
                "conflicts with"; "contradicts"; "disagrees with"; "competing hypothesis"; "conflicting note";
                "unresolved contradiction"; "needs human review"
            |]
            |> Array.filter (fun term -> textLower.Contains(term))
        let evidenceGapHits =
            [|
                "artifact missing"; "missing artifact"; "evidence gap"; "evidence link missing";
                "trace capture link is missing"; "restore evidence"; "restore the link"
            |]
            |> Array.filter (fun term -> textLower.Contains(term))
        researchHits, conflictHits, evidenceGapHits

    let private decisionIndexSectionSignals (section: DocChunk) =
        let headingLower = lower section.Heading
        let textLower = lower (section.Heading + "\n" + section.Content)
        let indexHits =
            [|
                "decision index"; "decision register"; "decision hub"; "decision catalog"; "decision registry";
                "register entry"; "registry entry"; "index entry"; "canonical register"; "decision target"
            |]
            |> Array.filter (fun term -> headingLower.Contains(term) || textLower.Contains(term))
        let contradictionHits =
            [|
                "contradiction"; "linked target disagrees"; "target disagrees"; "status mismatch";
                "reconcile this contradiction"; "correction guidance"; "needs human review"
            |]
            |> Array.filter (fun term -> textLower.Contains(term))
        let missingTargetHits =
            [|
                "artifact id unknown"; "artifact id missing"; "unknown artifact id"; "missing artifact id";
                "broken link"; "moved link"; "target moved"; "target missing"; "repair the target"; "repair the link"
            |]
            |> Array.filter (fun term -> textLower.Contains(term))
        let derivedHits =
            [|
                "derived index"; "derived register"; "derived decision index"; "generated from";
                "not the canonical decision register"; "not the canonical register"
            |]
            |> Array.filter (fun term -> textLower.Contains(term))
        let historicalHits =
            [|
                "historical snapshot"; "dated provenance"; "written at the time"; "recorded on"; "as of "
            |]
            |> Array.filter (fun term -> textLower.Contains(term))
        let datedHits =
            [|
                if Regex.IsMatch(textLower, @"\b20\d{2}[-/]\d{2}[-/]\d{2}\b", RegexOptions.IgnoreCase) then
                    "dated snapshot marker"
            |]
        indexHits, contradictionHits, missingTargetHits, derivedHits, Array.append historicalHits datedHits

    let private sectionBrokenLinks (index: DocIndex) (sourceFile: string) (section: DocChunk) =
        index.Links
        |> Array.filter (fun link ->
            normPath link.SourceFile = normPath sourceFile
            && link.SourceHeading = section.Heading
            && String.IsNullOrWhiteSpace(link.TargetResolved))

    let private docBrokenLinks (index: DocIndex) (sourceFile: string) =
        index.Links
        |> Array.filter (fun link ->
            normPath link.SourceFile = normPath sourceFile
            && String.IsNullOrWhiteSpace(link.TargetResolved))

    let private claimsCurrentAuthority (text: string) =
        let textLower = lower text
        let negated =
            containsAny textLower [|
                "not current"; "not the current"; "not latest"; "not the latest"; "not next"; "not the next"
            |]
        let positive =
            containsAny textLower [|
                "current authority"; "latest authority"; "next authority"; "current gate"; "active wave";
                "current status"; "latest status"; "next gate"; "current recommendation"
            |]
        positive && not negated

    let private sectionPairSimilarity (left: DocChunk) (right: DocChunk) =
        let leftTokens = normalizedTokens (left.Heading + "\n" + left.Content)
        let rightTokens = normalizedTokens (right.Heading + "\n" + right.Content)
        jaccard leftTokens rightTokens

    let private sectionSimilarity (section: DocChunk) (otherSections: DocChunk[]) =
        otherSections
        |> Array.filter (fun other ->
            not (normPath other.FilePath = normPath section.FilePath
                 && other.StartLine = section.StartLine
                 && other.Heading = section.Heading))
        |> Array.map (sectionPairSimilarity section)
        |> Array.fold max 0.0

    let private primarySectionPath (doc: DocProfile) =
        doc.Sections
        |> Array.tryFind (fun section -> section.Level > 0 && firstSnippet section.Content <> "")
        |> Option.orElseWith (fun () -> doc.Sections |> Array.tryFind (fun section -> section.Level > 0))
        |> Option.orElseWith (fun () -> doc.Sections |> Array.tryHead)
        |> Option.map (fun section -> section.HeadingPath)
        |> Option.defaultValue doc.Title

    let private docTokenSet (doc: DocProfile) =
        doc.Sections
        |> Array.map (fun section -> section.Heading + "\n" + section.Content)
        |> String.concat "\n"
        |> normalizedTokens

    let private docSimilarity (left: DocProfile) (right: DocProfile) =
        jaccard (docTokenSet left) (docTokenSet right)

    let private nearestDocCluster (docs: DocProfile[]) (doc: DocProfile) =
        docs
        |> Array.filter (fun other -> normPath other.FilePath <> normPath doc.FilePath)
        |> Array.map (fun other -> other, docSimilarity doc other)
        |> Array.sortByDescending snd
        |> Array.tryHead
        |> Option.filter (fun (_, similarity) -> similarity >= 0.18)
        |> Option.map (fun (other, similarity) -> other.RelativePath, similarity)

    let private nearestSectionCluster (docs: DocProfile[]) (section: DocChunk) =
        docs
        |> Array.collect (fun doc -> doc.Sections |> Array.map (fun other -> doc, other))
        |> Array.filter (fun (_, other) ->
            not (normPath other.FilePath = normPath section.FilePath
                 && other.StartLine = section.StartLine
                 && other.Heading = section.Heading))
        |> Array.map (fun (doc, other) -> doc, other, sectionPairSimilarity section other)
        |> Array.sortByDescending (fun (_, _, similarity) -> similarity)
        |> Array.tryHead
        |> Option.filter (fun (_, _, similarity) -> similarity >= 0.15)
        |> Option.map (fun (doc, other, similarity) -> sprintf "%s :: %s" doc.RelativePath other.HeadingPath, similarity)

    let private documentLinksToTarget (index: DocIndex) (sourceFile: string) (targetFile: string) =
        index.Links
        |> Array.exists (fun link ->
            normPath link.SourceFile = normPath sourceFile
            && link.TargetResolved <> ""
            && normPath link.TargetResolved = normPath targetFile)

    let private sectionLinksToTarget (index: DocIndex) (sourceFile: string) (section: DocChunk) (targetFile: string) =
        index.Links
        |> Array.exists (fun link ->
            normPath link.SourceFile = normPath sourceFile
            && link.SourceHeading = section.Heading
            && link.TargetResolved <> ""
            && normPath link.TargetResolved = normPath targetFile)

    let private boundedLiveSummarySignals (index: DocIndex) (doc: DocProfile) (section: DocChunk) (candidateDoc: DocProfile) =
        let textLower = lower (section.Heading + "\n" + section.Content)
        let reasons = ResizeArray<string>()
        let scopeCue =
            containsAny textLower [|
                "summary only"; "bounded summary"; "context only"; "context snapshot"; "planning context"; "for planning";
                "customer-facing summary"; "customer recap"; "brief status note"; "reader-facing summary"; "status recap";
                "non-authoritative summary"; "not the canonical owner"; "for this brief"; "for this update"; "for operators skimming";
                "release note"; "release recap"; "runbook context"; "operator handoff"; "incident context"; "operational summary"
            |]
            || containsAny section.Heading [| "context snapshot"; "brief"; "recap"; "summary"; "release"; "handoff" |]
        let redirectCue =
            containsAny textLower [|
                "authoritative live snapshot"; "authoritative status"; "see current status"; "for exact status";
                "for the full live snapshot"; "for the current live snapshot"; "for full details"
            |]
        let linkToOwner =
            sectionLinksToTarget index doc.FilePath section candidateDoc.FilePath
            || documentLinksToTarget index doc.FilePath candidateDoc.FilePath

        if scopeCue then reasons.Add("Content explicitly scopes the live summary as bounded/non-authoritative.")
        if linkToOwner then reasons.Add(sprintf "Section links to canonical owner `%s`." candidateDoc.RelativePath)
        if redirectCue then reasons.Add("Section redirects readers to the canonical owner for full detail.")
        if doc.Role = "product_or_control_plane_doc" then reasons.Add("Document role is planning/control-plane rather than canonical owner.")

        scopeCue && linkToOwner, reasons.ToArray()

    let private sectionMentionsEntity (entity: string) (section: DocChunk) =
        let haystack = lower (section.Heading + "\n" + section.Content)
        let compactEntity = entity.Replace(".", "").Replace("-", "").Replace("_", "")
        haystack.Contains(entity)
        || haystack.Contains(entity.Replace(".", " "))
        || (compactEntity <> entity && haystack.Replace(" ", "").Contains(compactEntity))

    let private determineAction proposedAction confidence risk =
        if confidence < 0.70 || risk = "high" then "needs_human_review" else proposedAction

    let private normalizeSuggestedAction (suggestedAction: string) =
        match suggestedAction with
        | "replace_with_pointer" -> "link"
        | "compact" -> "reduce"
        | "move_to_archive" -> "archive"
        | "needs_owner" -> "needs_human_review"
        | _ -> suggestedAction

    let private normalizeNearestOwnerOrCluster (nearestOwnerOrCluster: string) =
        if String.IsNullOrWhiteSpace(nearestOwnerOrCluster) then "unknown"
        else nearestOwnerOrCluster

    let private hygieneFinding
        (findingType: string)
        (scenarioId: string)
        (sourceFile: string)
        (sourceSection: string)
        (files: string[])
        (sections: string[])
        (docRole: string)
        (canonicalOwnerCandidate: string)
        (canonicalOwnerConfidence: float)
        (canonicalOwnerStatus: string)
        (nearestOwnerOrCluster: string)
        (evidence: string[])
        (suggestedAction: string)
        (expectedHumanActionShape: string)
        (confidence: float)
        (risk: string)
        (preserveNotes: string)
        (whyFlagged: string) =
        let nearestOwnerOrCluster = normalizeNearestOwnerOrCluster nearestOwnerOrCluster
        let suggestedAction = normalizeSuggestedAction suggestedAction
        mdict [
            "finding_type", box findingType
            "acceptance_scenario_id", box scenarioId
            "source_file", box sourceFile
            "source_section", box sourceSection
            "files", box files
            "sections", box sections
            "doc_role", box docRole
            "canonical_owner_candidate", box canonicalOwnerCandidate
            "canonical_owner_confidence", box (Math.Round(canonicalOwnerConfidence, 3))
            "canonical_owner_status", box canonicalOwnerStatus
            "nearest_owner_or_cluster", box nearestOwnerOrCluster
            "evidence", box evidence
            "suggested_action", box suggestedAction
            "expected_human_action_shape", box expectedHumanActionShape
            "confidence", box (Math.Round(confidence, 3))
            "risk", box risk
            "preserve_notes", box preserveNotes
            "why_flagged", box whyFlagged
        ]

    let private findingString (finding: IDictionary<string, obj>) (key: string) =
        match finding.TryGetValue(key) with
        | true, value when not (isNull value) -> string value
        | _ -> ""

    let private chronologyCompactionTerms =
        [|
            "delivered"
            "delivery"
            "tests baseline"
            "baseline"
            "current wave"
            "current gate"
            "current register"
            "current recommendation"
            "active rollout"
            "rollout"
            "release readiness"
            "promoted rollout"
        |]

    let private chronologyCompactionHits (text: string) =
        let haystack = lower text
        chronologyCompactionTerms
        |> Array.filter haystack.Contains
        |> Array.distinct

    let private isCompactionProfile (profile: string) =
        String.Equals(profile, "compaction", StringComparison.OrdinalIgnoreCase)

    let private compactionTrace (message: string) =
        CliOutput.info "[hygiene/compaction] %s" message

    let private compactionShortlistLimit (limit: int) =
        max 1 (min 25 limit)

    let private compactionActionWeight (suggestedAction: string) =
        match suggestedAction with
        | "reduce" -> 18.0
        | "link" -> 10.0
        | "needs_human_review" -> 4.0
        | _ -> 0.0

    let private compactionRiskPenalty (risk: string) =
        match risk with
        | "low" -> 0.0
        | "medium" -> 6.0
        | "high" -> 16.0
        | _ -> 8.0

    let private compactionScopeContains (scopeDocPaths: Set<string> option) (relativePath: string) =
        match scopeDocPaths with
        | None -> true
        | Some paths -> paths.Contains(relativePath)

    let private hasFrontierOrInvariantSummary (doc: DocProfile) =
        doc.Sections
        |> Array.exists (fun section ->
            let text = section.Heading + "\n" + section.Content
            containsAny text [|
                "invariant"
                "frontier"
                "durable order"
                "durable summary"
                "frontier summary"
                "next phase"
                "open problem"
                "open questions"
                "watch area"
                "guardrail"
                "core idea"
                "why this matters"
                "durable context"
            |])

    let private ambientCompactionReplacementShape (family: string) =
        match family with
        | "duplicate_active_state" ->
            "Replace the duplicated live-status detail with a short durable summary plus a pointer to the canonical owner."
        | "chronology_heavy" ->
            "Replace closeout chronology with a brief frontier summary; let source/tests/git own the timeline detail."
        | "section_compaction" ->
            "Keep the durable reasoning, but collapse implementation-history scaffolding into a short summary or pointer."
        | "canonical_owner_conflict" ->
            "Confirm the canonical owner first, then replace competing active-state text with a pointer."
        | "missing_invariant_or_frontier_summary" ->
            "Add a 2-3 bullet invariant/frontier summary first, then trim or link out the active-history detail."
        | _ ->
            "Replace the distracting maintenance detail with a brief durable summary and a pointer to the canonical owner."

    let private decorateCompactionFinding
        (family: string)
        (reason: string)
        (chronologyHits: string[])
        (score: float)
        (finding: Dictionary<string, obj>) =
        finding.["hygiene_profile"] <- box "compaction"
        finding.["compaction_family"] <- box family
        finding.["compaction_reason"] <- box reason
        finding.["compaction_score"] <- box (Math.Round(score, 3))
        finding.["compaction_marker_hits"] <- box chronologyHits
        finding

    let private hygieneCompactionCore (index: DocIndex) (allChunks: DocChunk[]) (repoRoot: string) (limit: int) (scopeDocPaths: Set<string> option) (emitTrace: bool) =
        let shortlistLimit = compactionShortlistLimit limit
        let docs = buildDocProfiles index repoRoot allChunks
        let scopedDocs = docs |> Array.filter (fun doc -> compactionScopeContains scopeDocPaths doc.RelativePath)
        let findings = ResizeArray<float * Dictionary<string, obj>>()
        let trace message = if emitTrace then compactionTrace message

        trace (sprintf "fast_path=enabled limit=%d shortlist_limit=%d" limit shortlistLimit)
        trace "bypass=broad_findings,orphan_pass,gap_pass,reassurance_pass,out_of_pool_section_similarity"
        match scopeDocPaths with
        | Some _ -> trace (sprintf "scope=changed_docs docs=%d" scopedDocs.Length)
        | None -> ()

        let docsWithStatus =
            docs
            |> Array.choose (fun doc ->
                let statusSections =
                    doc.Sections
                    |> Array.filter (fun section ->
                        section.Level > 0
                        && firstSnippet section.Content <> ""
                        && statusSectionScore section >= 3)
                if statusSections.Length = 0 then None else Some (doc, statusSections))

        let rankedStatusDocs =
            docsWithStatus
            |> Array.map (fun (doc, statusSections) ->
                let reasons = ResizeArray<string>()
                let mutable score = statusSections.Length
                if doc.Role = "canonical_live_status_owner" then
                    score <- score + 4
                    reasons.Add("Document role is canonical_live_status_owner.")
                if containsAny doc.RelativePath [| "status"; "current-status"; "progress" |]
                   || containsAny doc.Title [| "status"; "progress" |] then
                    score <- score + 3
                    reasons.Add("File/title looks like a live-status owner.")
                if doc.Backlinks > 0 then
                    score <- score + (min 2 doc.Backlinks)
                    reasons.Add(sprintf "Referenced by %d other document(s)." doc.Backlinks)
                if doc.Role = "historical_archive" then
                    score <- score - 2
                    reasons.Add("Archive role lowers canonical-owner confidence.")
                doc, statusSections, score, reasons.ToArray())
            |> Array.sortByDescending (fun (_, _, score, _) -> score)

        let ownerCandidateBudget =
            rankedStatusDocs.Length
            |> min (max 3 (shortlistLimit * 2))

        let candidateOwner =
            if rankedStatusDocs.Length = 0 then
                None
            else
                let ownerCandidates = rankedStatusDocs |> Array.truncate ownerCandidateBudget
                let candidateDoc, candidateSections, candidateScore, candidateReasons = ownerCandidates.[0]
                let secondScore =
                    if ownerCandidates.Length > 1 then
                        let _, _, score, _ = ownerCandidates.[1]
                        score
                    else 0
                let candidateConfidence =
                    clamp01 (0.45 + (float candidateScore / 12.0) + (float (max 0 (candidateScore - secondScore)) / 15.0))
                let candidateStatus =
                    if candidateConfidence >= 0.85 && candidateScore - secondScore >= 3 then "asserted" else "candidate"
                Some (candidateDoc, candidateSections, candidateScore, secondScore, candidateConfidence, candidateStatus, candidateReasons)

        let analyzedStatusDocs =
            match candidateOwner with
            | None -> [||]
            | Some (candidateDoc, candidateSections, candidateScore, secondScore, candidateConfidence, candidateStatus, candidateReasons) ->
                trace (
                    sprintf
                        "canonical_owner=%s status=%s confidence=%.3f owner_score=%d next_score=%d"
                        candidateDoc.RelativePath
                        candidateStatus
                        candidateConfidence
                        candidateScore
                        secondScore)

                if candidateStatus <> "asserted" then
                    let conflictFinding =
                        hygieneFinding
                            "canonical_owner_candidate"
                            "neon-live-status"
                            candidateDoc.RelativePath
                            (primarySectionPath candidateDoc)
                            [| candidateDoc.RelativePath |]
                            (candidateSections |> Array.map (fun section -> section.HeadingPath))
                            candidateDoc.Role
                            candidateDoc.RelativePath
                            candidateConfidence
                            candidateStatus
                            candidateDoc.RelativePath
                            (Array.append
                                [| sprintf "Candidate score %d vs next-best %d." candidateScore secondScore |]
                                (Array.append candidateReasons candidateDoc.RoleEvidence |> Array.distinct))
                            "needs_human_review"
                            "needs_human_review"
                            candidateConfidence
                            "medium"
                            "Keep as the leading candidate until a maintainer confirms ownership."
                            "Resolve canonical ownership before duplicate-state cleanup so compaction loops have a stable owner."
                    let conflictScore =
                        45.0
                        + compactionActionWeight "needs_human_review"
                        + (candidateConfidence * 40.0)
                        - compactionRiskPenalty "medium"
                    findings.Add((
                        conflictScore,
                        decorateCompactionFinding
                            "canonical_owner_conflict"
                            "Resolve canonical ownership before repeated compaction loops so duplicate-state cleanup has a stable owner."
                            [||]
                            conflictScore
                            conflictFinding))

                let statusDocBudget =
                    max 0 (docsWithStatus.Length - 1)
                    |> min (max 4 (shortlistLimit * 3))

                let statusCandidates =
                    rankedStatusDocs
                    |> Array.filter (fun (doc, _, _, _) ->
                        doc.RelativePath <> candidateDoc.RelativePath
                        && compactionScopeContains scopeDocPaths doc.RelativePath)
                    |> Array.truncate statusDocBudget

                trace (sprintf "owner_candidate_budget=%d status_doc_budget=%d" ownerCandidateBudget statusDocBudget)

                for (doc, statusSections, _, _) in statusCandidates do
                    let bestSimilarity =
                        statusSections
                        |> Array.map (fun section ->
                            candidateSections
                            |> Array.map (fun candidateSection -> statusSimilarity section candidateSection)
                            |> Array.fold max 0.0)
                        |> Array.fold max 0.0

                    let staleMarkers =
                        statusSections
                        |> Array.collect (fun section -> statusPhraseHits section.Content)
                        |> Array.distinct

                    let boundedSections =
                        statusSections
                        |> Array.choose (fun section ->
                            let bounded, reasons = boundedLiveSummarySignals index doc section candidateDoc
                            if bounded then Some (section, reasons) else None)

                    let explicitBounded = boundedSections.Length > 0

                    if not explicitBounded then
                        let sourceSection = statusSections.[0].HeadingPath
                        let proposedAction, expectedHumanActionShape, risk, preserveNotes, whyFlagged =
                            if bestSimilarity >= 0.45 then
                                "replace_with_pointer",
                                "link",
                                (if doc.Role = "mixed" || doc.Role = "unknown" then "high" else "low"),
                                "Prefer replacing duplicate current-state text with a pointer to the canonical owner.",
                                "This document contains role-aware live-state text that overlaps with or competes with the canonical current-status owner."
                            elif staleMarkers.Length > 0 || doc.Role = "historical_archive" then
                                "compact",
                                "reduce",
                                (if doc.Role = "mixed" || doc.Role = "unknown" then "high" elif doc.Role = "historical_archive" then "medium" else "medium"),
                                (if doc.Role = "historical_archive" then "Historical current-state text should be reduced behind archive framing." else ""),
                                "This document contains role-aware live-state text that overlaps with or competes with the canonical current-status owner."
                            else
                                "needs_human_review",
                                "needs_human_review",
                                "high",
                                "",
                                "This document contains live-state language, but the workflow cannot safely classify its relationship to the canonical owner."

                        let confidence =
                            clamp01 (0.40 + (bestSimilarity / 1.4) + (float staleMarkers.Length / 12.0))

                        let finalAction =
                            if proposedAction = "needs_human_review" then proposedAction
                            else determineAction proposedAction confidence risk

                        let evidence =
                            Array.concat [|
                                [| sprintf "Best similarity to canonical owner candidate `%s`: %.2f." candidateDoc.RelativePath bestSimilarity |]
                                if staleMarkers.Length > 0 then [| sprintf "Stale-prone live-state markers: %s." (String.concat ", " staleMarkers) |] else [||]
                                [| sprintf "Section snippet: %s" (statusSections |> Array.map (fun section -> firstSnippet section.Content) |> Array.filter ((<>) "") |> Array.tryHead |> Option.defaultValue "(no snippet)") |]
                                doc.RoleEvidence
                            |]

                        let sectionTexts =
                            statusSections
                            |> Array.map (fun section -> section.Heading + "\n" + section.Content)

                        let chronologyHits =
                            String.concat "\n" (Array.concat [| [| sourceSection; preserveNotes; whyFlagged |]; sectionTexts |])
                            |> chronologyCompactionHits

                        let family, reason, familyWeight =
                            if expectedHumanActionShape = "link" then
                                "duplicate_active_state",
                                "Duplicate active-state text overlaps the canonical owner and is a strong link/reduce candidate.",
                                36.0
                            elif chronologyHits.Length > 0 then
                                "chronology_heavy",
                                "Chronology-heavy current-state residue is a strong compaction candidate; git/source/tests should own the timeline details.",
                                42.0
                            else
                                "duplicate_active_state",
                                "Duplicate active-state text overlaps the canonical owner and is a strong link/reduce candidate.",
                                36.0

                        let finding =
                            hygieneFinding
                                "live_status_triage"
                                (if chronologyHits.Length > 0 then "neon-live-status-stale" else "neon-live-status")
                                doc.RelativePath
                                sourceSection
                                [| doc.RelativePath |]
                                (statusSections |> Array.map (fun section -> section.HeadingPath))
                                doc.Role
                                candidateDoc.RelativePath
                                candidateConfidence
                                candidateStatus
                                candidateDoc.RelativePath
                                evidence
                                finalAction
                                expectedHumanActionShape
                                confidence
                                risk
                                preserveNotes
                                whyFlagged

                        let score =
                            familyWeight
                            + compactionActionWeight (findingString finding "suggested_action")
                            + (float chronologyHits.Length * 3.0)
                            + (confidence * 40.0)
                            - compactionRiskPenalty risk

                        findings.Add((score, decorateCompactionFinding family reason chronologyHits score finding))

                statusCandidates

        let mixedSectionCandidates =
            scopedDocs
            |> Array.collect (fun doc ->
                if doc.Role <> "mixed" then
                    [||]
                else
                    let meaningfulSections =
                        doc.Sections
                        |> Array.filter (fun section -> section.Level > 0 && firstSnippet section.Content <> "")
                    let hasStatusCompanion =
                        meaningfulSections
                        |> Array.exists (fun section -> statusSectionScore section >= 3)
                    meaningfulSections
                    |> Array.filter (fun section -> statusSectionScore section < 3)
                    |> Array.choose (fun section ->
                        let uniqueHits, genericHits = sectionSignals section
                        let chronologyHits = chronologyCompactionHits (section.Heading + "\n" + section.Content)
                        let looksReducible = genericHits.Length > 0 || chronologyHits.Length > 0
                        if not looksReducible then None
                        else
                            let quickScore =
                                (genericHits.Length * 3)
                                + (chronologyHits.Length * 2)
                                + (if hasStatusCompanion then 3 else 0)
                            Some (doc, section, uniqueHits, genericHits, chronologyHits, hasStatusCompanion, quickScore)))
            |> Array.sortByDescending (fun (_, _, _, _, _, _, quickScore) -> quickScore)

        match scopeDocPaths with
        | Some _ ->
            let missingSummaryCandidates =
                scopedDocs
                |> Array.choose (fun doc ->
                    let statusSections =
                        doc.Sections
                        |> Array.filter (fun section ->
                            section.Level > 0
                            && firstSnippet section.Content <> ""
                            && ((statusSectionScore section >= 3)
                                || (chronologyCompactionHits (section.Heading + "\n" + section.Content)).Length > 0))

                    let hasMissingSummaryPressure = statusSections.Length > 0 && not (hasFrontierOrInvariantSummary doc)

                    if not hasMissingSummaryPressure then
                        None
                    else
                        let markerHits =
                            statusSections
                            |> Array.collect (fun section -> chronologyCompactionHits (section.Heading + "\n" + section.Content))
                            |> Array.distinct

                        let sourceSection =
                            statusSections
                            |> Array.tryHead
                            |> Option.map (fun section -> section.HeadingPath)
                            |> Option.defaultValue (primarySectionPath doc)

                        let canonicalOwnerCandidate, canonicalOwnerConfidence, canonicalOwnerStatus =
                            match candidateOwner with
                            | Some (candidateDoc, _, _, _, candidateConfidence, candidateStatus, _) when candidateDoc.RelativePath <> doc.RelativePath ->
                                candidateDoc.RelativePath, candidateConfidence, candidateStatus
                            | _ -> "", 0.0, ""

                        let evidence =
                            Array.concat [|
                                [| "Changed/current doc carries active-state or chronology residue without a durable invariant/frontier summary." |]
                                if markerHits.Length > 0 then [| sprintf "Residue markers: %s." (String.concat ", " markerHits) |] else [||]
                                [| sprintf "Section snippet: %s" (statusSections |> Array.map (fun section -> firstSnippet section.Content) |> Array.filter ((<>) "") |> Array.tryHead |> Option.defaultValue "(no snippet)") |]
                                doc.RoleEvidence
                            |]

                        let confidence =
                            clamp01 (0.60 + (float statusSections.Length / 10.0) + (float markerHits.Length / 12.0))
                        let finding =
                            hygieneFinding
                                "ambient_compaction_hint"
                                "missing-invariant-or-frontier-summary"
                                doc.RelativePath
                                sourceSection
                                [| doc.RelativePath |]
                                (statusSections |> Array.map (fun section -> section.HeadingPath))
                                doc.Role
                                canonicalOwnerCandidate
                                canonicalOwnerConfidence
                                canonicalOwnerStatus
                                canonicalOwnerCandidate
                                evidence
                                "needs_human_review"
                                "reduce"
                                confidence
                                "medium"
                                "Add a brief invariant/frontier summary before reducing active-wave or closeout detail."
                                "This changed/current doc lacks the durable summary that future readers need once the live detail is trimmed."

                        let score =
                            40.0
                            + (confidence * 40.0)
                            + (float markerHits.Length * 3.0)
                            - compactionRiskPenalty "medium"

                        let decorated =
                            decorateCompactionFinding
                                "missing_invariant_or_frontier_summary"
                                "Active-state residue is outrunning the durable invariant/frontier summary this doc needs to stay useful after cleanup."
                                markerHits
                                score
                                finding

                        Some (score, decorated))
                |> Array.sortByDescending fst
                |> Array.truncate 1

            for candidate in missingSummaryCandidates do
                findings.Add(candidate)
        | None -> ()

        let sectionBudget =
            mixedSectionCandidates.Length
            |> min (max 4 (shortlistLimit * 3))

        trace (sprintf "mixed_section_budget=%d" sectionBudget)

        let analyzedSections = mixedSectionCandidates |> Array.truncate sectionBudget
        let sectionPool = analyzedSections |> Array.map (fun (doc, section, _, _, _, _, _) -> doc, section)

        for (doc, section, uniqueHits, genericHits, chronologyHits, _, _) in analyzedSections do
            let comparableSections =
                sectionPool
                |> Array.filter (fun (_, other) ->
                    not (normPath other.FilePath = normPath section.FilePath
                         && other.StartLine = section.StartLine
                         && other.Heading = section.Heading))

            let maxSimilarity =
                comparableSections
                |> Array.map (fun (_, other) -> sectionPairSimilarity section other)
                |> Array.fold max 0.0

            let nearestCluster =
                comparableSections
                |> Array.map (fun (otherDoc, otherSection) -> otherDoc, otherSection, sectionPairSimilarity section otherSection)
                |> Array.sortByDescending (fun (_, _, similarity) -> similarity)
                |> Array.tryHead
                |> Option.filter (fun (_, _, similarity) -> similarity >= 0.15)
                |> Option.map (fun (otherDoc, otherSection, _) -> sprintf "%s :: %s" otherDoc.RelativePath otherSection.HeadingPath)
                |> Option.defaultValue ""

            let isUnique = uniqueHits.Length >= 2 || (uniqueHits.Length >= 1 && maxSimilarity < 0.18)
            let isGeneric = genericHits.Length >= 2 || chronologyHits.Length > 0 || (genericHits.Length >= 1 && maxSimilarity >= 0.12)

            if not (isUnique && not isGeneric) then
                let proposedAction, expectedHumanActionShape, preserveNotes, risk, whyFlagged =
                    if isGeneric && not isUnique then
                        (if maxSimilarity >= 0.18 || genericHits.Length >= 3 then "replace_with_pointer" else "compact"),
                        (if maxSimilarity >= 0.18 || genericHits.Length >= 3 then "link" else "reduce"),
                        "",
                        "medium",
                        "This section looks like reducible scaffolding inside a mixed-role document."
                    else
                        "needs_human_review",
                        "needs_human_review",
                        "Section has both preserve and reduce signals or is too ambiguous for a safe automatic call.",
                        "high",
                        "This section needs explicit human review so mixed-role documents do not silently lose important context."

                let confidence =
                    clamp01 (0.45 + (float uniqueHits.Length / 10.0) + (float genericHits.Length / 10.0) + (maxSimilarity / 4.0))

                let finalAction =
                    if proposedAction = "needs_human_review" then proposedAction
                    else determineAction proposedAction confidence risk

                let evidence =
                    Array.concat [|
                        if uniqueHits.Length > 0 then [| sprintf "Unique markers: %s." (String.concat ", " uniqueHits) |] else [||]
                        if genericHits.Length > 0 then [| sprintf "Generic scaffolding markers: %s." (String.concat ", " genericHits) |] else [||]
                        if chronologyHits.Length > 0 then [| sprintf "Chronology-heavy markers: %s." (String.concat ", " chronologyHits) |] else [||]
                        [| sprintf "In-pool section similarity: %.2f." maxSimilarity |]
                        [| sprintf "Section snippet: %s" (firstSnippet section.Content) |]
                        doc.RoleEvidence
                    |]

                let family, reason, familyWeight =
                    if chronologyHits.Length > 0 then
                        "chronology_heavy",
                        "This reducible section is dominated by chronology-heavy rollout/baseline residue rather than durable knowledge.",
                        34.0
                    else
                        "section_compaction",
                        "This section is the reducible part of a mixed or superseded document while durable reasoning can remain in place.",
                        30.0

                let finding =
                    hygieneFinding
                        "section_triage"
                        "section-preserve-reduce"
                        doc.RelativePath
                        section.HeadingPath
                        [| doc.RelativePath |]
                        [| section.HeadingPath |]
                        doc.Role
                        ""
                        0.0
                        ""
                        nearestCluster
                        evidence
                        finalAction
                        expectedHumanActionShape
                        confidence
                        risk
                        preserveNotes
                        whyFlagged

                let score =
                    familyWeight
                    + compactionActionWeight (findingString finding "suggested_action")
                    + (float chronologyHits.Length * 3.0)
                    + (confidence * 40.0)
                    - compactionRiskPenalty risk

                findings.Add((score, decorateCompactionFinding family reason chronologyHits score finding))

        trace (
            sprintf
                "analysis docs_total=%d status_docs=%d status_docs_analyzed=%d mixed_sections=%d mixed_sections_analyzed=%d"
                docs.Length
                docsWithStatus.Length
                analyzedStatusDocs.Length
                mixedSectionCandidates.Length
                analyzedSections.Length)

        let ranked =
            findings
            |> Seq.sortBy (fun (score, finding) ->
                -score,
                findingString finding "source_file",
                findingString finding "source_section")
            |> Seq.toArray

        let selected = ResizeArray<float * Dictionary<string, obj>>()
        let selectedKeys = HashSet<string>(StringComparer.OrdinalIgnoreCase)
        let seenFamilies = HashSet<string>(StringComparer.OrdinalIgnoreCase)

        let candidateKey (finding: IDictionary<string, obj>) =
            String.concat "||" [|
                findingString finding "compaction_family"
                findingString finding "source_file"
                findingString finding "source_section"
            |]

        for (score, finding) in ranked do
            let family = findingString finding "compaction_family"
            let key = candidateKey finding
            if family <> "" && seenFamilies.Add(family) && selectedKeys.Add(key) then
                selected.Add((score, finding))

        for (score, finding) in ranked do
            if selected.Count < shortlistLimit then
                let key = candidateKey finding
                if selectedKeys.Add(key) then
                    selected.Add((score, finding))

        let ordered =
            selected
            |> Seq.sortBy (fun (score, finding) ->
                -score,
                findingString finding "source_file",
                findingString finding "source_section")
            |> Seq.truncate shortlistLimit
            |> Seq.toArray
            |> Array.mapi (fun index (_, finding) ->
                finding.["compaction_rank"] <- box (index + 1)
                finding)

        trace (sprintf "shortlist candidates=%d emitted=%d" findings.Count ordered.Length)

        if ordered.Length = 0 then
            [|
                mdict [
                    "hygiene_profile", box "compaction"
                    "note", box "No bounded compaction candidates detected for the current index."
                ]
            |]
        else
            ordered

    let private hygieneCompaction (index: DocIndex) (allChunks: DocChunk[]) (repoRoot: string) (limit: int) =
        hygieneCompactionCore index allChunks repoRoot limit None true

    let ambientCompactionHints (index: DocIndex) (chunks: DocChunk[] option) (repoRoot: string) (changedDocPaths: string[]) (limit: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks ->
            let scopeDocPaths =
                changedDocPaths
                |> Array.filter (String.IsNullOrWhiteSpace >> not)
                |> Array.map (fun path -> path.Replace('\\', '/'))
                |> Set.ofArray

            if scopeDocPaths.Count = 0 then
                [||]
            else
                hygieneCompactionCore index allChunks repoRoot limit (Some scopeDocPaths) false
                |> Array.choose (fun finding ->
                    if finding.ContainsKey("error") || finding.ContainsKey("note") then
                        None
                    else
                        let canonicalOwner =
                            let candidate = findingString finding "canonical_owner_candidate"
                            if String.IsNullOrWhiteSpace(candidate) then ""
                            else candidate

                        Some (mdict [
                            "file", box (findingString finding "source_file")
                            "section", box (findingString finding "source_section")
                            "family", box (findingString finding "compaction_family")
                            "why_this_distracts_future_context", box (findingString finding "compaction_reason")
                            "suggested_replacement_shape", box (ambientCompactionReplacementShape (findingString finding "compaction_family"))
                            "canonical_owner_or_link", box canonicalOwner
                            "suggested_action", box (findingString finding "suggested_action")
                        ]))

    let private hygieneBroad (index: DocIndex) (chunks: DocChunk[] option) (repoRoot: string) (_profile: string) (_limit: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks ->
            let docs = buildDocProfiles index repoRoot allChunks
            let findings = ResizeArray<Dictionary<string, obj>>()

            let docsWithStatus =
                docs
                |> Array.choose (fun doc ->
                    let statusSections =
                        doc.Sections
                        |> Array.filter (fun section ->
                            section.Level > 0
                            && firstSnippet section.Content <> ""
                            && statusSectionScore section >= 3)
                    if statusSections.Length = 0 then None else Some (doc, statusSections))

            if docsWithStatus.Length > 0 then
                let rankedCandidates =
                    docsWithStatus
                    |> Array.map (fun (doc, statusSections) ->
                        let reasons = ResizeArray<string>()
                        let mutable score = statusSections.Length
                        if doc.Role = "canonical_live_status_owner" then
                            score <- score + 4
                            reasons.Add("Document role is canonical_live_status_owner.")
                        if containsAny doc.RelativePath [| "status"; "current-status"; "progress" |]
                           || containsAny doc.Title [| "status"; "progress" |] then
                            score <- score + 3
                            reasons.Add("File/title looks like a live-status owner.")
                        if doc.Backlinks > 0 then
                            score <- score + (min 2 doc.Backlinks)
                            reasons.Add(sprintf "Referenced by %d other document(s)." doc.Backlinks)
                        if doc.Role = "historical_archive" then
                            score <- score - 2
                            reasons.Add("Archive role lowers canonical-owner confidence.")
                        doc, statusSections, score, reasons.ToArray())
                    |> Array.sortByDescending (fun (_, _, score, _) -> score)

                let candidateDoc, candidateSections, candidateScore, candidateReasons = rankedCandidates.[0]
                let secondScore =
                    if rankedCandidates.Length > 1 then
                        let _, _, score, _ = rankedCandidates.[1]
                        score
                    else 0
                let candidateConfidence =
                    clamp01 (0.45 + (float candidateScore / 12.0) + (float (max 0 (candidateScore - secondScore)) / 15.0))
                let candidateStatus =
                    if candidateConfidence >= 0.85 && candidateScore - secondScore >= 3 then "asserted" else "candidate"
                let candidateSuggestedAction =
                    determineAction
                        (if candidateStatus = "asserted" then "preserve" else "needs_owner")
                        candidateConfidence
                        (if candidateStatus = "asserted" then "low" else "medium")
                findings.Add(
                    hygieneFinding
                        "canonical_owner_candidate"
                        "neon-live-status"
                        candidateDoc.RelativePath
                        (primarySectionPath candidateDoc)
                        [| candidateDoc.RelativePath |]
                        (candidateSections |> Array.map (fun section -> section.HeadingPath))
                        candidateDoc.Role
                        candidateDoc.RelativePath
                        candidateConfidence
                        candidateStatus
                        candidateDoc.RelativePath
                        (Array.append
                            [| sprintf "Candidate score %d vs next-best %d." candidateScore secondScore |]
                            (Array.append candidateReasons candidateDoc.RoleEvidence |> Array.distinct))
                        candidateSuggestedAction
                        (if candidateStatus = "asserted" then "ignore" else "needs_human_review")
                        candidateConfidence
                        (if candidateStatus = "asserted" then "low" else "medium")
                        (if candidateStatus = "asserted" then "Preserve as the current canonical live-status owner." else "Keep as the leading candidate until a maintainer confirms ownership.")
                        "Role-aware live-status triage needs a canonical owner candidate before duplicate/stale copies can be judged safely.")

                for (doc, statusSections) in docsWithStatus |> Array.filter (fun (doc, _) -> doc.RelativePath <> candidateDoc.RelativePath) do
                    let bestSimilarity =
                        statusSections
                        |> Array.map (fun section ->
                            candidateSections
                            |> Array.map (fun candidateSection -> statusSimilarity section candidateSection)
                            |> Array.fold max 0.0)
                        |> Array.fold max 0.0
                    let staleMarkers =
                        statusSections
                        |> Array.collect (fun section -> statusPhraseHits section.Content)
                        |> Array.distinct
                    let boundedSections =
                        statusSections
                        |> Array.choose (fun section ->
                            let bounded, reasons = boundedLiveSummarySignals index doc section candidateDoc
                            if bounded then Some (section, reasons) else None)
                    let explicitBounded = boundedSections.Length > 0
                    let sourceSection =
                        if explicitBounded then boundedSections.[0] |> fst |> fun section -> section.HeadingPath
                        else statusSections.[0].HeadingPath
                    let boundedEvidence =
                        boundedSections
                        |> Array.collect snd
                        |> Array.distinct
                    let findingType, scenarioId, proposedAction, expectedHumanActionShape, risk, preserveNotes, whyFlagged =
                        if doc.Role = "historical_archive" then
                            "live_status_triage",
                            "neon-live-status-stale",
                            "move_to_archive",
                            "archive",
                            "medium",
                            "Historical current-state text should move behind archive framing.",
                            "This document contains role-aware live-state text that overlaps with or competes with the canonical current-status owner."
                        elif explicitBounded then
                            "bounded_live_summary_protection",
                            (if doc.Role = "product_or_control_plane_doc" then "bounded-live-summary-control" else "bounded-live-summary-protection"),
                            "preserve",
                            "ignore",
                            "low",
                            "Bounded summaries are valid when they stay clearly scoped and non-authoritative.",
                            "This section mentions current state, but it is explicitly bounded and points readers back to the canonical owner."
                        elif bestSimilarity >= 0.45 then
                            "live_status_triage",
                            "neon-live-status",
                            "replace_with_pointer",
                            "link",
                            (if doc.Role = "mixed" || doc.Role = "unknown" then "high" else "low"),
                            "Prefer replacing duplicate current-state text with a pointer to the canonical owner.",
                            "This document contains role-aware live-state text that overlaps with or competes with the canonical current-status owner."
                        elif staleMarkers.Length > 0 then
                            "live_status_triage",
                            "neon-live-status-stale",
                            "compact",
                            "reduce",
                            (if doc.Role = "mixed" || doc.Role = "unknown" then "high" else "medium"),
                            "",
                            "This document contains role-aware live-state text that overlaps with or competes with the canonical current-status owner."
                        else
                            "live_status_triage",
                            "neon-live-status-stale",
                            "needs_human_review",
                            "needs_human_review",
                            "high",
                            "",
                            "This document contains live-state language, but the workflow cannot safely classify its relationship to the canonical owner."

                    let confidence =
                        if explicitBounded then
                            clamp01 (0.74 + (float boundedEvidence.Length / 20.0))
                        else
                            clamp01 (0.40 + (bestSimilarity / 1.4) + (float staleMarkers.Length / 12.0))
                    let finalAction =
                        if explicitBounded then "preserve"
                        else determineAction proposedAction confidence risk
                    let evidence =
                        Array.concat [|
                            [| sprintf "Best similarity to canonical owner candidate `%s`: %.2f." candidateDoc.RelativePath bestSimilarity |]
                            if staleMarkers.Length > 0 then [| sprintf "Stale-prone live-state markers: %s." (String.concat ", " staleMarkers) |] else [||]
                            if boundedEvidence.Length > 0 then boundedEvidence else [||]
                            [| sprintf "Section snippet: %s" (statusSections |> Array.map (fun section -> firstSnippet section.Content) |> Array.filter ((<>) "") |> Array.tryHead |> Option.defaultValue "(no snippet)") |]
                            doc.RoleEvidence
                        |]
                    if not explicitBounded then
                        findings.Add(
                            hygieneFinding
                                findingType
                                scenarioId
                                doc.RelativePath
                                sourceSection
                                [| doc.RelativePath |]
                                (statusSections |> Array.map (fun section -> section.HeadingPath))
                                doc.Role
                                candidateDoc.RelativePath
                                candidateConfidence
                                candidateStatus
                                candidateDoc.RelativePath
                                evidence
                                finalAction
                                expectedHumanActionShape
                                confidence
                                risk
                                preserveNotes
                                whyFlagged)

            for doc in docs do
                let meaningfulSections =
                    doc.Sections
                    |> Array.filter (fun section -> section.Level > 0 && firstSnippet section.Content <> "")

                let nonStatusSections =
                    meaningfulSections
                    |> Array.filter (fun section -> statusSectionScore section < 3)

                if doc.Role = "mixed" then
                    for section in nonStatusSections do
                        let uniqueHits, genericHits = sectionSignals section
                        let maxSimilarity = sectionSimilarity section allChunks
                        let nearestCluster =
                            nearestSectionCluster docs section
                            |> Option.map fst
                            |> Option.defaultValue ""
                        let isUnique = uniqueHits.Length >= 2 || (uniqueHits.Length >= 1 && maxSimilarity < 0.18)
                        let isGeneric = genericHits.Length >= 2 || (genericHits.Length >= 1 && maxSimilarity >= 0.12)
                        let proposedAction, expectedHumanActionShape, preserveNotes, risk, whyFlagged =
                            if isUnique && not isGeneric then
                                "preserve",
                                "ignore",
                                "Protect the design/invariant section even inside a mixed-role document.",
                                "medium",
                                "Mixed-role docs need section-level preserve/reduce output rather than a whole-document judgment."
                            elif isGeneric && not isUnique then
                                (if maxSimilarity >= 0.25 then "replace_with_pointer" else "compact"),
                                (if maxSimilarity >= 0.25 then "link" else "reduce"),
                                "",
                                "medium",
                                "This section looks like reducible scaffolding inside a mixed-role document."
                            else
                                "needs_human_review",
                                "needs_human_review",
                                "Section has both preserve and reduce signals or is too ambiguous for a safe automatic call.",
                                "high",
                                "This section needs explicit human review so mixed-role documents do not silently lose important context."
                        let confidence =
                            clamp01 (0.45 + (float uniqueHits.Length / 10.0) + (float genericHits.Length / 10.0) + (maxSimilarity / 4.0))
                        let finalAction =
                            if proposedAction = "needs_human_review" then proposedAction
                            else determineAction proposedAction confidence risk
                        let evidence =
                            Array.concat [|
                                if uniqueHits.Length > 0 then [| sprintf "Unique markers: %s." (String.concat ", " uniqueHits) |] else [||]
                                if genericHits.Length > 0 then [| sprintf "Generic scaffolding markers: %s." (String.concat ", " genericHits) |] else [||]
                                [| sprintf "Max similarity to other sections: %.2f." maxSimilarity |]
                                [| sprintf "Section snippet: %s" (firstSnippet section.Content) |]
                                doc.RoleEvidence
                            |]
                        let hasStatusCompanion =
                            meaningfulSections
                            |> Array.exists (fun other -> other.Level > 0 && statusSectionScore other >= 3)
                        let hasCompanionMixedTask =
                            nonStatusSections
                            |> Array.exists (fun other ->
                                if other.HeadingPath = section.HeadingPath then
                                    false
                                else
                                    let otherUniqueHits, otherGenericHits = sectionSignals other
                                    let otherMaxSimilarity = sectionSimilarity other allChunks
                                    let otherIsUnique = otherUniqueHits.Length >= 2 || (otherUniqueHits.Length >= 1 && otherMaxSimilarity < 0.18)
                                    let otherIsGeneric = otherGenericHits.Length >= 2 || (otherGenericHits.Length >= 1 && otherMaxSimilarity >= 0.12)
                                    not (otherIsUnique && not otherIsGeneric))
                        let suppressMixedPreserve =
                            proposedAction = "preserve"
                            && expectedHumanActionShape = "ignore"
                            && (hasStatusCompanion || hasCompanionMixedTask)
                        if not suppressMixedPreserve then
                            findings.Add(
                                hygieneFinding
                                    "section_triage"
                                    "section-preserve-reduce"
                                    doc.RelativePath
                                    section.HeadingPath
                                    [| doc.RelativePath |]
                                    [| section.HeadingPath |]
                                    doc.Role
                                    ""
                                    0.0
                                    ""
                                    nearestCluster
                                    evidence
                                    finalAction
                                    expectedHumanActionShape
                                    confidence
                                    risk
                                    preserveNotes
                                    whyFlagged)
                elif doc.Role = "decision_index" then
                    for section in nonStatusSections do
                        let textLower = lower (section.Heading + "\n" + section.Content)
                        let uniqueHits, genericHits = sectionSignals section
                        let decisionHits = decisionSectionSignals section
                        let indexHits, contradictionHits, missingTargetHits, derivedHits, historicalHits = decisionIndexSectionSignals section
                        let brokenLinks = sectionBrokenLinks index doc.FilePath section
                        let maxSimilarity = sectionSimilarity section allChunks
                        let nearestCluster =
                            nearestSectionCluster docs section
                            |> Option.map fst
                            |> Option.defaultValue ""
                        let docHasStatusSections =
                            doc.Sections
                            |> Array.exists (fun other -> other.Level > 0 && statusSectionScore other >= 3)
                        let boundedDiscoverabilitySectionCue =
                            containsAny textLower [| "add this register to knowledge overview"; "link this register from the overview"; "add an entry link" |]
                        let preservesDecisionIndexMemory =
                            indexHits.Length >= 1
                            || decisionHits.Length >= 1
                            || derivedHits.Length >= 1
                            || historicalHits.Length >= 1
                            || (uniqueHits.Length >= 1 && genericHits.Length = 0)
                        let looksLikeResidue =
                            genericHits.Length >= 2
                            || (genericHits.Length >= 1 && maxSimilarity >= 0.12)
                            || containsAny textLower [| "current recommendation"; "implementation step"; "operator checklist"; "stale instruction" |]
                        let isHistoricalSnapshot =
                            historicalHits.Length > 0 && not (claimsCurrentAuthority textLower)
                        let hasMissingTarget = missingTargetHits.Length > 0 || brokenLinks.Length > 0
                        let proposedAction, expectedHumanActionShape, preserveNotes, risk, whyFlagged =
                            if contradictionHits.Length > 0 then
                                "needs_human_review",
                                "link",
                                "Reconcile the contradiction between the index metadata and its linked decision target before treating this register as trustworthy.",
                                "medium",
                                "Decision-index target contradictions need explicit review or correction guidance rather than silent protection."
                            elif hasMissingTarget && preservesDecisionIndexMemory then
                                "preserve",
                                "link",
                                "Preserve the decision index, but repair the missing artifact or broken target link so the register stays trustworthy and discoverable.",
                                "medium",
                                "This decision index still carries durable navigational value, but one or more decision targets are missing or broken."
                            elif derivedHits.Length > 0 then
                                "preserve",
                                "link",
                                "Keep the derived decision index, but point readers back to the canonical register so duplicated navigation stays bounded.",
                                "low",
                                "Derived decision indexes can stay reviewable when they clearly point back to the canonical register."
                            elif doc.Backlinks = 0 && boundedDiscoverabilitySectionCue then
                                "preserve",
                                "link",
                                "Preserve the decision index and add the explicit overview link named in the note.",
                                "low",
                                "This decision index keeps durable value and already names the bounded discoverability step needed to keep it reviewable."
                            elif isHistoricalSnapshot then
                                "preserve",
                                "accept",
                                "Preserve dated gate/wave provenance when the register is clearly historical rather than current authority.",
                                "low",
                                "Historical decision snapshots are durable provenance, not duplicate current-owner drift."
                            elif preservesDecisionIndexMemory && docHasStatusSections then
                                "preserve",
                                "link",
                                "Preserve the long-lived register entry, but keep readers pointed at the canonical current-status owner for live rollout detail.",
                                "low",
                                "Decision indexes can preserve durable navigation while still linking away copied live rollout detail."
                            elif preservesDecisionIndexMemory && not looksLikeResidue then
                                "preserve",
                                "accept",
                                "Protect durable decision navigation and register memory when the index is clearly pointing to long-lived decision targets.",
                                "low",
                                "Decision indexes should preserve navigational value unless a section becomes stale instruction or live-state drift."
                            elif looksLikeResidue && not preservesDecisionIndexMemory then
                                (if maxSimilarity >= 0.25 then "replace_with_pointer" else "compact"),
                                (if maxSimilarity >= 0.25 then "link" else "reduce"),
                                "",
                                "medium",
                                "This section looks like stale rollout or recommendation residue inside a decision index."
                            else
                                "needs_human_review",
                                "link",
                                "Decision-index sections that mix registry value with residue need explicit review before they are rewritten.",
                                "medium",
                                "This decision-index section mixes preserve and reduce signals, so the workflow should stay honest and route it to review."
                        let confidence =
                            clamp01 (
                                0.48
                                + (float indexHits.Length / 12.0)
                                + (float decisionHits.Length / 14.0)
                                + (float contradictionHits.Length / 10.0)
                                + (float missingTargetHits.Length / 12.0)
                                + (float derivedHits.Length / 12.0)
                                + (float historicalHits.Length / 12.0)
                                + (float brokenLinks.Length / 10.0)
                                + (float uniqueHits.Length / 18.0)
                                + (float genericHits.Length / 18.0)
                                + (maxSimilarity / 4.0))
                        let finalAction =
                            if proposedAction = "needs_human_review" then proposedAction
                            elif boundedDiscoverabilitySectionCue && proposedAction = "preserve" && expectedHumanActionShape = "link" then "preserve"
                            else determineAction proposedAction confidence risk
                        let evidence =
                            Array.concat [|
                                if indexHits.Length > 0 then [| sprintf "Decision-index markers: %s." (String.concat ", " indexHits) |] else [||]
                                if decisionHits.Length > 0 then [| sprintf "Decision-memory markers: %s." (String.concat ", " decisionHits) |] else [||]
                                if contradictionHits.Length > 0 then [| sprintf "Contradiction markers: %s." (String.concat ", " contradictionHits) |] else [||]
                                if missingTargetHits.Length > 0 then [| sprintf "Missing-target markers: %s." (String.concat ", " missingTargetHits) |] else [||]
                                if derivedHits.Length > 0 then [| sprintf "Derived-index markers: %s." (String.concat ", " derivedHits) |] else [||]
                                if historicalHits.Length > 0 then [| sprintf "Historical markers: %s." (String.concat ", " historicalHits) |] else [||]
                                if brokenLinks.Length > 0 then
                                    brokenLinks
                                    |> Array.map (fun link ->
                                        sprintf "Broken outgoing link: %s." (if String.IsNullOrWhiteSpace(link.TargetPath) then "(empty target)" else link.TargetPath))
                                else [||]
                                if uniqueHits.Length > 0 then [| sprintf "Unique markers: %s." (String.concat ", " uniqueHits) |] else [||]
                                if genericHits.Length > 0 then [| sprintf "Generic scaffolding markers: %s." (String.concat ", " genericHits) |] else [||]
                                [| sprintf "Max similarity to other sections: %.2f." maxSimilarity |]
                                [| sprintf "Section snippet: %s" (firstSnippet section.Content) |]
                                doc.RoleEvidence
                            |]
                        let suppressPureDecisionIndexPreserve =
                            proposedAction = "preserve"
                            && expectedHumanActionShape = "accept"
                            && not isHistoricalSnapshot
                        if not suppressPureDecisionIndexPreserve then
                            findings.Add(
                                hygieneFinding
                                    "section_triage"
                                    (if contradictionHits.Length > 0 then "decision-index-contradiction"
                                     elif hasMissingTarget then "decision-index-target-missing"
                                     elif derivedHits.Length > 0 then "decision-index-derived"
                                     elif isHistoricalSnapshot then "decision-index-historical-snapshot"
                                     else "decision-index-section-preserve-reduce")
                                    doc.RelativePath
                                    section.HeadingPath
                                    [| doc.RelativePath |]
                                    [| section.HeadingPath |]
                                    doc.Role
                                    ""
                                    0.0
                                    ""
                                    nearestCluster
                                    evidence
                                    finalAction
                                    expectedHumanActionShape
                                    confidence
                                    risk
                                    preserveNotes
                                    whyFlagged)
                elif doc.Role = "research_note" then
                    for section in nonStatusSections do
                        let textLower = lower (section.Heading + "\n" + section.Content)
                        let uniqueHits, genericHits = sectionSignals section
                        let researchHits, conflictHits, evidenceGapHits = researchSectionSignals section
                        let maxSimilarity = sectionSimilarity section allChunks
                        let nearestCluster =
                            nearestSectionCluster docs section
                            |> Option.map fst
                            |> Option.defaultValue ""
                        let preservesResearchMemory =
                            researchHits.Length >= 1 || (uniqueHits.Length >= 1 && genericHits.Length = 0)
                        let looksLikeResidue =
                            genericHits.Length >= 2
                            || (genericHits.Length >= 1 && maxSimilarity >= 0.12)
                            || containsAny textLower [| "setup"; "migration"; "operator sign-off"; "implementation residue"; "stale setup"; "rollout checklist" |]
                        let proposedAction, expectedHumanActionShape, preserveNotes, risk, whyFlagged =
                            if conflictHits.Length > 0 then
                                "needs_human_review",
                                "link",
                                "Keep both research notes available and add comparison links rather than guessing a winner.",
                                "medium",
                                "Conflicting research notes need explicit human review so durable memory is preserved without inventing a false resolution."
                            elif evidenceGapHits.Length > 0 && preservesResearchMemory then
                                "preserve",
                                "link",
                                "Preserve the research note, but restore the missing artifact/evidence link before treating it as settled.",
                                "medium",
                                "This research note carries durable learning, but its supporting artifact/evidence trail is incomplete."
                            elif preservesResearchMemory && not looksLikeResidue then
                                "preserve",
                                "link",
                                "Preserve durable research memory and keep it discoverable from a stable nearby home.",
                                "low",
                                "Long-lived research findings, negative results, and open-problem notes should stay reviewable rather than being compacted away."
                            elif looksLikeResidue && not preservesResearchMemory then
                                (if maxSimilarity >= 0.25 then "replace_with_pointer" else "compact"),
                                (if maxSimilarity >= 0.25 then "link" else "reduce"),
                                "",
                                "medium",
                                "This section looks like setup or implementation residue inside a research note."
                            else
                                "needs_human_review",
                                "link",
                                "Research notes that mix durable findings with setup residue need explicit review before they are rewritten.",
                                "medium",
                                "This research-note section mixes preserve and reduce signals, so the workflow should stay honest and route it to review."
                        let confidence =
                            clamp01 (
                                0.50
                                + (float researchHits.Length / 12.0)
                                + (float conflictHits.Length / 10.0)
                                + (float evidenceGapHits.Length / 10.0)
                                + (float uniqueHits.Length / 14.0)
                                + (float genericHits.Length / 16.0)
                                + (maxSimilarity / 4.0))
                        let finalAction =
                            if proposedAction = "needs_human_review" then proposedAction
                            else determineAction proposedAction confidence risk
                        let evidence =
                            Array.concat [|
                                if researchHits.Length > 0 then [| sprintf "Research-memory markers: %s." (String.concat ", " researchHits) |] else [||]
                                if conflictHits.Length > 0 then [| sprintf "Conflict markers: %s." (String.concat ", " conflictHits) |] else [||]
                                if evidenceGapHits.Length > 0 then [| sprintf "Evidence-gap markers: %s." (String.concat ", " evidenceGapHits) |] else [||]
                                if uniqueHits.Length > 0 then [| sprintf "Unique markers: %s." (String.concat ", " uniqueHits) |] else [||]
                                if genericHits.Length > 0 then [| sprintf "Generic scaffolding markers: %s." (String.concat ", " genericHits) |] else [||]
                                [| sprintf "Max similarity to other sections: %.2f." maxSimilarity |]
                                [| sprintf "Section snippet: %s" (firstSnippet section.Content) |]
                                doc.RoleEvidence
                            |]
                        findings.Add(
                            hygieneFinding
                                "section_triage"
                                (if conflictHits.Length > 0 then "research-note-conflict"
                                 elif evidenceGapHits.Length > 0 then "research-note-evidence-gap"
                                 else "research-note-section-preserve-reduce")
                                doc.RelativePath
                                section.HeadingPath
                                [| doc.RelativePath |]
                                [| section.HeadingPath |]
                                doc.Role
                                ""
                                0.0
                                ""
                                nearestCluster
                                evidence
                                finalAction
                                expectedHumanActionShape
                                confidence
                                risk
                                preserveNotes
                                whyFlagged)
                elif doc.Role = "review_or_decision_record" then
                    for section in nonStatusSections do
                        let uniqueHits, genericHits = sectionSignals section
                        let decisionHits = decisionSectionSignals section
                        let maxSimilarity = sectionSimilarity section allChunks
                        let nearestCluster =
                            nearestSectionCluster docs section
                            |> Option.map fst
                            |> Option.defaultValue ""
                        let preservesDecisionMemory = decisionHits.Length >= 1 || (uniqueHits.Length >= 1 && genericHits.Length = 0)
                        let isGeneric = genericHits.Length >= 2 || (genericHits.Length >= 1 && maxSimilarity >= 0.12)
                        let proposedAction, expectedHumanActionShape, preserveNotes, risk, whyFlagged =
                            if preservesDecisionMemory && not isGeneric then
                                "preserve",
                                "accept",
                                "Protect durable decision memory and rationale inside long-lived decision records.",
                                "low",
                                "Decision records should preserve durable rationale unless a section is clearly stale implementation residue."
                            elif isGeneric && not preservesDecisionMemory then
                                (if maxSimilarity >= 0.25 then "replace_with_pointer" else "compact"),
                                (if maxSimilarity >= 0.25 then "link" else "reduce"),
                                "",
                                "medium",
                                "This section looks like stale implementation or process residue inside a long-lived decision record."
                            else
                                "needs_human_review",
                                "needs_human_review",
                                "Section mixes durable decision memory with stale implementation/process details.",
                                "medium",
                                "Decision-record sections that mix rationale and residue need explicit human review so long-lived value is not dropped."
                        let confidence =
                            clamp01 (0.45 + (float decisionHits.Length / 12.0) + (float uniqueHits.Length / 12.0) + (float genericHits.Length / 12.0) + (maxSimilarity / 4.0))
                        let finalAction =
                            if proposedAction = "needs_human_review" then proposedAction
                            else determineAction proposedAction confidence risk
                        let evidence =
                            Array.concat [|
                                if decisionHits.Length > 0 then [| sprintf "Decision-memory markers: %s." (String.concat ", " decisionHits) |] else [||]
                                if uniqueHits.Length > 0 then [| sprintf "Unique markers: %s." (String.concat ", " uniqueHits) |] else [||]
                                if genericHits.Length > 0 then [| sprintf "Generic scaffolding markers: %s." (String.concat ", " genericHits) |] else [||]
                                [| sprintf "Max similarity to other sections: %.2f." maxSimilarity |]
                                [| sprintf "Section snippet: %s" (firstSnippet section.Content) |]
                                doc.RoleEvidence
                            |]
                        findings.Add(
                            hygieneFinding
                                "section_triage"
                                "decision-record-section-preserve-reduce"
                                doc.RelativePath
                                section.HeadingPath
                                [| doc.RelativePath |]
                                [| section.HeadingPath |]
                                doc.Role
                                ""
                                0.0
                                ""
                                nearestCluster
                                evidence
                                finalAction
                                expectedHumanActionShape
                                confidence
                                risk
                                preserveNotes
                                whyFlagged)
                elif doc.Role = "research_deep_dive" then
                    let preserveSections =
                        nonStatusSections
                        |> Array.filter (fun section ->
                            let uniqueHits, genericHits = sectionSignals section
                            let maxSimilarity = sectionSimilarity section allChunks
                            (uniqueHits.Length >= 1 && genericHits.Length <= 1 && maxSimilarity < 0.25)
                            || containsAny section.Heading [| "core idea"; "invariants"; "why" |])
                    let hasStatusSections =
                        meaningfulSections
                        |> Array.exists (fun section -> section.Level > 0 && statusSectionScore section >= 3)
                    let suppressDeepDivePreserve =
                        preserveSections.Length > 0
                        && (doc.Backlinks = 0 || hasStatusSections)
                    if preserveSections.Length > 0 && not suppressDeepDivePreserve then
                        let nearestCluster =
                            nearestSectionCluster docs preserveSections.[0]
                            |> Option.map fst
                            |> Option.defaultValue ""
                        let evidence =
                            Array.append
                                [| sprintf "Research deep-dive role with %d preserve-worthy section(s)." preserveSections.Length |]
                                doc.RoleEvidence
                        findings.Add(
                            hygieneFinding
                                "section_triage"
                                "neon-deep-dive-protection"
                                doc.RelativePath
                                preserveSections.[0].HeadingPath
                                [| doc.RelativePath |]
                                (preserveSections |> Array.map (fun section -> section.HeadingPath))
                                doc.Role
                                ""
                                0.0
                                ""
                                nearestCluster
                                evidence
                                "preserve"
                                "ignore"
                                0.88
                                "low"
                                "High-novelty deep-dive content should be preserved in the first report slice."
                                "Deep-dive protection is part of the first-cut acceptance gate, not a later optimization.")

            let orphanDocs =
                docs
                |> Array.filter (fun doc -> doc.Backlinks = 0)

            for doc in orphanDocs do
                let nearestCluster, nearestSimilarity =
                    nearestDocCluster docs doc
                    |> Option.defaultValue ("", 0.0)
                let outlinkCount =
                    doc.Sections
                    |> Array.sumBy (fun section -> section.OutLinks.Length)
                let artifactReferenceCount = decisionArtifactReferenceCount doc
                let brokenLinkCount = docBrokenLinks index doc.FilePath |> Array.length
                let looksLikeIndexDoc =
                    doc.Role = "entrypoint_or_index_doc"
                    || containsAny doc.RelativePath [| "overview"; "index"; "readme" |]
                    || containsAny doc.Title [| "overview"; "index"; "readme" |]
                    || (doc.RoleEvidence |> Array.exists (fun evidence -> evidence.Contains("entrypoint or index")))
                let docTextLower =
                    doc.Sections
                    |> Array.collect (fun section -> [| section.Heading; section.Content |])
                    |> String.concat "\n"
                    |> lower
                let hasResearchEvidenceGap =
                    containsAny docTextLower [| "artifact missing"; "missing artifact"; "evidence gap"; "evidence link missing"; "restore evidence" |]
                let hasDecisionIndexEvidenceGap =
                    brokenLinkCount > 0
                    || containsAny docTextLower [| "artifact id unknown"; "artifact id missing"; "unknown artifact id"; "missing artifact id"; "broken link"; "target moved"; "repair the target"; "repair the link" |]
                let hasBoundedDiscoverabilityCue =
                    containsAny docTextLower [| "add this register to knowledge overview"; "link this register from the overview"; "add an entry link"; "repair the target"; "repair the link"; "restore the target" |]
                let suggestedAction, expectedHumanActionShape, confidence, risk, preserveNotes, whyFlagged =
                    if doc.Role = "decision_index" && hasDecisionIndexEvidenceGap then
                        "preserve",
                        "link",
                        0.84,
                        "medium",
                        "Preserve the decision index, but repair the missing artifact or broken target link before treating the register as trustworthy.",
                        "This decision index has no incoming links and one or more targets are missing or broken, so the registry should stay preserved while the target path is repaired."
                    elif doc.Role = "decision_index" && (outlinkCount >= 2 || artifactReferenceCount >= 1) then
                        "preserve",
                        "ignore",
                        0.86,
                        "low",
                        "Decision indexes with clear registry links can remain lightly linked by design.",
                        "This decision index has no incoming links, but its outgoing targets or artifact references show clear long-lived registry value."
                    elif doc.Role = "decision_index" && hasBoundedDiscoverabilityCue then
                        "preserve",
                        "link",
                        0.8,
                        "medium",
                        "Preserve the decision index and add the bounded entry link or correction step named in the note.",
                        "This decision index has no incoming links, but the next discoverability step is explicit enough to keep the output action-worthy."
                    elif looksLikeIndexDoc && outlinkCount >= 3 then
                        "preserve",
                        "ignore",
                        0.92,
                        "low",
                        "Entrypoint/root docs can legitimately have no backlinks.",
                        "This doc has no incoming links, but its role signals match an intentional root or index."
                    elif doc.Role = "historical_archive" then
                        "preserve",
                        "ignore",
                        0.82,
                        "low",
                        "Historical/archive docs may remain lightly linked by design.",
                        "This doc has no incoming links, but archive/history role signals suggest it is intentionally retained."
                    elif doc.Role = "research_deep_dive" then
                        "preserve",
                        "link",
                        0.86,
                        "low",
                        "Protect intentional deep-dive knowledge; if discoverability feels low, add an entry link rather than reducing content.",
                        "This doc has no incoming links, but its role signals indicate intentional deep-dive knowledge that should be preserved."
                    elif doc.Role = "review_or_decision_record" && (artifactReferenceCount >= 2 || outlinkCount >= 2) then
                        "preserve",
                        "ignore",
                        0.85,
                        "low",
                        "Long-lived decision records with clear artifact or cross-reference value should not trigger orphan panic by default.",
                        "This decision record has no incoming links, but its artifact references or outgoing links show clear long-lived decision value."
                    elif doc.Role = "review_or_decision_record" then
                        "preserve",
                        "link",
                        0.8,
                        "medium",
                        "Preserve long-lived decision memory; if discoverability feels weak, add an entry link rather than reducing the record.",
                        "This decision record has no incoming links, but it still carries durable value; review discoverability before treating it as stray content."
                    elif doc.Role = "research_note" && hasResearchEvidenceGap then
                        "preserve",
                        "link",
                        0.82,
                        "medium",
                        "Preserve research intent, but restore the missing evidence path so the note stays trustworthy and discoverable.",
                        "This research note has no incoming links and its evidence trail is incomplete, so the note should stay preserved while the missing link is restored."
                    elif doc.Role = "research_note" then
                        "preserve",
                        "link",
                        0.8,
                        "medium",
                        "Preserve long-lived research memory; if discoverability feels weak, add an entry link rather than reducing the note.",
                        "This research note has no incoming links, but it still carries durable findings or open questions that should remain reviewable."
                    elif outlinkCount = 0 && nearestCluster = "" then
                        "needs_human_review",
                        "link",
                        0.74,
                        "medium",
                        "Check whether this doc needs an entry link or should remain isolated by intent.",
                        "This doc has no incoming links and no nearby owner/cluster, so discoverability may be weak."
                    else
                        "needs_human_review",
                        "link",
                        clamp01 (0.62 + (nearestSimilarity / 3.0)),
                        "medium",
                        "Review whether this doc needs a clearer inbound link from its nearest related cluster.",
                        "This doc has no incoming links, but it does have related material nearby; linkage may simply be weak."
                let evidence =
                    Array.concat [|
                        [| "Incoming link count: 0." |]
                        [| sprintf "Outgoing link count: %d." outlinkCount |]
                        if artifactReferenceCount > 0 then [| sprintf "Artifact reference count: %d." artifactReferenceCount |] else [||]
                        if brokenLinkCount > 0 then [| sprintf "Broken outgoing link count: %d." brokenLinkCount |] else [||]
                        if nearestCluster <> "" then [| sprintf "Nearest related cluster: %s (similarity %.2f)." nearestCluster nearestSimilarity |] else [| "No nearby owner/cluster was found." |]
                        doc.RoleEvidence
                    |]
                findings.Add(
                    hygieneFinding
                        "orphan_triage"
                        "orphan-actionability"
                        doc.RelativePath
                        (primarySectionPath doc)
                        [| doc.RelativePath |]
                        [| primarySectionPath doc |]
                        doc.Role
                        ""
                        0.0
                        ""
                        nearestCluster
                        evidence
                        suggestedAction
                        expectedHumanActionShape
                        confidence
                        risk
                        preserveNotes
                        whyFlagged)

            let fileContents =
                allChunks
                |> Array.groupBy (fun chunk -> normPath chunk.FilePath)
                |> Array.map (fun (_, sections) ->
                    let doc = docs |> Array.find (fun item -> normPath item.FilePath = normPath sections.[0].FilePath)
                    doc, (sections |> Array.map (fun section -> section.Content) |> String.concat "\n"))

            let entityToDocs = Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
            for (doc, content) in fileContents do
                let entities =
                    extractEntityRefs content
                    |> Array.map normalizeEntity
                    |> Array.filter (fun entity -> entity.Length >= 3)
                    |> Array.distinct
                for entity in entities do
                    if not (entityToDocs.ContainsKey(entity)) then
                        entityToDocs.[entity] <- HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    entityToDocs.[entity].Add(doc.RelativePath) |> ignore

            let indexedFileNames =
                fileContents
                |> Array.map (fun (doc, _) -> Path.GetFileNameWithoutExtension(doc.RelativePath).ToLowerInvariant())
                |> Set.ofArray

            let entityImportance (entity: string) =
                let lengthScore = min 5 (entity.Length / 3)
                let fileBonus = if indexedFileNames.Contains(entity) then 3 else 0
                let structureBonus =
                    if entity.Contains(".") then 2
                    elif entity.Length > 12 then 1
                    else 0
                lengthScore + fileBonus + structureBonus

            let gapCandidates =
                entityToDocs
                |> Seq.map (fun kv ->
                    let sources = kv.Value |> Seq.sort |> Seq.toArray
                    let count = sources.Length
                    let signal =
                        if count >= max 5 (int (ceil (float fileContents.Length * 0.05))) then "god-node"
                        elif count >= 2 then "shared"
                        else "isolated"
                    kv.Key, sources, count, signal, entityImportance kv.Key)
                |> Seq.filter (fun (_, _, _, signal, importance) -> (signal = "isolated" || signal = "god-node") && importance >= 4)
                |> Seq.sortByDescending (fun (_, _, count, _, importance) -> count, importance)
                |> Seq.truncate 5
                |> Seq.toArray

            for (entity, sources, count, signal, importance) in gapCandidates do
                let sourceFile = sources.[0]
                let sourceDoc = docs |> Array.find (fun doc -> doc.RelativePath = sourceFile)
                let sourceSection =
                    sourceDoc.Sections
                    |> Array.tryFind (sectionMentionsEntity entity)
                    |> Option.orElseWith (fun () -> sourceDoc.Sections |> Array.tryFind (fun section -> section.Level > 0))
                    |> Option.map (fun section -> section.HeadingPath)
                    |> Option.defaultValue sourceDoc.Title
                let nearestOwnerOrCluster =
                    if count > 1 then String.concat ", " sources
                    else
                        nearestDocCluster docs sourceDoc
                        |> Option.map fst
                        |> Option.defaultValue ""
                let suggestedAction, expectedHumanActionShape, confidence, risk, preserveNotes, whyFlagged =
                    if signal = "god-node" then
                        "needs_human_review",
                        "reduce",
                        0.79,
                        "medium",
                        "Review whether this entity needs a clearer canonical explanation instead of repeated scattered mentions.",
                        "This entity appears across many docs, which can indicate over-centralized ownership or a missing canonical explanation."
                    else
                        "needs_human_review",
                        "link",
                        0.76,
                        "medium",
                        "Review whether this entity needs an additional link or a clearer home in adjacent docs.",
                        "This entity appears in only one doc, which can indicate weak coverage or low discoverability."
                let evidence =
                    [|
                        sprintf "Signal `%s`: entity appears in %d doc(s)." signal count
                        sprintf "Entity importance score: %d." importance
                        sprintf "Sources: %s." (String.concat ", " sources)
                        if nearestOwnerOrCluster <> "" then sprintf "Nearest owner/cluster considered: %s." nearestOwnerOrCluster else "No nearby owner/cluster was found."
                    |]
                findings.Add(
                    hygieneFinding
                        "gap_triage"
                        "gap-explainability"
                        sourceFile
                        sourceSection
                        sources
                        [| sourceSection |]
                        sourceDoc.Role
                        ""
                        0.0
                        ""
                        nearestOwnerOrCluster
                        evidence
                        suggestedAction
                        expectedHumanActionShape
                        confidence
                        risk
                        preserveNotes
                        whyFlagged)

            if findings.Count = 0 then
                [| mdict [ "note", box "No hygiene findings detected for the current index."; "docs", box docs.Length ] |]
            else
                let ordered =
                    findings
                    |> Seq.sortByDescending (fun finding ->
                        match finding.["confidence"] with
                        | :? float as confidence -> confidence
                        | _ -> 0.0)
                    |> Seq.toArray

                ordered

    let hygiene (index: DocIndex) (chunks: DocChunk[] option) (repoRoot: string) (profile: string) (limit: int) =
        match chunks with
        | None -> [| mdict [ "error", box "source chunks not loaded — run 'knowledge-sight index' first" ] |]
        | Some allChunks when isCompactionProfile profile -> hygieneCompaction index allChunks repoRoot limit
        | Some _ -> hygieneBroad index chunks repoRoot profile limit

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

    /// explain(refId) — debug primitive showing index metadata, findSource diagnosis, and indexed frontmatter.
    let explain (index: DocIndex) (session: QuerySession) (chunks: DocChunk[] option) (refId: string) =
        match session.GetRef(refId) with
        | None -> mdict [ "error", box (sprintf "ref %s not found in session" refId) ]
        | Some idx when idx < 0 || idx >= index.Chunks.Length ->
            mdict [ "error", box (sprintf "ref %s points to chunk %d but index has %d chunks" refId idx index.Chunks.Length) ]
        | Some idx ->
            let c = index.Chunks.[idx]
            let cid = IndexStore.chunkId c.FilePath c.Heading c.StartLine
            let fm = frontmatterForFile index c.FilePath
            let frontmatterSource, frontmatter = indexedFrontmatterPayload fm
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
                "summary", box c.Summary; "sourceMatch", box sourceMatch
                "frontmatterSource", box frontmatterSource
                "frontmatter", box frontmatter ]
