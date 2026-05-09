namespace AITeam.KnowledgeSight

open System
open System.IO
open System.Numerics.Tensors

/// Index persistence and query operations.
module IndexStore =

    /// Deterministic chunk identifier from key fields. Survives reindex as long as the chunk's position is stable.
    let chunkId (filePath: string) (heading: string) (startLine: int) =
        let input = sprintf "%s|%s|%d" (filePath.Replace('\\', '/')) heading startLine
        let bytes = System.Text.Encoding.UTF8.GetBytes(input)
        let hash = System.Security.Cryptography.SHA256.HashData(bytes)
        let hex = BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant()
        hex.Substring(0, 12)

    let private frontmatterJsonPath (dir: string) =
        Path.Combine(dir, "frontmatters.jsonl")

    let private frontmatterTsvPath (dir: string) =
        Path.Combine(dir, "frontmatters.tsv")

    let private saveFrontmatters (dir: string) (frontmatters: Map<string, Frontmatter>) =
        let esc (s: string) =
            s.Replace("\t", " ").Replace("\n", " ").Replace("\r", "")

        let jsonLines =
            frontmatters
            |> Map.toArray
            |> Array.map (fun (filePath, frontmatter) ->
                let fields =
                    frontmatterFields frontmatter
                    |> Map.toArray
                    |> Array.map (fun (key, value) ->
                        match value with
                        | Scalar scalar ->
                            {| key = key; kind = "scalar"; value = scalar; values = [||] |}
                        | StringList values ->
                            {| key = key; kind = "list"; value = ""; values = values |})

                System.Text.Json.JsonSerializer.Serialize({| filePath = filePath; fields = fields |}))

        File.WriteAllLines(frontmatterJsonPath dir, jsonLines)

        let legacyLines =
            frontmatters
            |> Map.toArray
            |> Array.map (fun (file, fm) ->
                sprintf "%s\t%s\t%s\t%s\t%s\t%s"
                    (esc file) (esc fm.Id) (esc fm.Title) (esc fm.Status)
                    (fm.Tags |> String.concat ",") (fm.Related |> String.concat ","))

        File.WriteAllLines(frontmatterTsvPath dir, legacyLines)

    let private loadFrontmattersJson (path: string) =
        try
            let rows =
                File.ReadAllLines(path)
                |> Array.map (fun line ->
                    use doc = System.Text.Json.JsonDocument.Parse(line)
                    let root = doc.RootElement

                    let filePath =
                        match root.TryGetProperty("filePath") with
                        | true, value -> value.GetString()
                        | _ -> ""

                    let fields =
                        match root.TryGetProperty("fields") with
                        | true, value when value.ValueKind = System.Text.Json.JsonValueKind.Array ->
                            value.EnumerateArray()
                            |> Seq.choose (fun item ->
                                let key =
                                    match item.TryGetProperty("key") with
                                    | true, keyValue -> keyValue.GetString()
                                    | _ -> ""

                                if String.IsNullOrWhiteSpace(key) then None
                                else
                                    let kind =
                                        match item.TryGetProperty("kind") with
                                        | true, kindValue -> kindValue.GetString()
                                        | _ -> "scalar"

                                    let parsedValue =
                                        if String.Equals(kind, "list", StringComparison.OrdinalIgnoreCase) then
                                            let values =
                                                match item.TryGetProperty("values") with
                                                | true, valuesEl when valuesEl.ValueKind = System.Text.Json.JsonValueKind.Array ->
                                                    valuesEl.EnumerateArray()
                                                    |> Seq.map (fun v -> v.ToString())
                                                    |> Seq.filter (String.IsNullOrWhiteSpace >> not)
                                                    |> Seq.toArray
                                                | _ -> [||]
                                            StringList values
                                        else
                                            let scalar =
                                                match item.TryGetProperty("value") with
                                                | true, scalarEl -> scalarEl.ToString()
                                                | _ -> ""
                                            Scalar scalar

                                    Some (key, parsedValue))
                            |> Map.ofSeq
                        | _ -> Map.empty

                    if String.IsNullOrWhiteSpace(filePath) then
                        invalidOp "frontmatters.jsonl row missing filePath"

                    filePath, frontmatterFromFields fields)

            Ok (rows |> Map.ofArray)
        with ex ->
            Error ex.Message

    let private loadFrontmattersLegacy (path: string) =
        File.ReadAllLines(path)
        |> Array.choose (fun line ->
            let p = line.Split('\t')
            if p.Length >= 6 then
                Some (p.[0], { Id = p.[1]; Title = p.[2]; Status = p.[3]
                               Tags = p.[4].Split(',') |> Array.filter ((<>) "")
                               Related = p.[5].Split(',') |> Array.filter ((<>) "")
                               Extra = Map.empty })
            else None)
        |> Map.ofArray

    // ── Source chunk cache ──

    let saveSourceChunks (dir: string) (chunks: DocChunk[]) =
        let path = Path.Combine(dir, "source-chunks.jsonl")
        use writer = new StreamWriter(path)
        for c in chunks do
            let cid = chunkId c.FilePath c.Heading c.StartLine
            let json = System.Text.Json.JsonSerializer.Serialize({|
                cid = cid; filePath = c.FilePath; heading = c.Heading; headingPath = c.HeadingPath
                level = c.Level; startLine = c.StartLine; endLine = c.EndLine
                content = c.Content; summary = c.Summary
                tags = c.Tags; outLinks = c.OutLinks |})
            writer.WriteLine(json)
        CliOutput.info "  Cached %d source chunks → source-chunks.jsonl" chunks.Length

    let loadSourceChunks (dir: string) : DocChunk[] option =
        let path = Path.Combine(dir, "source-chunks.jsonl")
        if not (File.Exists path) then None
        else
            try
                let chunks =
                    File.ReadAllLines(path)
                    |> Array.choose (fun line ->
                        try
                            use doc = System.Text.Json.JsonDocument.Parse(line)
                            let r = doc.RootElement
                            let str (p: string) = match r.TryGetProperty(p) with true, v -> v.GetString() | _ -> ""
                            let int' (p: string) = match r.TryGetProperty(p) with true, v -> v.GetInt32() | _ -> 0
                            let strArr (p: string) =
                                match r.TryGetProperty(p) with
                                | true, v when v.ValueKind = System.Text.Json.JsonValueKind.Array ->
                                    v.EnumerateArray() |> Seq.map (fun x -> x.GetString()) |> Seq.toArray
                                | _ -> [||]
                            Some { FilePath = str "filePath"; Heading = str "heading"; HeadingPath = str "headingPath"
                                   Level = int' "level"; StartLine = int' "startLine"; EndLine = int' "endLine"
                                   Content = str "content"; Summary = str "summary"
                                   Tags = strArr "tags"; OutLinks = strArr "outLinks" }
                        with _ -> None)
                Some chunks
            with _ -> None

    // ── Persistence ──

    let private escape (s: string) =
        s.Replace("\t", " ").Replace("\n", " ").Replace("\r", "")

    let private writeEmbeddings (path: string) (embeddings: float32[][]) =
        use fs = File.Create(path)
        use bw = new BinaryWriter(fs)
        bw.Write(embeddings.Length)
        if embeddings.Length > 0 then
            bw.Write(embeddings.[0].Length)
            for emb in embeddings do
                for v in emb do bw.Write(v)

    let private readEmbeddings (path: string) =
        if not (File.Exists path) then None
        else
            use fs = File.OpenRead(path)
            use br = new BinaryReader(fs)
            let count = br.ReadInt32()
            if count = 0 then Some [||]
            else
                let dim = br.ReadInt32()
                Some (Array.init count (fun _ -> Array.init dim (fun _ -> br.ReadSingle())))

    let save (dir: string) (index: DocIndex) =
        Directory.CreateDirectory(dir) |> ignore

        let header = "#fields:FilePath\tHeading\tHeadingPath\tLevel\tStartLine\tEndLine\tSummary\tTags\tLinkCount\tWordCount"
        let chunkLines =
            index.Chunks |> Array.map (fun c ->
                sprintf "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%d\t%d"
                    (escape c.FilePath) (escape c.Heading) (escape c.HeadingPath)
                    c.Level c.StartLine c.EndLine (escape c.Summary)
                    (escape c.Tags) c.LinkCount c.WordCount)
        File.WriteAllLines(Path.Combine(dir, "chunks.tsv"), Array.append [| header |] chunkLines)

        writeEmbeddings (Path.Combine(dir, "embeddings.emb")) index.Embeddings

        let linkLines =
            index.Links |> Array.map (fun l ->
                sprintf "%s\t%s\t%s\t%s\t%s\t%d"
                    (escape l.SourceFile) (escape l.SourceHeading)
                    (escape l.TargetPath) (escape l.TargetResolved)
                    (escape l.LinkText) l.Line)
        File.WriteAllLines(Path.Combine(dir, "links.tsv"), linkLines)

        // Save frontmatters
        saveFrontmatters dir index.Frontmatters

        CliOutput.info "  Index saved: %d chunks, %d links, %d docs with frontmatter → %s"
            index.Chunks.Length index.Links.Length index.Frontmatters.Count dir

    let load (dir: string) : DocIndex option =
        let chunkFile = Path.Combine(dir, "chunks.tsv")
        let embFile = Path.Combine(dir, "embeddings.emb")
        if not (File.Exists chunkFile) || not (File.Exists embFile) then None
        else
            let allLines = File.ReadAllLines(chunkFile)
            let dataLines =
                if allLines.Length > 0 && allLines.[0].StartsWith("#fields:") then allLines.[1..]
                else allLines
            let chunks =
                dataLines |> Array.choose (fun line ->
                    let p = line.Split('\t')
                    if p.Length >= 10 then
                        Some { FilePath = p.[0]; Heading = p.[1]; HeadingPath = p.[2]
                               Level = int p.[3]; StartLine = int p.[4]; EndLine = int p.[5]
                               Summary = p.[6]; Tags = p.[7]; LinkCount = int p.[8]; WordCount = int p.[9] }
                    else None)
            let embeddings = readEmbeddings embFile |> Option.defaultValue [||]
            let links =
                let f = Path.Combine(dir, "links.tsv")
                if File.Exists f then
                    File.ReadAllLines(f) |> Array.choose (fun line ->
                        let p = line.Split('\t')
                        if p.Length >= 6 then
                            Some { SourceFile = p.[0]; SourceHeading = p.[1]
                                   TargetPath = p.[2]; TargetResolved = p.[3]
                                   LinkText = p.[4]; Line = int p.[5] }
                        else None)
                else [||]
            let frontmatters =
                let jsonPath = frontmatterJsonPath dir
                let legacyPath = frontmatterTsvPath dir
                if File.Exists jsonPath then
                    match loadFrontmattersJson jsonPath with
                    | Ok frontmatters when File.Exists legacyPath ->
                        let legacy = loadFrontmattersLegacy legacyPath
                        let missingLegacyKeys =
                            legacy
                            |> Map.toSeq
                            |> Seq.map fst
                            |> Seq.filter (fun key -> not (Map.containsKey key frontmatters))
                            |> Seq.toArray

                        if missingLegacyKeys.Length > 0 then
                            eprintfn "  Warning: frontmatters.jsonl incomplete (%d missing rows); filling gaps from frontmatters.tsv" missingLegacyKeys.Length
                            legacy
                            |> Map.fold (fun acc key value ->
                                if Map.containsKey key acc then acc
                                else acc.Add(key, value)) frontmatters
                        else
                            frontmatters
                    | Ok frontmatters -> frontmatters
                    | Error error when File.Exists legacyPath ->
                        eprintfn "  Warning: frontmatters.jsonl unreadable (%s); falling back to frontmatters.tsv" error
                        loadFrontmattersLegacy legacyPath
                    | Error _ -> Map.empty
                elif File.Exists legacyPath then
                    loadFrontmattersLegacy legacyPath
                else Map.empty
            let dim = if embeddings.Length > 0 then embeddings.[0].Length else 0
            Some { Chunks = chunks; Embeddings = embeddings; Links = links
                   Frontmatters = frontmatters; EmbeddingDim = dim }

    // ── Query functions ──

    let search (index: DocIndex) (queryEmbedding: float32[]) (k: int) =
        if index.Embeddings.Length = 0 || queryEmbedding.Length = 0 then [||]
        else
            index.Embeddings
            |> Array.mapi (fun i emb ->
                if emb.Length = 0 || emb.Length <> queryEmbedding.Length then i, -1f
                else i, TensorPrimitives.CosineSimilarity(ReadOnlySpan(queryEmbedding), ReadOnlySpan(emb)))
            |> Array.sortByDescending snd
            |> Array.take (min k index.Embeddings.Length)

    let similar (index: DocIndex) (chunkIdx: int) (k: int) =
        if index.Embeddings.Length = 0 || chunkIdx >= index.Embeddings.Length then [||]
        else
            let target = index.Embeddings.[chunkIdx]
            if target.Length = 0 then [||]
            else
                index.Embeddings
                |> Array.mapi (fun i emb ->
                    if i = chunkIdx || emb.Length = 0 || emb.Length <> target.Length then i, -1f
                    else i, TensorPrimitives.CosineSimilarity(ReadOnlySpan(target), ReadOnlySpan(emb)))
                |> Array.sortByDescending snd
                |> Array.take (min k index.Embeddings.Length)

    /// Normalize file input: may be full path, relative, or just filename.
    let matchFile (filePath: string) (input: string) =
        let inputLower = input.Replace("\\", "/").ToLowerInvariant()
        let pathLower = filePath.Replace("\\", "/").ToLowerInvariant()
        let fileNameLower = Path.GetFileName(filePath).ToLowerInvariant()
        fileNameLower = inputLower
        || pathLower = inputLower
        || pathLower.EndsWith("/" + inputLower)
        || inputLower.EndsWith("/" + fileNameLower)

    let fileChunks (index: DocIndex) (fileName: string) =
        index.Chunks |> Array.indexed
        |> Array.filter (fun (_, c) -> matchFile c.FilePath fileName)

    let backlinks (index: DocIndex) (fileName: string) =
        index.Links |> Array.filter (fun l -> matchFile l.TargetResolved fileName || matchFile l.TargetPath fileName)

    let outlinks (index: DocIndex) (fileName: string) =
        index.Links |> Array.filter (fun l -> matchFile l.SourceFile fileName)
