namespace AITeam.CodeSight

open System
open System.IO
open System.Numerics.Tensors

/// Index persistence and in-memory query operations.
module IndexStore =

    let private coreFields = [| "FilePath"; "Module"; "Name"; "Kind"; "StartLine"; "EndLine"; "Summary"; "Signature" |]

    // ── Persistence helpers ──

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

    // ── Save ──

    let save (dir: string) (index: CodeIndex) =
        Directory.CreateDirectory(dir) |> ignore

        let extraKeys =
            index.Chunks
            |> Array.collect (fun c -> c.Extra |> Map.toArray |> Array.map fst)
            |> Array.distinct |> Array.sort

        let allFields = Array.append coreFields extraKeys
        let header = sprintf "#fields:%s" (allFields |> String.concat "\t")
        let chunkLines =
            index.Chunks |> Array.map (fun c ->
                let core = sprintf "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s"
                               (escape c.FilePath) (escape c.Module) (escape c.Name)
                               c.Kind c.StartLine c.EndLine (escape c.Summary) (escape c.Signature)
                let extras = extraKeys |> Array.map (fun k -> c.Extra |> Map.tryFind k |> Option.defaultValue "" |> escape)
                if extras.Length > 0 then sprintf "%s\t%s" core (extras |> String.concat "\t")
                else core)
        File.WriteAllLines(Path.Combine(dir, "chunks.tsv"), Array.append [| header |] chunkLines)

        writeEmbeddings (Path.Combine(dir, "code.emb")) index.CodeEmbeddings
        writeEmbeddings (Path.Combine(dir, "summary.emb")) index.SummaryEmbeddings

        let importLines = index.Imports |> Array.map (fun (f, m) -> sprintf "%s\t%s" (escape f) (escape m))
        File.WriteAllLines(Path.Combine(dir, "imports.tsv"), importLines)

        let refLines = index.TypeRefs |> Array.map (fun (f, refs) -> sprintf "%s\t%s" (escape f) (refs |> String.concat ","))
        File.WriteAllLines(Path.Combine(dir, "typerefs.tsv"), refLines)

        eprintfn "  Index saved: %d chunks (%d extra fields), %d imports → %s"
            index.Chunks.Length extraKeys.Length index.Imports.Length dir

    // ── Load ──

    let load (dir: string) : CodeIndex option =
        let chunkFile = Path.Combine(dir, "chunks.tsv")
        let codeEmbFile = Path.Combine(dir, "code.emb")
        if not (File.Exists chunkFile) || not (File.Exists codeEmbFile) then None
        else
            let allLines = File.ReadAllLines(chunkFile)
            let headerLine, dataLines =
                if allLines.Length > 0 && allLines.[0].StartsWith("#fields:") then
                    Some (allLines.[0].Substring(8).Split('\t')), allLines.[1..]
                else None, allLines

            let extraFieldNames =
                match headerLine with
                | Some fields when fields.Length > 8 -> fields.[8..]
                | _ -> [||]

            let chunks =
                dataLines |> Array.choose (fun line ->
                    let p = line.Split('\t')
                    if p.Length >= 7 then
                        let sigStr = if p.Length >= 8 then p.[7] else ""
                        let extra =
                            extraFieldNames
                            |> Array.mapi (fun i name ->
                                let colIdx = 8 + i
                                if colIdx < p.Length then Some (name, p.[colIdx]) else None)
                            |> Array.choose id |> Map.ofArray
                        Some { FilePath = p.[0]; Module = p.[1]; Name = p.[2]; Kind = p.[3]
                               StartLine = int p.[4]; EndLine = int p.[5]; Summary = p.[6]
                               Signature = sigStr; Extra = extra }
                    else None)
            let codeEmbs = readEmbeddings codeEmbFile |> Option.defaultValue [||]
            let sumEmbFile = Path.Combine(dir, "summary.emb")
            let sumEmbs = if File.Exists sumEmbFile then readEmbeddings sumEmbFile |> Option.defaultValue [||] else [||]
            let imports =
                let f = Path.Combine(dir, "imports.tsv")
                if File.Exists f then
                    File.ReadAllLines(f) |> Array.choose (fun line ->
                        let p = line.Split('\t')
                        if p.Length >= 2 then Some (p.[0], p.[1]) else None)
                else [||]
            let typeRefs =
                let f = Path.Combine(dir, "typerefs.tsv")
                if File.Exists f then
                    File.ReadAllLines(f) |> Array.choose (fun line ->
                        let p = line.Split('\t')
                        if p.Length >= 2 then Some (p.[0], p.[1].Split(',') |> Array.filter (fun s -> s <> ""))
                        else None)
                else [||]
            let dim = if codeEmbs.Length > 0 then codeEmbs.[0].Length else 0
            Some { Chunks = chunks; CodeEmbeddings = codeEmbs; SummaryEmbeddings = sumEmbs
                   Imports = imports; TypeRefs = typeRefs; EmbeddingDim = dim }

    // ── Query functions ──

    let search (index: CodeIndex) (queryEmbedding: float32[]) (k: int) =
        if index.SummaryEmbeddings.Length = 0 then [||]
        else
            index.SummaryEmbeddings
            |> Array.mapi (fun i emb ->
                let sim = TensorPrimitives.CosineSimilarity(ReadOnlySpan(queryEmbedding), ReadOnlySpan(emb))
                i, sim)
            |> Array.sortByDescending snd
            |> Array.take (min k index.SummaryEmbeddings.Length)

    let similar (index: CodeIndex) (chunkIdx: int) (k: int) (useSummary: bool) =
        let embeddings = if useSummary then index.SummaryEmbeddings else index.CodeEmbeddings
        if embeddings.Length = 0 || chunkIdx >= embeddings.Length then [||]
        else
            let target = embeddings.[chunkIdx]
            embeddings
            |> Array.mapi (fun i emb ->
                if i = chunkIdx then i, -1f
                else i, TensorPrimitives.CosineSimilarity(ReadOnlySpan(target), ReadOnlySpan(emb)))
            |> Array.sortByDescending snd
            |> Array.take (min k embeddings.Length)

    let fileContextByName (index: CodeIndex) (fileName: string) =
        let target = fileName.ToLowerInvariant()
        index.Chunks |> Array.indexed
        |> Array.filter (fun (_, c) -> Path.GetFileName(c.FilePath).ToLowerInvariant() = target)

    let fileImports (index: CodeIndex) (fileName: string) =
        let target = fileName.ToLowerInvariant()
        index.Imports
        |> Array.filter (fun (f, _) -> Path.GetFileName(f).ToLowerInvariant() = target)
        |> Array.map snd

    let dependents (index: CodeIndex) (moduleName: string) =
        let target = moduleName.ToLowerInvariant()
        index.Imports
        |> Array.filter (fun (_, m) -> m.ToLowerInvariant().Contains(target))
        |> Array.map fst |> Array.distinct

    let typeImpact (index: CodeIndex) (typeName: string) =
        index.TypeRefs
        |> Array.filter (fun (_, refs) -> refs |> Array.exists (fun r -> r = typeName))
        |> Array.map fst

    let formatChunk (index: CodeIndex) (chunkIdx: int) =
        let c = index.Chunks.[chunkIdx]
        let file = Path.GetFileName(c.FilePath)
        let sigStr = if c.Signature <> "" then sprintf "  %s" c.Signature else ""
        let extraStr =
            if c.Extra.IsEmpty then ""
            else c.Extra |> Map.toSeq |> Seq.map (fun (k, v) -> sprintf " [%s=%s]" k v) |> String.concat ""
        sprintf "%s:%s (%s:%d)%s — %s%s" c.Kind c.Name file c.StartLine sigStr c.Summary extraStr
