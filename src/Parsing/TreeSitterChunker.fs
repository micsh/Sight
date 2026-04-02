namespace AITeam.CodeSight

open System
open System.Diagnostics
open System.IO
open System.Text.Json

/// Wraps the Node.js tree-sitter chunker process (ts-chunker.js).
/// Communicates via JSONL over stdin/stdout.
module TreeSitterChunker =

    type private ChunkerProcess(nodePath: string, chunkerScript: string, repoRoot: string) =
        let psi = ProcessStartInfo(nodePath, sprintf "\"%s\"" chunkerScript)
        do psi.WorkingDirectory <- repoRoot
        do psi.RedirectStandardInput <- true
        do psi.RedirectStandardOutput <- true
        do psi.RedirectStandardError <- true
        do psi.UseShellExecute <- false
        do psi.CreateNoWindow <- true
        let proc = Process.Start(psi)

        member _.Send(json: string) =
            proc.StandardInput.WriteLine(json)
            proc.StandardInput.Flush()
            let line = proc.StandardOutput.ReadLine()
            if line = null then failwith "ts-chunker process died"
            line

        interface IDisposable with
            member _.Dispose() =
                try proc.StandardInput.WriteLine("{\"cmd\":\"quit\"}")
                    proc.WaitForExit(2000) |> ignore
                    if not proc.HasExited then proc.Kill()
                with _ -> ()

    let private sourceFiles (dir: string) (extensions: string[]) (exclude: string[]) =
        let extSet = Set.ofArray extensions
        let excludeSet = Set.ofArray exclude
        Directory.EnumerateFiles(dir, "*.*", SearchOption.AllDirectories)
        |> Seq.filter (fun f ->
            let ext = Path.GetExtension(f).ToLowerInvariant()
            extSet.Contains ext &&
            not (f.Replace("\\", "/").Split('/') |> Array.exists (fun p -> excludeSet.Contains p)))
        |> Seq.toArray

    /// Chunk all source files in given directories.
    let chunkFiles (cfg: CodeSightConfig) (files: string[]) : CodeChunk[] =
        use chunker = new ChunkerProcess(cfg.NodePath, cfg.ChunkerScript, cfg.RepoRoot)
        let results = ResizeArray()
        for file in files do
            let relPath = Path.GetRelativePath(cfg.RepoRoot, file).Replace("\\", "/")
            let cmd = sprintf """{"cmd":"parse","file":"%s","maxChars":%d}""" (file.Replace("\\", "\\\\")) cfg.MaxChunkChars
            try
                let response = chunker.Send(cmd)
                let doc = JsonDocument.Parse(response)
                let root = doc.RootElement
                match root.TryGetProperty("chunks") with
                | true, chunks ->
                    for ch in chunks.EnumerateArray() do
                        let str (p: string) = match ch.TryGetProperty(p) with true, v -> v.GetString() | _ -> ""
                        let int' (p: string) = match ch.TryGetProperty(p) with true, v -> v.GetInt32() | _ -> 0
                        results.Add({
                            FilePath = relPath
                            Module = str "module"
                            Name = str "name"
                            Kind = str "kind"
                            StartLine = int' "startLine"
                            EndLine = int' "endLine"
                            Content = str "content"
                            Context = str "context"
                        })
                | _ -> ()
            with ex ->
                eprintfn "  Warning: failed to chunk %s: %s" relPath ex.Message
        results.ToArray()

    /// Extract imports from source files.
    let extractImports (cfg: CodeSightConfig) (files: string[]) : FileImport[] =
        use chunker = new ChunkerProcess(cfg.NodePath, cfg.ChunkerScript, cfg.RepoRoot)
        let results = ResizeArray()
        for file in files do
            let relPath = Path.GetRelativePath(cfg.RepoRoot, file).Replace("\\", "/")
            let cmd = sprintf """{"cmd":"imports","file":"%s"}""" (file.Replace("\\", "\\\\"))
            try
                let response = chunker.Send(cmd)
                let doc = JsonDocument.Parse(response)
                match doc.RootElement.TryGetProperty("imports") with
                | true, imports ->
                    for imp in imports.EnumerateArray() do
                        let str (p: string) = match imp.TryGetProperty(p) with true, v -> v.GetString() | _ -> ""
                        let int' (p: string) = match imp.TryGetProperty(p) with true, v -> v.GetInt32() | _ -> 0
                        results.Add({ FilePath = relPath; Module = str "module"; Line = int' "line"; Raw = str "raw" })
                | _ -> ()
            with _ -> ()
        results.ToArray()

    /// Extract signatures from source files.
    let extractSignatures (cfg: CodeSightConfig) (files: string[]) : DeclSignature[] =
        use chunker = new ChunkerProcess(cfg.NodePath, cfg.ChunkerScript, cfg.RepoRoot)
        let results = ResizeArray()
        for file in files do
            let relPath = Path.GetRelativePath(cfg.RepoRoot, file).Replace("\\", "/")
            let cmd = sprintf """{"cmd":"signatures","file":"%s"}""" (file.Replace("\\", "\\\\"))
            try
                let response = chunker.Send(cmd)
                let doc = JsonDocument.Parse(response)
                match doc.RootElement.TryGetProperty("signatures") with
                | true, sigs ->
                    for s in sigs.EnumerateArray() do
                        let str (p: string) = match s.TryGetProperty(p) with true, v -> v.GetString() | _ -> ""
                        let int' (p: string) = match s.TryGetProperty(p) with true, v -> v.GetInt32() | _ -> 0
                        results.Add({ Name = str "name"; Kind = str "kind"; Signature = str "signature"; FilePath = relPath; StartLine = int' "startLine" })
                | _ -> ()
            with _ -> ()
        results.ToArray()

    /// Extract type references from source files.
    let extractTypeRefs (cfg: CodeSightConfig) (files: string[]) : FileTypeRef[] =
        use chunker = new ChunkerProcess(cfg.NodePath, cfg.ChunkerScript, cfg.RepoRoot)
        let results = ResizeArray()
        for file in files do
            let relPath = Path.GetRelativePath(cfg.RepoRoot, file).Replace("\\", "/")
            let cmd = sprintf """{"cmd":"typerefs","file":"%s"}""" (file.Replace("\\", "\\\\"))
            try
                let response = chunker.Send(cmd)
                let doc = JsonDocument.Parse(response)
                match doc.RootElement.TryGetProperty("typeRefs") with
                | true, refs ->
                    let typeNames = refs.EnumerateArray() |> Seq.map (fun v -> v.GetString()) |> Seq.toArray
                    if typeNames.Length > 0 then
                        results.Add({ FilePath = relPath; TypeRefs = typeNames })
                | _ -> ()
            with _ -> ()
        results.ToArray()

    /// Enumerate source files matching config.
    let findSourceFiles (cfg: CodeSightConfig) =
        cfg.SrcDirs
        |> Array.collect (fun d ->
            let dir = Path.Combine(cfg.RepoRoot, d)
            if Directory.Exists dir then sourceFiles dir cfg.Extensions cfg.Exclude
            else [||])
