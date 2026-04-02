namespace AITeam.CodeSight

open System
open System.IO
open System.Text.Json

/// Configuration loaded from code-intel.json (or defaults).
type CodeSightConfig = {
    RepoRoot: string
    SrcDirs: string[]
    Extensions: string[]
    Exclude: string[]
    IndexDir: string
    SummaryCache: string
    EmbeddingUrl: string
    EmbeddingBatchSize: int
    LlmUrl: string
    LlmModel: string
    LlmMaxTokens: int
    LlmTemperature: float
    ChunkerScript: string
    NodePath: string
    MaxChunkChars: int
    NoiseWords: string[]
    UtilityPatterns: string[]
    StagesDir: string
}

module Config =

    let private defaultExtensions = [| ".fs"; ".fsi"; ".cs"; ".js"; ".ts"; ".py"; ".go"; ".rs" |]
    let private defaultExclude = [| "node_modules"; "bin"; "obj"; ".git"; "wwwroot"; "dist" |]

    /// Auto-detect source directories by looking for common patterns.
    let private detectSrcDirs (repoRoot: string) =
        let candidates = [| "src"; "lib"; "Source"; "app" |]
        let found = candidates |> Array.filter (fun d -> Directory.Exists(Path.Combine(repoRoot, d)))
        if found.Length > 0 then found
        else [| "." |]  // fallback: index from root

    /// Load config from code-intel.json, or build defaults.
    let load (repoRoot: string) =
        let configPath = Path.Combine(repoRoot, "code-intel.json")
        let indexDir = Path.Combine(repoRoot, ".code-intel")

        // Locate parsers: alongside exe, or in the PoC directory
        let exeDir = AppDomain.CurrentDomain.BaseDirectory
        let parsersDir =
            let candidate = Path.Combine(exeDir, "parsers")
            if Directory.Exists candidate then candidate
            else
                // Fallback: look for the PoC directory
                let pocDir = Path.Combine(repoRoot, "tools", "embeddings-poc")
                if Directory.Exists pocDir then pocDir
                else exeDir

        if File.Exists configPath then
            let json = File.ReadAllText(configPath)
            let doc = JsonDocument.Parse(json)
            let root = doc.RootElement
            let str (p: string) d = match root.TryGetProperty(p) with true, v -> v.GetString() | _ -> d
            let int' (p: string) d = match root.TryGetProperty(p) with true, v -> v.GetInt32() | _ -> d
            let float' (p: string) d = match root.TryGetProperty(p) with true, v -> v.GetDouble() | _ -> d
            let strArr (p: string) d =
                match root.TryGetProperty(p) with
                | true, v -> v.EnumerateArray() |> Seq.map (fun x -> x.GetString()) |> Seq.toArray
                | _ -> d
            {
                RepoRoot = repoRoot
                SrcDirs = strArr "srcDirs" (detectSrcDirs repoRoot)
                Extensions = strArr "extensions" defaultExtensions
                Exclude = strArr "exclude" defaultExclude
                IndexDir = str "indexDir" indexDir
                SummaryCache = Path.Combine(indexDir, ".summary-cache.tsv")
                EmbeddingUrl = str "embeddingUrl" "http://localhost:1234/v1/embeddings"
                EmbeddingBatchSize = int' "embeddingBatchSize" 50
                LlmUrl = str "llmUrl" "http://127.0.0.1:8090/v1/chat/completions"
                LlmModel = str "llmModel" "bonsai"
                LlmMaxTokens = int' "llmMaxTokens" 60
                LlmTemperature = float' "llmTemperature" 0.0
                ChunkerScript = Path.Combine(parsersDir, "ts-chunker.js")
                NodePath = str "nodePath" "node"
                MaxChunkChars = int' "maxChunkChars" 3000
                NoiseWords = strArr "noiseWords" [||]
                UtilityPatterns = strArr "utilityPatterns" [| "Helper"; "Utils"; "Common"; "Shared" |]
                StagesDir = Path.Combine(parsersDir, "stages")
            }
        else
            {
                RepoRoot = repoRoot
                SrcDirs = detectSrcDirs repoRoot
                Extensions = defaultExtensions
                Exclude = defaultExclude
                IndexDir = indexDir
                SummaryCache = Path.Combine(indexDir, ".summary-cache.tsv")
                EmbeddingUrl = "http://localhost:1234/v1/embeddings"
                EmbeddingBatchSize = 50
                LlmUrl = "http://127.0.0.1:8090/v1/chat/completions"
                LlmModel = "bonsai"
                LlmMaxTokens = 60
                LlmTemperature = 0.0
                ChunkerScript = Path.Combine(parsersDir, "ts-chunker.js")
                NodePath = "node"
                MaxChunkChars = 3000
                NoiseWords = [||]
                UtilityPatterns = [| "Helper"; "Utils"; "Common"; "Shared" |]
                StagesDir = Path.Combine(parsersDir, "stages")
            }
