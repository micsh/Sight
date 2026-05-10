namespace AITeam.KnowledgeSight

open System
open System.IO
open System.Text.Json

type KnowledgeSightConfig = {
    RepoRoot: string
    DocDirs: string[]
    Exclude: string[]
    IndexDir: string
    EmbeddingUrl: string
    EmbeddingBatchSize: int
    CompletionUrl: string
    ConflictJudgeModel: string
    InboxDir: string
    ArchiveProcessed: bool
    RequireFields: string[]
    RequireFieldsMode: string
    PromoteCollision: string
    NoveltyCorpus: NoveltyCorpusConfig option
}

and NoveltyCorpusConfig = {
    ExcludePaths: string[]
    ExcludeFrontmatter: Map<string, FrontmatterValue>
}

module CliOutput =

    let mutable private quiet = false

    let setQuiet (enabled: bool) =
        quiet <- enabled

    let info format =
        Printf.kprintf
            (fun message ->
                if not quiet then
                    Console.Error.WriteLine(message))
            format

module Config =

    let private defaultExclude = [| "node_modules"; "bin"; "obj"; ".git"; "wwwroot"; "dist"; ".code-intel" |]

    let private normalizeScanDir (dir: string) =
        let trimmed = dir.Replace("\\", "/").Trim().Trim('/')
        if trimmed = "" then "."
        else trimmed

    let private normalizeConfiguredDir (dir: string) =
        normalizeScanDir dir

    let resolveInboxDir (cfg: KnowledgeSightConfig) =
        let normalizedInboxDir = normalizeConfiguredDir cfg.InboxDir

        if normalizedInboxDir = "."
           || normalizedInboxDir.Contains("/", StringComparison.Ordinal) then
            Ok normalizedInboxDir
        else
            let nestedCandidates =
                cfg.DocDirs
                |> Array.map normalizeConfiguredDir
                |> Array.filter (fun dir ->
                    dir <> "."
                    && not (String.Equals(dir, normalizedInboxDir, StringComparison.OrdinalIgnoreCase)))
                |> Array.map (fun dir -> normalizeConfiguredDir (dir + "/" + normalizedInboxDir))
                |> Array.distinct
                |> Array.filter (fun candidate ->
                    let candidatePath = Path.Combine(cfg.RepoRoot, candidate.Replace("/", string Path.DirectorySeparatorChar))
                    Directory.Exists candidatePath)

            if nestedCandidates.Length = 0 then
                Ok normalizedInboxDir
            else
                let configuredDocDirs =
                    cfg.DocDirs
                    |> Array.map normalizeConfiguredDir
                    |> String.concat ", "

                Error (
                    sprintf
                        "inboxDir '%s' is ambiguous for docDirs [%s]: found nested inbox root(s) at %s. Set inboxDir to the intended repo-relative inbox root explicitly."
                        cfg.InboxDir
                        configuredDocDirs
                        (String.concat ", " nestedCandidates)
                )

    /// Auto-detect directories containing markdown files.
    let private detectDocDirs (repoRoot: string) =
        // Check common doc locations
        let candidates = [| ".agents"; "docs"; "doc"; "wiki"; "knowledge"; "." |]
        let found = candidates |> Array.filter (fun d ->
            let dir = Path.Combine(repoRoot, d)
            Directory.Exists(dir) &&
            Directory.EnumerateFiles(dir, "*.md", SearchOption.AllDirectories) |> Seq.truncate 1 |> Seq.length > 0)
        if found.Length > 0 then found
        else [| "." |]

    let private strArrFromElement (element: JsonElement) (p: string) d =
        match element.TryGetProperty(p) with
        | true, v ->
            v.EnumerateArray()
            |> Seq.map (fun x -> x.GetString())
            |> Seq.filter (isNull >> not)
            |> Seq.map (fun value -> value.Trim())
            |> Seq.filter (String.IsNullOrWhiteSpace >> not)
            |> Seq.toArray
        | _ -> d

    let private parseNoveltyFrontmatterValue (field: string) (value: JsonElement) =
        let invalid () =
            invalidOp (
                sprintf
                    "knowledge-sight.json noveltyCorpus.excludeFrontmatter.%s must be a string or string array"
                    field
            )

        match value.ValueKind with
        | JsonValueKind.String ->
            let scalar = value.GetString()
            if String.IsNullOrWhiteSpace(scalar) then None
            else Some (field, Scalar scalar)
        | JsonValueKind.Array ->
            let values =
                value.EnumerateArray()
                |> Seq.map (fun item ->
                    if item.ValueKind <> JsonValueKind.String then invalid ()
                    item.GetString())
                |> Seq.filter (isNull >> not)
                |> Seq.map (fun item -> item.Trim())
                |> Seq.filter (String.IsNullOrWhiteSpace >> not)
                |> Seq.toArray

            if values.Length = 0 then None
            else Some (field, StringList values)
        | _ -> invalid ()

    let private parseNoveltyCorpus (root: JsonElement) =
        match root.TryGetProperty("noveltyCorpus") with
        | true, corpusElement ->
            if corpusElement.ValueKind <> JsonValueKind.Object then
                invalidOp "knowledge-sight.json noveltyCorpus must be an object"

            let excludeFrontmatter =
                match corpusElement.TryGetProperty("excludeFrontmatter") with
                | true, value ->
                    if value.ValueKind <> JsonValueKind.Object then
                        invalidOp "knowledge-sight.json noveltyCorpus.excludeFrontmatter must be an object"

                    value.EnumerateObject()
                    |> Seq.choose (fun property ->
                        let field = property.Name.Trim()
                        if String.IsNullOrWhiteSpace(field) then None
                        else parseNoveltyFrontmatterValue field property.Value)
                    |> Map.ofSeq
                | _ -> Map.empty

            Some {
                ExcludePaths = strArrFromElement corpusElement "excludePaths" [||]
                ExcludeFrontmatter = excludeFrontmatter
            }
        | _ -> None

    let load (repoRoot: string) =
        let repoRoot = Path.GetFullPath(repoRoot)
        let configPath = Path.Combine(repoRoot, "knowledge-sight.json")
        let indexDir = Path.Combine(repoRoot, ".knowledge-sight")

        if File.Exists configPath then
            let json = File.ReadAllText(configPath)
            let doc = JsonDocument.Parse(json)
            let root = doc.RootElement
            let str (p: string) d = match root.TryGetProperty(p) with true, v -> v.GetString() | _ -> d
            let int' (p: string) d = match root.TryGetProperty(p) with true, v -> v.GetInt32() | _ -> d
            let bool' (p: string) d = match root.TryGetProperty(p) with true, v -> v.GetBoolean() | _ -> d
            let strArr (p: string) d = strArrFromElement root p d
            {
                RepoRoot = repoRoot
                DocDirs = strArr "docDirs" (detectDocDirs repoRoot)
                Exclude = strArr "exclude" defaultExclude
                IndexDir = str "indexDir" indexDir
                EmbeddingUrl = str "embeddingUrl" "http://localhost:1234/v1/embeddings"
                EmbeddingBatchSize = int' "embeddingBatchSize" 50
                CompletionUrl = str "completionUrl" ""
                ConflictJudgeModel = str "conflictJudgeModel" ""
                InboxDir = str "inboxDir" "inbox"
                ArchiveProcessed = bool' "archiveProcessed" true
                RequireFields = strArr "requireFields" [| "verify"; "concept" |]
                RequireFieldsMode = str "requireFieldsMode" "warn"
                PromoteCollision = str "promoteCollision" "suffix"
                NoveltyCorpus = parseNoveltyCorpus root
            }
        else
            {
                RepoRoot = repoRoot
                DocDirs = detectDocDirs repoRoot
                Exclude = defaultExclude
                IndexDir = indexDir
                EmbeddingUrl = "http://localhost:1234/v1/embeddings"
                EmbeddingBatchSize = 50
                CompletionUrl = ""
                ConflictJudgeModel = ""
                InboxDir = "inbox"
                ArchiveProcessed = true
                RequireFields = [| "verify"; "concept" |]
                RequireFieldsMode = "warn"
                PromoteCollision = "suffix"
                NoveltyCorpus = None
            }

    let scanDocDirs (cfg: KnowledgeSightConfig) =
        resolveInboxDir cfg
        |> Result.map (fun inboxDir ->
            [| yield! cfg.DocDirs; yield inboxDir |]
            |> Array.filter (String.IsNullOrWhiteSpace >> not)
            |> Array.map normalizeScanDir
            |> Array.distinct)

    /// Find all .md files under the configured doc dirs.
    let findDocFiles (cfg: KnowledgeSightConfig) =
        scanDocDirs cfg
        |> Result.map (fun scanDirs ->
            scanDirs
            |> Array.collect (fun dir ->
                let absDir = Path.Combine(cfg.RepoRoot, dir)
                if Directory.Exists absDir then
                    Directory.EnumerateFiles(absDir, "*.md", SearchOption.AllDirectories)
                    |> Seq.filter (fun f ->
                        let rel = Path.GetRelativePath(cfg.RepoRoot, f).Replace("\\", "/")
                        cfg.Exclude |> Array.forall (fun ex -> not (rel.Contains(ex))))
                    |> Seq.toArray
                else [||])
            |> Array.distinct
            |> Array.sort)
