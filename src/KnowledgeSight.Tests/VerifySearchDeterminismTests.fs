namespace AITeam.KnowledgeSight.Tests

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Xunit
open AITeam.Sight.Core
open AITeam.KnowledgeSight

module VerifySearchDeterminismTests =

    let private writeFile (filePath: string) (content: string) =
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)) |> ignore
        File.WriteAllText(filePath, content.TrimStart(), Encoding.UTF8)

    let private writeConfigWithMutator
        (repoRoot: string)
        (embeddingUrl: string)
        (docDirs: string[])
        (inboxDir: string)
        (requireFieldsMode: string)
        (mutate: JsonObject -> unit)
        =
        let config = JsonObject()
        config["docDirs"] <- JsonSerializer.SerializeToNode(docDirs)
        config["inboxDir"] <- JsonValue.Create(inboxDir)
        config["archiveProcessed"] <- JsonValue.Create(true)
        config["embeddingUrl"] <- JsonValue.Create(embeddingUrl)
        config["requireFieldsMode"] <- JsonValue.Create(requireFieldsMode)
        mutate config

        let json = config.ToJsonString(JsonSerializerOptions(WriteIndented = true))
        File.WriteAllText(Path.Combine(repoRoot, "knowledge-sight.json"), json)

    let private writeConfig (repoRoot: string) (embeddingUrl: string) (docDirs: string[]) (inboxDir: string) (requireFieldsMode: string) =
        writeConfigWithMutator repoRoot embeddingUrl docDirs inboxDir requireFieldsMode ignore

    let private findFreePort () =
        let listener = new TcpListener(IPAddress.Loopback, 0)
        listener.Start()
        let port = (listener.LocalEndpoint :?> IPEndPoint).Port
        listener.Stop()
        port

    let private jsStringLiteral (value: string) = JsonSerializer.Serialize(value)

    type private EmbeddingServer private (listener: HttpListener, loopTask: Task, port: int, requestCount: int ref) =
        member _.EmbeddingUrl = sprintf "http://127.0.0.1:%d/v1/embeddings" port
        member _.RequestCount = requestCount.Value
        member _.Stop() =
            if listener.IsListening then
                listener.Stop()

        interface IDisposable with
            member _.Dispose() =
                try
                    if listener.IsListening then
                        listener.Stop()
                    listener.Close()
                with _ ->
                    ()

                try
                    loopTask.Wait(TimeSpan.FromSeconds(5.0)) |> ignore
                with _ ->
                    ()

        static member Start() =
            let port = findFreePort ()
            let listener = new HttpListener()
            listener.Prefixes.Add(sprintf "http://127.0.0.1:%d/" port)
            listener.Start()

            let requestCount = ref 0

            let loopTask =
                Task.Run(fun () ->
                    task {
                        try
                            while listener.IsListening do
                                let! context = listener.GetContextAsync()
                                use reader = new StreamReader(context.Request.InputStream, context.Request.ContentEncoding)
                                let! body = reader.ReadToEndAsync()

                                try
                                    if String.Equals(context.Request.RawUrl, "/v1/embeddings", StringComparison.Ordinal) then
                                        requestCount.Value <- requestCount.Value + 1

                                        use doc = JsonDocument.Parse(body)
                                        let input =
                                            match doc.RootElement.TryGetProperty("input") with
                                            | true, value -> value
                                            | _ -> failwith "embedding request missing input"

                                        let embeddings =
                                            input.EnumerateArray()
                                            |> Seq.map (fun _ -> {| embedding = [| 1.0f; 0.0f; 0.0f |] |})
                                            |> Seq.toArray

                                        let payload = JsonSerializer.Serialize({| data = embeddings |})
                                        let bytes = Encoding.UTF8.GetBytes(payload)
                                        context.Response.StatusCode <- 200
                                        context.Response.ContentType <- "application/json"
                                        context.Response.ContentLength64 <- int64 bytes.Length
                                        do! context.Response.OutputStream.WriteAsync(bytes, 0, bytes.Length)
                                    else
                                        context.Response.StatusCode <- 404
                                finally
                                    context.Response.Close()
                        with
                        | :? HttpListenerException
                        | :? ObjectDisposedException -> ()
                    } :> Task)

            new EmbeddingServer(listener, loopTask, port, requestCount)

    type private VerifyHarness private (repoRoot: string, server: EmbeddingServer, engine: Jint.Engine) =
        member _.EvalJson (query: string) = QueryEngine.evalJson engine query
        member _.EmbeddingRequestCount = server.RequestCount
        member _.StopEmbeddingServer() = server.Stop()
        member _.RepoRoot = repoRoot
        member _.FileExists (relativePath: string) =
            File.Exists(Path.Combine(repoRoot, relativePath.Replace("/", Path.DirectorySeparatorChar.ToString())))
        member _.ReadFile (relativePath: string) =
            File.ReadAllText(Path.Combine(repoRoot, relativePath.Replace("/", Path.DirectorySeparatorChar.ToString())))

        interface IDisposable with
            member _.Dispose() =
                (server :> IDisposable).Dispose()
                try
                    Directory.Delete(repoRoot, true)
                with _ ->
                    ()

        static member Create(seedRepo: string -> unit) =
            VerifyHarness.CreateWithConfig([| "docs"; "inbox" |], "inbox", seedRepo)

        static member CreateWithConfig(docDirs: string[], inboxDir: string, seedRepo: string -> unit) =
            VerifyHarness.CreateWithRequireFieldsMode(docDirs, inboxDir, "warn", seedRepo)

        static member CreateWithRequireFieldsMode(docDirs: string[], inboxDir: string, requireFieldsMode: string, seedRepo: string -> unit) =
            VerifyHarness.CreateWithConfigMutator(docDirs, inboxDir, requireFieldsMode, ignore, seedRepo)

        static member CreateWithConfigMutator(docDirs: string[], inboxDir: string, requireFieldsMode: string, mutate: JsonObject -> unit, seedRepo: string -> unit) =
            let server = EmbeddingServer.Start()
            let repoRoot = Path.Combine(Path.GetTempPath(), sprintf "ks-verify-search-tests-%s" (Guid.NewGuid().ToString("N")))
            Directory.CreateDirectory(repoRoot) |> ignore
            writeConfigWithMutator repoRoot server.EmbeddingUrl docDirs inboxDir requireFieldsMode mutate
            seedRepo repoRoot

            let cfg = Config.load repoRoot

            match IndexingWorkflow.rebuild cfg with
            | Error message ->
                (server :> IDisposable).Dispose()
                failwithf "Index build failed in test harness: %s" message
            | Ok (index, chunks) ->
                let engine = QueryEngine.create cfg index chunks
                new VerifyHarness(repoRoot, server, engine)

    type private TeeTextWriter(primary: TextWriter, secondary: TextWriter) =
        inherit TextWriter()

        override _.Encoding = primary.Encoding

        override _.Write(value: char) =
            primary.Write(value)
            secondary.Write(value)

        override _.Write(value: string) =
            primary.Write(value)
            secondary.Write(value)

        override _.Write(buffer: char[], index: int, count: int) =
            primary.Write(buffer, index, count)
            secondary.Write(buffer, index, count)

        override _.WriteLine(value: string) =
            primary.WriteLine(value)
            secondary.WriteLine(value)

        override _.Flush() =
            primary.Flush()
            secondary.Flush()

    type private CliRunResult = {
        ExitCode: int
        Stdout: string
        Stderr: string
        Combined: string
    }

    let private runCli (args: string[]) =
        let originalOut = Console.Out
        let originalErr = Console.Error
        use stdoutWriter = new StringWriter()
        use stderrWriter = new StringWriter()
        use combinedWriter = new StringWriter()
        use teeOut = new TeeTextWriter(stdoutWriter, combinedWriter)
        use teeErr = new TeeTextWriter(stderrWriter, combinedWriter)

        try
            Console.SetOut(teeOut)
            Console.SetError(teeErr)
            let exitCode = Program.main args
            teeOut.Flush()
            teeErr.Flush()

            {
                ExitCode = exitCode
                Stdout = stdoutWriter.ToString()
                Stderr = stderrWriter.ToString()
                Combined = combinedWriter.ToString()
            }
        finally
            Console.SetOut(originalOut)
            Console.SetError(originalErr)
            CliOutput.setQuiet false

    let private seedCliQuietRepo repoRoot =
        writeFile
            (Path.Combine(repoRoot, "docs", "canon", "render-fallback.md"))
            """
---
title: "Render fallback policy"
status: "active"
tags: ["render", "fallback"]
---
# Render fallback policy

Render fallback should remain visible to operators during verification.
"""

    let private getRequiredProperty (name: string) (element: JsonElement) =
        match element.TryGetProperty(name) with
        | true, value -> value
        | _ -> failwithf "Missing property '%s'" name

    let private getSingleArrayResult (element: JsonElement) =
        Assert.Equal(JsonValueKind.Array, element.ValueKind)
        Assert.Equal(1, element.GetArrayLength())
        element[0]

    let private tryGetProperty (name: string) (element: JsonElement) =
        match element.TryGetProperty(name) with
        | true, value -> Some value
        | _ -> None

    [<Fact>]
    let ``cli quiet keeps rebuild-driven json combined output machine-consumable`` () =
        use harness = VerifyHarness.Create(seedCliQuietRepo)

        let query =
            sprintf
                "propose(%s, {team:'ops', cycle:'2026-05-09T12-00-00Z', threshold:1.1})"
                (jsStringLiteral "CLI quiet mode should keep rebuild JSON output clean for piping.")

        let result =
            runCli
                [|
                    "eval"
                    query
                    "--json"
                    "--quiet"
                    "--repo"
                    harness.RepoRoot
                |]

        Assert.Equal(0, result.ExitCode)
        Assert.True(String.IsNullOrWhiteSpace(result.Stderr), sprintf "Expected no informational stderr, got: %s" result.Stderr)
        Assert.Equal(result.Stdout.Trim(), result.Combined.Trim())

        use doc = JsonDocument.Parse(result.Combined.Trim())
        let row = getSingleArrayResult doc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" row).GetString())

    [<Fact>]
    let ``cli default rebuild-driven json workflow still emits progress without quiet`` () =
        use harness = VerifyHarness.Create(seedCliQuietRepo)

        let query =
            sprintf
                "propose(%s, {team:'ops', cycle:'2026-05-09T12-05-00Z', threshold:1.1})"
                (jsStringLiteral "Default CLI JSON mode should keep progress visible unless quiet is requested.")

        let result =
            runCli
                [|
                    "eval"
                    query
                    "--json"
                    "--repo"
                    harness.RepoRoot
                |]

        Assert.Equal(0, result.ExitCode)
        Assert.Contains("▶ Indexing docs in", result.Stderr)
        Assert.Contains("✓ Index complete:", result.Stderr)
        Assert.ThrowsAny<JsonException>(fun () -> JsonDocument.Parse(result.Combined.Trim()) |> ignore)

    [<Fact>]
    let ``cli quiet suppresses standalone index progress when index is invoked directly`` () =
        use harness = VerifyHarness.Create(seedCliQuietRepo)

        writeFile
            (Path.Combine(harness.RepoRoot, "docs", "canon", "fresh-index-doc.md"))
            """
---
title: "Fresh index doc"
status: "active"
---
# Fresh index doc

Standalone index quiet should suppress informational stderr.
"""

        let result =
            runCli
                [|
                    "index"
                    "--quiet"
                    "--repo"
                    harness.RepoRoot
                |]

        Assert.Equal(0, result.ExitCode)
        Assert.True(String.IsNullOrWhiteSpace(result.Stdout))
        Assert.True(String.IsNullOrWhiteSpace(result.Stderr), sprintf "Expected standalone index quiet to suppress progress, got: %s" result.Stderr)

    [<Fact>]
    let ``cli quiet still leaves real stderr diagnostics visible`` () =
        let repoRoot = Path.Combine(Path.GetTempPath(), sprintf "ks-cli-quiet-no-index-%s" (Guid.NewGuid().ToString("N")))
        Directory.CreateDirectory(repoRoot) |> ignore

        try
            let result =
                runCli
                    [|
                        "search"
                        "files()"
                        "--json"
                        "--quiet"
                        "--repo"
                        repoRoot
                    |]

            Assert.Equal(1, result.ExitCode)
            Assert.Contains("No index found. Run: knowledge-sight index", result.Stderr)
        finally
            try
                Directory.Delete(repoRoot, true)
            with _ ->
                ()

    [<Fact>]
    let ``shared wrapper preserves semicolons inside quoted literals for string-first bindings`` () =
        let engine = Jint.Engine()
        engine.SetValue("propose", Func<string, obj, obj>(fun text _ -> box text)) |> ignore

        let result =
            QueryHelpers.evalJson
                engine
                "propose('Alpha should remain true; Beta should remain true; Gamma should remain true.', {team:'ops'})"

        Assert.Equal("Alpha should remain true; Beta should remain true; Gamma should remain true.", result)

    [<Fact>]
    let ``shared wrapper still returns the trailing expression after top-level semicolons`` () =
        let result = QueryHelpers.evalJson (Jint.Engine()) "let claim = 'Alpha should remain true'; claim"
        Assert.Equal("Alpha should remain true", result)

    [<Fact>]
    let ``supersede persists deterministic verify search cache and reverify reuses it without live embeddings`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "verify-alpha.md"))
                """
---
title: "Verify alpha"
status: "active"
source: "canon"
---
Deterministic verify search coverage for active canonical docs.
"""

        use harness = VerifyHarness.Create(seedRepo)

        let searchQuery = "search('deterministic verify search', {limit: 1, file: 'docs/canon/verify-alpha.md', status:['active']})"
        use searchDoc = JsonDocument.Parse(harness.EvalJson(searchQuery))

        let searchResults = searchDoc.RootElement
        Assert.Equal(JsonValueKind.Array, searchResults.ValueKind)
        Assert.Equal(1, searchResults.GetArrayLength())

        let refId = (getRequiredProperty "id" searchResults[0]).GetString()
        let newPath = "docs/canon/verify-alpha-v2.md"
        let verifyExpr = sprintf "search('deterministic verify search', {limit: 1, file: '%s', status:['active']}).length === 1" newPath
        let supersedeQuery =
            sprintf "supersede(%s, %s, {reason:'refresh deterministic verify search', by:'ops', verify:%s})"
                (jsStringLiteral refId)
                (jsStringLiteral "Deterministic verify search coverage for the replacement canonical doc.\n")
                (jsStringLiteral verifyExpr)
        use supersedeDoc = JsonDocument.Parse(harness.EvalJson(supersedeQuery))

        let supersedeRoot = supersedeDoc.RootElement
        Assert.Equal("supersede", (getRequiredProperty "action" supersedeRoot).GetString())
        Assert.Equal(newPath, (getRequiredProperty "newPath" supersedeRoot).GetString())

        let persistedMarkdown = harness.ReadFile(newPath)
        Assert.Contains(
            "verify: search('deterministic verify search', {limit: 1, file: 'docs/canon/verify-alpha-v2.md', status:['active']}).length === 1",
            persistedMarkdown,
            StringComparison.Ordinal)
        Assert.DoesNotContain("\\u0027", persistedMarkdown, StringComparison.OrdinalIgnoreCase)
        Assert.DoesNotContain("\\u003e", persistedMarkdown, StringComparison.OrdinalIgnoreCase)

        let contextQuery = sprintf "context(%s)" (jsStringLiteral newPath)
        use contextDoc = JsonDocument.Parse(harness.EvalJson(contextQuery))
        let frontmatter = getRequiredProperty "frontmatter" contextDoc.RootElement
        Assert.Equal(verifyExpr, (getRequiredProperty "verify" frontmatter).GetString())
        let persistedCache = (getRequiredProperty "verify_search_cache" frontmatter).GetString()
        Assert.False(String.IsNullOrWhiteSpace(persistedCache))

        let requestsBeforeReverify = harness.EmbeddingRequestCount
        let reverifyQuery = sprintf "reverify({scope:[%s]})" (jsStringLiteral newPath)
        use reverifyDoc = JsonDocument.Parse(harness.EvalJson(reverifyQuery))

        Assert.Equal(requestsBeforeReverify, harness.EmbeddingRequestCount)

        let reverifyResults = reverifyDoc.RootElement
        Assert.Equal(JsonValueKind.Array, reverifyResults.ValueKind)
        Assert.Equal(1, reverifyResults.GetArrayLength())
        let outcome = (getRequiredProperty "outcome" reverifyResults[0]).GetString()
        Assert.True(String.Equals(outcome, "ok", StringComparison.Ordinal), reverifyResults.GetRawText())

    [<Fact>]
    let ``legacy escaped verify docs still reverify successfully`` () =
        let path = "docs/canon/legacy-escaped-verify.md"
        let escapedVerify = @"files(\u0027docs/canon/legacy-escaped-verify.md\u0027).length \u003E= 1"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, path.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Legacy escaped verify"
status: "active"
source: "canon"
verify: "%s"
verify_snapshot: "legacy-snapshot"
---
Legacy escaped verify doc.
""" escapedVerify)

        use harness = VerifyHarness.Create(seedRepo)

        let persistedMarkdown = harness.ReadFile(path)
        Assert.Contains(escapedVerify, persistedMarkdown, StringComparison.Ordinal)

        use reverifyDoc = JsonDocument.Parse(harness.EvalJson(sprintf "reverify({scope:[%s]})" (jsStringLiteral path)))
        let result = reverifyDoc.RootElement[0]
        let outcome = (getRequiredProperty "outcome" result).GetString()
        Assert.False(String.Equals(outcome, "error", StringComparison.OrdinalIgnoreCase), reverifyDoc.RootElement.GetRawText())

    [<Fact>]
    let ``files exposes persisted indexed frontmatter when available`` () =
        let path = "docs/canon/files-frontmatter-target.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, path.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Files frontmatter target"
status: "active"
tags:
  - "operator"
related:
  - "src/Engine/Loop.fs"
team: "ops"
---
files() should expose this persisted frontmatter.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use filesDoc =
            JsonDocument.Parse(
                harness.EvalJson("files('files-frontmatter-target.md')")
            )

        let fileRow = getSingleArrayResult filesDoc.RootElement
        Assert.Equal("index", (getRequiredProperty "frontmatterSource" fileRow).GetString())

        let fileFrontmatter = getRequiredProperty "frontmatter" fileRow
        Assert.Equal("Files frontmatter target", (getRequiredProperty "title" fileFrontmatter).GetString())
        Assert.Equal("active", (getRequiredProperty "status" fileFrontmatter).GetString())
        Assert.Equal("ops", (getRequiredProperty "team" fileFrontmatter).GetString())

        let related = getRequiredProperty "related" fileFrontmatter
        Assert.Equal(JsonValueKind.Array, related.ValueKind)
        Assert.Equal("src/Engine/Loop.fs", related[0].GetString())

        use contextDoc = JsonDocument.Parse(harness.EvalJson(sprintf "context(%s)" (jsStringLiteral path)))
        Assert.Equal("index", (getRequiredProperty "frontmatterSource" contextDoc.RootElement).GetString())
        let contextFrontmatter = getRequiredProperty "frontmatter" contextDoc.RootElement
        Assert.Equal((getRequiredProperty "title" fileFrontmatter).GetString(), (getRequiredProperty "title" contextFrontmatter).GetString())
        Assert.Equal((getRequiredProperty "status" fileFrontmatter).GetString(), (getRequiredProperty "status" contextFrontmatter).GetString())
        Assert.Equal((getRequiredProperty "team" fileFrontmatter).GetString(), (getRequiredProperty "team" contextFrontmatter).GetString())
        let fileRelated = getRequiredProperty "related" fileFrontmatter
        let contextRelated = getRequiredProperty "related" contextFrontmatter
        Assert.Equal(fileRelated[0].GetString(), contextRelated[0].GetString())

    [<Fact>]
    let ``files keeps files without indexed frontmatter honest`` () =
        let path = "docs/canon/files-no-frontmatter.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, path.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
Plain markdown without frontmatter should still be listed honestly.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use filesDoc =
            JsonDocument.Parse(
                harness.EvalJson("files('files-no-frontmatter.md')")
            )

        let fileRow = getSingleArrayResult filesDoc.RootElement
        Assert.Equal("", (getRequiredProperty "frontmatterSource" fileRow).GetString())

        let fileFrontmatter = getRequiredProperty "frontmatter" fileRow
        Assert.Equal(JsonValueKind.Object, fileFrontmatter.ValueKind)
        Assert.Equal(0, fileFrontmatter.EnumerateObject() |> Seq.length)

    [<Fact>]
    let ``reverify errors when search verify lacks persisted deterministic cache and never falls back live`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "legacy-search.md"))
                """
---
title: "Legacy search verify"
status: "active"
source: "canon"
verify: "search('legacy deterministic verify', {file:'docs/canon/legacy-search.md', status:['active']}).length === 1"
verify_snapshot: "legacy-snapshot"
---
Legacy deterministic verify search doc.
"""

        use harness = VerifyHarness.Create(seedRepo)

        let requestsBeforeReverify = harness.EmbeddingRequestCount
        let reverifyQuery = "reverify({scope:['docs/canon/legacy-search.md']})"
        use reverifyDoc = JsonDocument.Parse(harness.EvalJson(reverifyQuery))

        Assert.Equal(requestsBeforeReverify, harness.EmbeddingRequestCount)

        let result = reverifyDoc.RootElement[0]
        Assert.Equal("error", (getRequiredProperty "outcome" result).GetString())
        Assert.Contains(
            "persisted deterministic query embeddings",
            (getRequiredProperty "error" result).GetString(),
            StringComparison.OrdinalIgnoreCase)

    [<Fact>]
    let ``reverify keeps novelty outside the verify sandbox`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "novelty-verify.md"))
                """
---
title: "Novelty verify"
status: "active"
source: "canon"
verify: "novelty('verify novelty', {status:['active']}).length === 0"
verify_snapshot: "legacy-snapshot"
---
Novelty should remain outside the deterministic verify sandbox.
"""

        use harness = VerifyHarness.Create(seedRepo)

        let requestsBeforeReverify = harness.EmbeddingRequestCount
        let reverifyQuery = "reverify({scope:['docs/canon/novelty-verify.md']})"
        use reverifyDoc = JsonDocument.Parse(harness.EvalJson(reverifyQuery))

        Assert.Equal(requestsBeforeReverify, harness.EmbeddingRequestCount)

        let result = reverifyDoc.RootElement[0]
        Assert.Equal("error", (getRequiredProperty "outcome" result).GetString())
        Assert.Contains("novelty", (getRequiredProperty "error" result).GetString(), StringComparison.OrdinalIgnoreCase)

    [<Fact>]
    let ``promote fails and leaves inbox pending when deterministic search capture is unavailable`` () =
        let inboxPath = "inbox/ops/2026-05-08T00-00-00Z-promote-search.md"
        let targetPath = "docs/canon/promoted-search.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, inboxPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Promote search"
status: "pending"
source: "ops"
cycle: "2026-05-08T00-00-00Z"
---
Promote deterministic search capture should fail closed.
"""

        use harness = VerifyHarness.Create(seedRepo)
        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let inboxRef = (getRequiredProperty "id" triageDoc.RootElement[0]).GetString()

        harness.StopEmbeddingServer()

        let verifyExpr = "search('promote deterministic search', {file:'docs/canon/promoted-search.md', status:['active']}).length === 1"
        let disposeQuery =
            sprintf "dispose(%s, {action:'promote', target:%s, verify:%s})"
                (jsStringLiteral inboxRef)
                (jsStringLiteral targetPath)
                (jsStringLiteral verifyExpr)
        use disposeDoc = JsonDocument.Parse(harness.EvalJson(disposeQuery))

        let error = (getRequiredProperty "error" disposeDoc.RootElement).GetString()
        Assert.Contains("cannot capture deterministic query embeddings", error, StringComparison.OrdinalIgnoreCase)
        Assert.False(harness.FileExists(targetPath))
        Assert.True(harness.FileExists(inboxPath))

    [<Fact>]
    let ``merge fails and leaves canonical target unchanged when deterministic search capture is unavailable`` () =
        let inboxPath = "inbox/ops/2026-05-08T00-00-00Z-merge-search.md"
        let targetPath = "docs/canon/merge-target.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, targetPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Merge target"
status: "active"
source: "canon"
verify: "search('merge deterministic search', {file:'docs/canon/merge-target.md', status:['active']}).length === 1"
verify_snapshot: "legacy-snapshot"
---
Canonical merge target body.
"""

            writeFile
                (Path.Combine(repoRoot, inboxPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Merge search"
status: "pending"
source: "ops"
cycle: "2026-05-08T00-00-00Z"
---
Merged corroboration should not land when capture fails.
"""

        use harness = VerifyHarness.Create(seedRepo)
        let baselineTarget = harness.ReadFile(targetPath)
        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let inboxRef = (getRequiredProperty "id" triageDoc.RootElement[0]).GetString()

        harness.StopEmbeddingServer()

        let disposeQuery =
            sprintf "dispose(%s, {action:'merge', target:%s})"
                (jsStringLiteral inboxRef)
                (jsStringLiteral targetPath)
        use disposeDoc = JsonDocument.Parse(harness.EvalJson(disposeQuery))

        let error = (getRequiredProperty "error" disposeDoc.RootElement).GetString()
        Assert.Contains("cannot capture deterministic query embeddings", error, StringComparison.OrdinalIgnoreCase)
        Assert.Equal(baselineTarget, harness.ReadFile(targetPath))
        Assert.True(harness.FileExists(inboxPath))

    [<Fact>]
    let ``merge refuses a genuinely held canonical target and leaves inbox pending`` () =
        let inboxPath = "inbox/ops/2026-05-08T00-00-00Z-merge-busy.md"
        let targetPath = "docs/canon/merge-busy-target.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, targetPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Merge busy target"
status: "active"
source: "canon"
---
Canonical merge target body.
"""

            writeFile
                (Path.Combine(repoRoot, inboxPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Merge busy"
status: "pending"
source: "ops"
cycle: "2026-05-08T00-00-00Z"
---
Merged corroboration should fail closed when the target is genuinely busy.
"""

        use harness = VerifyHarness.Create(seedRepo)
        let baselineTarget = harness.ReadFile(targetPath)
        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let inboxRef = (getRequiredProperty "id" triageDoc.RootElement[0]).GetString()
        let absoluteTargetPath =
            Path.Combine(harness.RepoRoot, targetPath.Replace("/", Path.DirectorySeparatorChar.ToString()))

        let error =
            use holdStream = new FileStream(absoluteTargetPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read)

            let disposeQuery =
                sprintf "dispose(%s, {action:'merge', target:%s})"
                    (jsStringLiteral inboxRef)
                    (jsStringLiteral targetPath)
            use disposeDoc = JsonDocument.Parse(harness.EvalJson(disposeQuery))
            (getRequiredProperty "error" disposeDoc.RootElement).GetString()

        Assert.Contains("changed concurrently", error, StringComparison.OrdinalIgnoreCase)
        Assert.Equal(baselineTarget, harness.ReadFile(targetPath))
        Assert.True(harness.FileExists(inboxPath))

        use triageAfterDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        Assert.Contains(
            triageAfterDoc.RootElement.EnumerateArray(),
            fun item -> String.Equals((getRequiredProperty "path" item).GetString(), inboxPath, StringComparison.Ordinal))

    [<Fact>]
    let ``supersede fails and leaves original active doc in place when deterministic search capture is unavailable`` () =
        let oldPath = "docs/canon/supersede-search.md"
        let newPath = "docs/canon/supersede-search-v2.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, oldPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Supersede search"
status: "active"
source: "canon"
---
Supersede deterministic search capture should fail closed.
"""

        use harness = VerifyHarness.Create(seedRepo)
        let searchQuery = "search('supersede deterministic search', {limit: 1, file: 'docs/canon/supersede-search.md', status:['active']})"
        use searchDoc = JsonDocument.Parse(harness.EvalJson(searchQuery))
        let refId = (getRequiredProperty "id" searchDoc.RootElement[0]).GetString()

        harness.StopEmbeddingServer()

        let verifyExpr = "search('supersede deterministic search', {file:'docs/canon/supersede-search-v2.md', status:['active']}).length === 1"
        let supersedeQuery =
            sprintf "supersede(%s, %s, {reason:'bounded correction', by:'ops', verify:%s})"
                (jsStringLiteral refId)
                (jsStringLiteral "Replacement content that should not land.\n")
                (jsStringLiteral verifyExpr)
        use supersedeDoc = JsonDocument.Parse(harness.EvalJson(supersedeQuery))

        let error = (getRequiredProperty "error" supersedeDoc.RootElement).GetString()
        Assert.Contains("cannot capture deterministic query embeddings", error, StringComparison.OrdinalIgnoreCase)
        Assert.False(harness.FileExists(newPath))
        Assert.True(harness.FileExists(oldPath))

        let contextQuery = sprintf "context(%s)" (jsStringLiteral oldPath)
        use contextDoc = JsonDocument.Parse(harness.EvalJson(contextQuery))
        Assert.Equal("active", (getRequiredProperty "status" contextDoc.RootElement).GetString())

    [<Fact>]
    let ``propose dry run accepts semicolon comma and plain claim text with the same intake path`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "placement-target.md"))
                """
---
title: "Placement target"
status: "active"
source: "canon"
---
Canonical placement target for propose parser coverage.
"""

        use harness = VerifyHarness.Create(seedRepo)

        let assertFiled (query: string) =
            use resultDoc = JsonDocument.Parse(harness.EvalJson(query))
            let row = getSingleArrayResult resultDoc.RootElement
            Assert.Equal("filed", (getRequiredProperty "status" row).GetString())
            Assert.True((getRequiredProperty "inboxPath" row).GetString().StartsWith("inbox/ops/", StringComparison.Ordinal))

        assertFiled "propose('Alpha should remain true; Beta should remain true; Gamma should remain true.', {team:'ops', cycle:'2026-05-09T00-00-00Z', dryRun:true, threshold:1.1})"
        assertFiled "propose('Alpha should remain true, Beta should remain true, Gamma should remain true.', {team:'ops', cycle:'2026-05-09T00-00-00Z', dryRun:true, threshold:1.1})"
        assertFiled "propose('Alpha should remain true because the intake path should stay stable across literal punctuation.', {team:'ops', cycle:'2026-05-09T00-00-00Z', dryRun:true, threshold:1.1})"

    [<Fact>]
    let ``explicit nested inbox root stays aligned across propose triage and archive paths`` () =
        let cycle = "2026-05-09T01-00-00Z"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "canon", "baseline.md"))
                """
---
title: "Baseline"
status: "active"
source: "canon"
concept: "nested-inbox"
---
Baseline canonical knowledge.
"""

        use harness = VerifyHarness.CreateWithConfig([| "knowledge-base" |], "knowledge-base/inbox", seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf
                        "propose('Nested inbox claims should file under the explicit inbox root.', {team:'ops', cycle:'%s', threshold:1.1, concept:'nested-inbox'})"
                        cycle
                )
            )

        let proposeRow = getSingleArrayResult proposeDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" proposeRow).GetString())
        let inboxPath = (getRequiredProperty "inboxPath" proposeRow).GetString()
        Assert.StartsWith(sprintf "knowledge-base/inbox/ops/%s-" cycle, inboxPath, StringComparison.Ordinal)
        Assert.True(harness.FileExists(inboxPath))

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let triageRow = getSingleArrayResult triageDoc.RootElement
        Assert.Equal(inboxPath, (getRequiredProperty "path" triageRow).GetString())
        let inboxRef = (getRequiredProperty "id" triageRow).GetString()

        use rejectDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "dispose(%s, {action:'reject', reason:'needs corroboration'})" (jsStringLiteral inboxRef)
                )
            )

        Assert.Equal("reject", (getRequiredProperty "action" rejectDoc.RootElement).GetString())
        let archivedPath = (getRequiredProperty "archivedPath" rejectDoc.RootElement).GetString()
        Assert.StartsWith(sprintf "knowledge-base/inbox/_processed/%s/" cycle, archivedPath, StringComparison.Ordinal)
        Assert.False(harness.FileExists(inboxPath))
        Assert.True(harness.FileExists(archivedPath))

    [<Fact>]
    let ``triage adopts pre-existing configured inbox docs without explicit pending status`` () =
        let cycle = "2026-05-09T02-00-00Z"
        let inboxPath = sprintf "knowledge-base/inbox/ops/%s-existing.md" cycle

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "canon", "baseline.md"))
                """
---
title: "Baseline"
status: "active"
source: "canon"
---
Baseline canonical knowledge.
"""

            writeFile
                (Path.Combine(repoRoot, inboxPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Existing pending adoption"
source: "ops"
cycle: "%s"
---
Pre-existing inbox content should default to pending on the triage seam.
""" cycle)

        use harness = VerifyHarness.CreateWithConfig([| "knowledge-base" |], "knowledge-base/inbox", seedRepo)

        let persistedMarkdown = harness.ReadFile(inboxPath)
        Assert.DoesNotContain("status:", persistedMarkdown, StringComparison.OrdinalIgnoreCase)

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let triageRow = getSingleArrayResult triageDoc.RootElement
        Assert.Equal(inboxPath, (getRequiredProperty "path" triageRow).GetString())
        Assert.Equal("Existing pending adoption", (getRequiredProperty "title" triageRow).GetString())
        let inboxRef = (getRequiredProperty "id" triageRow).GetString()

        use rejectDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "dispose(%s, {action:'reject', reason:'retrofit adoption proof'})" (jsStringLiteral inboxRef)
                )
            )

        Assert.Equal("reject", (getRequiredProperty "action" rejectDoc.RootElement).GetString())
        let archivedPath = (getRequiredProperty "archivedPath" rejectDoc.RootElement).GetString()
        Assert.StartsWith(sprintf "knowledge-base/inbox/_processed/%s/" cycle, archivedPath, StringComparison.Ordinal)
        Assert.False(harness.FileExists(inboxPath))
        Assert.True(harness.FileExists(archivedPath))

    [<Fact>]
    let ``triage excludes configured inbox docs with explicit non-pending status or disposition`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "inbox", "ops", "2026-05-09T03-00-00Z-active.md"))
                """
---
title: "Inbox but active"
status: "active"
source: "ops"
cycle: "2026-05-09T03-00-00Z"
---
Inbox-path docs with explicit non-pending status stay excluded from triage.
"""

            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "inbox", "ops", "2026-05-09T04-00-00Z-disposed.md"))
                """
---
title: "Already dispositioned"
source: "ops"
cycle: "2026-05-09T04-00-00Z"
disposition: "reject"
---
Dispositioned inbox docs stay excluded even when status defaults to pending by path.
"""

        use harness = VerifyHarness.CreateWithConfig([| "knowledge-base" |], "knowledge-base/inbox", seedRepo)

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        Assert.Equal(JsonValueKind.Array, triageDoc.RootElement.ValueKind)
        Assert.Equal(0, triageDoc.RootElement.GetArrayLength())

    [<Fact>]
    let ``catalog preserves distinct repo-relative buckets when directories share a basename`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "actor-engine", "overview.md"))
                """
---
title: "Actor Engine Overview"
status: "active"
source: "canon"
---
Canonical actor-engine guidance.
"""

            writeFile
                (Path.Combine(repoRoot, "inbox", "actor-engine", "2026-05-09T05-00-00Z-pending-claim.md"))
                """
---
title: "Pending Actor Engine Claim"
status: "pending"
source: "ops"
cycle: "2026-05-09T05-00-00Z"
---
Pending actor-engine inbox claim.
"""

        use harness = VerifyHarness.CreateWithConfig([| "actor-engine"; "inbox" |], "inbox", seedRepo)

        use catalogDoc =
            JsonDocument.Parse(
                harness.EvalJson("catalog({status:['active','pending']})")
            )

        let rows = catalogDoc.RootElement.EnumerateArray() |> Seq.toArray
        let directories = rows |> Array.map (getRequiredProperty "directory" >> fun value -> value.GetString())
        Assert.Equal<string[]>([| "actor-engine"; "inbox/actor-engine" |], directories)

        let actorEngineRow = rows[0]
        Assert.Equal(1, (getRequiredProperty "docs" actorEngineRow).GetInt32())
        Assert.Equal(1, (getRequiredProperty "sections" actorEngineRow).GetInt32())
        Assert.Equal("overview.md", (getRequiredProperty "fileList" actorEngineRow).GetString())

        let inboxRow = rows[1]
        Assert.Equal(1, (getRequiredProperty "docs" inboxRow).GetInt32())
        Assert.Equal(1, (getRequiredProperty "sections" inboxRow).GetInt32())
        Assert.Equal("2026-05-09T05-00-00Z-pending-claim.md", (getRequiredProperty "fileList" inboxRow).GetString())

    [<Fact>]
    let ``catalog keeps unique directories and root-level buckets honest`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "root-guide.md"))
                """
---
title: "Root Guide"
status: "active"
source: "canon"
---
Root-level catalog guidance.
"""

            writeFile
                (Path.Combine(repoRoot, "design", "architecture.md"))
                """
---
title: "Architecture Notes"
status: "active"
source: "canon"
---
Design directory guidance.
"""

        use harness = VerifyHarness.CreateWithConfig([| "."; "inbox" |], "inbox", seedRepo)

        use catalogDoc = JsonDocument.Parse(harness.EvalJson("catalog()"))

        let rows = catalogDoc.RootElement.EnumerateArray() |> Seq.toArray
        let directories = rows |> Array.map (getRequiredProperty "directory" >> fun value -> value.GetString())
        Assert.Equal<string[]>([| "design"; "root" |], directories)

        let designRow = rows[0]
        Assert.Equal("architecture.md", (getRequiredProperty "fileList" designRow).GetString())

        let rootRow = rows[1]
        Assert.Equal("root-guide.md", (getRequiredProperty "fileList" rootRow).GetString())

    [<Fact>]
    let ``rejected archived inbox docs do not make a later similar proposal known`` () =
        let firstCycle = "2026-05-09T05-00-00Z"
        let secondCycle = "2026-05-09T06-00-00Z"
        let proposalText = "Operators must capture rollback runbook ownership before the deploy window."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "canon", "stale-baseline.md"))
                """
---
title: "Stale baseline"
status: "stale"
source: "canon"
---
Historical baseline that should not participate in active proposal suppression.
"""

        use harness = VerifyHarness.CreateWithConfig([| "knowledge-base" |], "knowledge-base/inbox", seedRepo)

        let proposeQuery cycle =
            sprintf
                "propose(%s, {team:'ops', cycle:'%s'})"
                (jsStringLiteral proposalText)
                cycle

        use firstProposeDoc = JsonDocument.Parse(harness.EvalJson(proposeQuery firstCycle))
        let firstRow = getSingleArrayResult firstProposeDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" firstRow).GetString())
        let firstInboxPath = (getRequiredProperty "inboxPath" firstRow).GetString()
        Assert.StartsWith(sprintf "knowledge-base/inbox/ops/%s-" firstCycle, firstInboxPath, StringComparison.Ordinal)
        Assert.True(harness.FileExists(firstInboxPath))

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let triageRow = getSingleArrayResult triageDoc.RootElement
        Assert.Equal(firstInboxPath, (getRequiredProperty "path" triageRow).GetString())
        let inboxRef = (getRequiredProperty "id" triageRow).GetString()

        use rejectDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "dispose(%s, {action:'reject', reason:'novelty corpus residue repro'})" (jsStringLiteral inboxRef)
                )
            )

        Assert.Equal("reject", (getRequiredProperty "action" rejectDoc.RootElement).GetString())
        let archivedPath = (getRequiredProperty "archivedPath" rejectDoc.RootElement).GetString()
        Assert.StartsWith(sprintf "knowledge-base/inbox/_processed/%s/" firstCycle, archivedPath, StringComparison.Ordinal)
        Assert.False(harness.FileExists(firstInboxPath))
        Assert.True(harness.FileExists(archivedPath))

        use secondProposeDoc = JsonDocument.Parse(harness.EvalJson(proposeQuery secondCycle))
        let secondRow = getSingleArrayResult secondProposeDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" secondRow).GetString())
        let secondInboxPath = (getRequiredProperty "inboxPath" secondRow).GetString()
        Assert.StartsWith(sprintf "knowledge-base/inbox/ops/%s-" secondCycle, secondInboxPath, StringComparison.Ordinal)
        Assert.NotEqual<string>(firstInboxPath, secondInboxPath)
        Assert.True(harness.FileExists(secondInboxPath))

    [<Fact>]
    let ``active canonical near matches still return known on the proposal seam`` () =
        let canonicalPath = "knowledge-base/canon/rollback-runbook.md"
        let proposalText = "Operators must capture rollback runbook ownership before the deploy window."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, canonicalPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Rollback runbook ownership"
status: "active"
source: "canon"
---
%s
""" proposalText)

        use harness = VerifyHarness.CreateWithConfig([| "knowledge-base" |], "knowledge-base/inbox", seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-09T07-00-00Z'})" (jsStringLiteral proposalText)
                )
            )

        let row = getSingleArrayResult proposeDoc.RootElement
        Assert.Equal("known", (getRequiredProperty "status" row).GetString())
        Assert.Equal(canonicalPath, (getRequiredProperty "suggestedTarget" row).GetString())

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        Assert.Equal(JsonValueKind.Array, triageDoc.RootElement.ValueKind)
        Assert.Equal(0, triageDoc.RootElement.GetArrayLength())

    [<Fact>]
    let ``noveltyCorpus excludePaths scope propose and novelty without scoping search or placement`` () =
        let planningPath = "docs/_planning/rollback-planning-residue.md"
        let proposalText = "Rollback runbook ownership should stay explicit before the deploy window for operators."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, planningPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Rollback planning residue"
status: "active"
source: "ops"
---
%s
""" proposalText)

        let enableNoveltyCorpus (config: JsonObject) =
            config["noveltyCorpus"] <-
                JsonSerializer.SerializeToNode(
                    {| excludePaths = [| "docs/_planning/**" |] |}
                )

        use defaultHarness = VerifyHarness.Create(seedRepo)

        use defaultProposeDoc =
            JsonDocument.Parse(
                defaultHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-10T07-00-00Z'})" (jsStringLiteral proposalText)
                )
            )

        let defaultProposeRow = getSingleArrayResult defaultProposeDoc.RootElement
        Assert.Equal("known", (getRequiredProperty "status" defaultProposeRow).GetString())
        Assert.Equal(planningPath, (getRequiredProperty "suggestedTarget" defaultProposeRow).GetString())

        use defaultNoveltyDoc =
            JsonDocument.Parse(
                defaultHarness.EvalJson(
                    sprintf "novelty(%s, {threshold:0.9, status:['active']})" (jsStringLiteral proposalText)
                )
            )

        let defaultNoveltyRow = getSingleArrayResult defaultNoveltyDoc.RootElement
        Assert.Equal("covered", (getRequiredProperty "status" defaultNoveltyRow).GetString())
        Assert.Equal("rollback-planning-residue.md", (getRequiredProperty "nearDoc" defaultNoveltyRow).GetString())

        use scopedHarness =
            VerifyHarness.CreateWithConfigMutator([| "docs"; "inbox" |], "inbox", "warn", enableNoveltyCorpus, seedRepo)

        use scopedProposeDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-10T07-05-00Z', threshold:0.9})" (jsStringLiteral proposalText)
                )
            )

        let scopedProposeRow = getSingleArrayResult scopedProposeDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" scopedProposeRow).GetString())
        Assert.Equal("", (getRequiredProperty "suggestedTarget" scopedProposeRow).GetString())

        use scopedNoveltyDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "novelty(%s, {threshold:0.9, status:['active']})" (jsStringLiteral proposalText)
                )
            )

        let scopedNoveltyRow = getSingleArrayResult scopedNoveltyDoc.RootElement
        Assert.Equal("off-topic", (getRequiredProperty "status" scopedNoveltyRow).GetString())
        Assert.Equal("", (getRequiredProperty "nearDoc" scopedNoveltyRow).GetString())

        use searchDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "search('rollback runbook ownership', {limit:1, file:%s, status:['active']})" (jsStringLiteral planningPath)
                )
            )

        let searchRow = getSingleArrayResult searchDoc.RootElement
        Assert.Equal("rollback-planning-residue.md", (getRequiredProperty "file" searchRow).GetString())

        use placementDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "placement(%s, {limit:3, status:['active']})" (jsStringLiteral proposalText)
                )
            )

        Assert.Equal(JsonValueKind.Array, placementDoc.RootElement.ValueKind)
        Assert.Contains(
            placementDoc.RootElement.EnumerateArray() |> Seq.map (fun row -> (getRequiredProperty "file" row).GetString()),
            fun fileName -> String.Equals(fileName, "rollback-planning-residue.md", StringComparison.Ordinal)
        )

    [<Fact>]
    let ``noveltyCorpus excludeFrontmatter scalar rules exclude matching docs while canonical docs still suppress`` () =
        let scratchPath = "docs/0-scratch/operator-scratch-guidance.md"
        let canonicalPath = "docs/canon/rollback-runbook.md"
        let proposalText = "Rollback runbook ownership should stay explicit before the deploy window for operators."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, scratchPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Operator scratch guidance"
status: "active"
source: "ops"
tags: ["operators", "scratch", "ephemeral"]
---
%s
""" proposalText)

            writeFile
                (Path.Combine(repoRoot, canonicalPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Rollback runbook ownership"
status: "active"
source: "canon"
tags: ["operators", "canon"]
---
%s
""" proposalText)

        let enableNoveltyCorpus (config: JsonObject) =
            config["noveltyCorpus"] <-
                JsonSerializer.SerializeToNode(
                    {|
                        excludeFrontmatter = {| source = "ops" |}
                    |}
                )

        use defaultHarness = VerifyHarness.Create(seedRepo)

        use defaultProposeDoc =
            JsonDocument.Parse(
                defaultHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-10T08-00-00Z'})" (jsStringLiteral proposalText)
                )
            )

        let defaultProposeRow = getSingleArrayResult defaultProposeDoc.RootElement
        Assert.Equal("known", (getRequiredProperty "status" defaultProposeRow).GetString())
        Assert.Equal(scratchPath, (getRequiredProperty "suggestedTarget" defaultProposeRow).GetString())

        use defaultNoveltyDoc =
            JsonDocument.Parse(
                defaultHarness.EvalJson(
                    sprintf "novelty(%s, {threshold:0.9, status:['active']})" (jsStringLiteral proposalText)
                )
            )

        Assert.Equal("operator-scratch-guidance.md", (getRequiredProperty "nearDoc" (getSingleArrayResult defaultNoveltyDoc.RootElement)).GetString())

        use scopedHarness =
            VerifyHarness.CreateWithConfigMutator([| "docs"; "inbox" |], "inbox", "warn", enableNoveltyCorpus, seedRepo)

        use scopedProposeDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-10T08-05-00Z'})" (jsStringLiteral proposalText)
                )
            )

        let scopedProposeRow = getSingleArrayResult scopedProposeDoc.RootElement
        Assert.Equal("known", (getRequiredProperty "status" scopedProposeRow).GetString())
        Assert.Equal(canonicalPath, (getRequiredProperty "suggestedTarget" scopedProposeRow).GetString())

        use scopedNoveltyDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "novelty(%s, {threshold:0.9, status:['active']})" (jsStringLiteral proposalText)
                )
            )

        let scopedNoveltyRow = getSingleArrayResult scopedNoveltyDoc.RootElement
        Assert.Equal("covered", (getRequiredProperty "status" scopedNoveltyRow).GetString())
        Assert.Equal("rollback-runbook.md", (getRequiredProperty "nearDoc" scopedNoveltyRow).GetString())

    [<Fact>]
    let ``noveltyCorpus excludeFrontmatter list rules use containment rather than full list equality`` () =
        let scratchPath = "docs/0-scratch/operator-scratch-guidance.md"
        let canonicalPath = "docs/canon/rollback-runbook.md"
        let proposalText = "Rollback runbook ownership should stay explicit before the deploy window for operators."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, scratchPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Operator scratch guidance"
status: "active"
source: "canon"
tags: ["operators", "scratch", "ephemeral"]
---
%s
""" proposalText)

            writeFile
                (Path.Combine(repoRoot, canonicalPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                (sprintf """
---
title: "Rollback runbook ownership"
status: "active"
source: "canon"
tags: ["operators", "canon"]
---
%s
""" proposalText)

        let enableNoveltyCorpus (config: JsonObject) =
            config["noveltyCorpus"] <-
                JsonSerializer.SerializeToNode(
                    {|
                        excludeFrontmatter = {| tags = [| "scratch"; "operators" |] |}
                    |}
                )

        use defaultHarness = VerifyHarness.Create(seedRepo)

        use defaultProposeDoc =
            JsonDocument.Parse(
                defaultHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-10T08-10-00Z'})" (jsStringLiteral proposalText)
                )
            )

        let defaultProposeRow = getSingleArrayResult defaultProposeDoc.RootElement
        Assert.Equal("known", (getRequiredProperty "status" defaultProposeRow).GetString())
        Assert.Equal(scratchPath, (getRequiredProperty "suggestedTarget" defaultProposeRow).GetString())

        use scopedHarness =
            VerifyHarness.CreateWithConfigMutator([| "docs"; "inbox" |], "inbox", "warn", enableNoveltyCorpus, seedRepo)

        use scopedProposeDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-10T08-15-00Z'})" (jsStringLiteral proposalText)
                )
            )

        let scopedProposeRow = getSingleArrayResult scopedProposeDoc.RootElement
        Assert.Equal("known", (getRequiredProperty "status" scopedProposeRow).GetString())
        Assert.Equal(canonicalPath, (getRequiredProperty "suggestedTarget" scopedProposeRow).GetString())

        use scopedNoveltyDoc =
            JsonDocument.Parse(
                scopedHarness.EvalJson(
                    sprintf "novelty(%s, {threshold:0.9, status:['active']})" (jsStringLiteral proposalText)
                )
            )

        let scopedNoveltyRow = getSingleArrayResult scopedNoveltyDoc.RootElement
        Assert.Equal("covered", (getRequiredProperty "status" scopedNoveltyRow).GetString())
        Assert.Equal("rollback-runbook.md", (getRequiredProperty "nearDoc" scopedNoveltyRow).GetString())

    [<Fact>]
    let ``propose files terse valid short claims through the propose-only gate`` () =
        let cycle = "2026-05-09T08-00-00Z"
        let proposalText = "Deploys must keep rollback ownership."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "stale-baseline.md"))
                """
---
title: "Stale baseline"
status: "stale"
source: "canon"
---
Historical baseline that should not suppress new proposals.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'%s'})" (jsStringLiteral proposalText) cycle
                )
            )

        let row = getSingleArrayResult proposeDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" row).GetString())
        let inboxPath = (getRequiredProperty "inboxPath" row).GetString()
        Assert.StartsWith(sprintf "inbox/ops/%s-" cycle, inboxPath, StringComparison.Ordinal)
        Assert.True(harness.FileExists(inboxPath))

    [<Fact>]
    let ``propose still blocks terse hedged musings`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "stale-baseline.md"))
                """
---
title: "Stale baseline"
status: "stale"
source: "canon"
---
Historical baseline that should not suppress new proposals.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-09T09-00-00Z'})" (jsStringLiteral "This might be deploy noise.")
                )
            )

        let row = getSingleArrayResult proposeDoc.RootElement
        let status = (getRequiredProperty "status" row).GetString()
        Assert.True(String.Equals(status, "blocked", StringComparison.Ordinal), proposeDoc.RootElement.GetRawText())

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        Assert.Equal(JsonValueKind.Array, triageDoc.RootElement.ValueKind)
        Assert.Equal(0, triageDoc.RootElement.GetArrayLength())

    [<Fact>]
    let ``propose synthesizes suggestedVerify from later placement candidates while keeping the top suggested target`` () =
        let proposalText = "Render fallback should emit diagnostic telemetry when wide-FOV clipping activates."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "render", "fallback-routing.md"))
                """
---
title: "Render fallback routing"
status: "active"
source: "canon"
---
Render fallback should emit diagnostic telemetry when wide-FOV clipping activates during routing.
"""

            writeFile
                (Path.Combine(repoRoot, "docs", "render", "fallback-diagnostics.md"))
                """
---
title: "Render fallback diagnostics"
status: "active"
source: "canon"
related:
  - "src/Render/Culling.fs"
---
Render fallback telemetry covers wide-FOV clipping diagnostics.
"""

        let assertSuggestedVerify (row: JsonElement) =
            Assert.Equal("docs/render/fallback-routing.md", (getRequiredProperty "suggestedTarget" row).GetString())
            let warnings =
                (getRequiredProperty "warnings" row).EnumerateArray()
                |> Seq.map (fun item -> item.GetString())
                |> Seq.toArray
            Assert.Contains("no_verify", warnings)
            Assert.Equal(
                "grep('Render fallback should emit diagnostic telemetry', {file:'src/Render/Culling.fs'}).length > 0",
                (getRequiredProperty "suggestedVerify" row).GetString())

        use warnHarness = VerifyHarness.Create(seedRepo)
        warnHarness.StopEmbeddingServer()

        use warnDoc =
            JsonDocument.Parse(
                warnHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-09T11-00-00Z', concept:'render-fallback', observable:'Render debugger shows the fallback trigger.', forbids:'Silent corner clipping.', threshold:1.1})"
                        (jsStringLiteral proposalText)
                )
            )

        let warnRow = getSingleArrayResult warnDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" warnRow).GetString())
        assertSuggestedVerify warnRow

        use errorHarness = VerifyHarness.CreateWithRequireFieldsMode([| "docs"; "inbox" |], "inbox", "error", seedRepo)
        errorHarness.StopEmbeddingServer()

        use blockedDoc =
            JsonDocument.Parse(
                errorHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-09T11-01-00Z', concept:'render-fallback', observable:'Render debugger shows the fallback trigger.', forbids:'Silent corner clipping.', threshold:1.1})"
                        (jsStringLiteral proposalText)
                )
            )

        let blockedRow = getSingleArrayResult blockedDoc.RootElement
        Assert.Equal("blocked", (getRequiredProperty "status" blockedRow).GetString())
        assertSuggestedVerify blockedRow

        use dryRunDoc =
            JsonDocument.Parse(
                errorHarness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-09T11-02-00Z', concept:'render-fallback', observable:'Render debugger shows the fallback trigger.', forbids:'Silent corner clipping.', threshold:1.1, dryRun:true})"
                        (jsStringLiteral proposalText)
                )
            )

        let dryRunRow = getSingleArrayResult dryRunDoc.RootElement
        Assert.Equal("blocked", (getRequiredProperty "status" dryRunRow).GetString())
        assertSuggestedVerify dryRunRow

    [<Fact>]
    let ``propose leaves suggestedVerify absent when ranked placement candidates have no related evidence`` () =
        let proposalText = "Render fallback should emit diagnostic telemetry when wide-FOV clipping activates."

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "render", "fallback-routing.md"))
                """
---
title: "Render fallback routing"
status: "active"
source: "canon"
---
Render fallback should emit diagnostic telemetry when wide-FOV clipping activates during routing.
"""

            writeFile
                (Path.Combine(repoRoot, "docs", "render", "fallback-overview.md"))
                """
---
title: "Render fallback overview"
status: "active"
source: "canon"
---
Render fallback telemetry overview for wide-FOV clipping.
"""

        use harness = VerifyHarness.Create(seedRepo)
        harness.StopEmbeddingServer()

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:'2026-05-09T11-03-00Z', concept:'render-fallback', observable:'Render debugger shows the fallback trigger.', forbids:'Silent corner clipping.', threshold:1.1})"
                        (jsStringLiteral proposalText)
                )
            )

        let row = getSingleArrayResult proposeDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" row).GetString())
        Assert.Equal("docs/render/fallback-routing.md", (getRequiredProperty "suggestedTarget" row).GetString())
        Assert.True(tryGetProperty "suggestedVerify" row |> Option.isNone, proposeDoc.RootElement.GetRawText())

    [<Fact>]
    let ``propose accepts integer cycle ids across triage frontmatter and archive paths`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "stale-baseline.md"))
                """
---
title: "Stale baseline"
status: "stale"
source: "canon"
---
Historical baseline that should not suppress new proposals.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use proposeThousandDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    "propose('Numeric cycle thousand claim should file cleanly.', {team:'ops', cycle:1000, threshold:1.1})"
                )
            )

        Assert.Equal("filed", (getRequiredProperty "status" (getSingleArrayResult proposeThousandDoc.RootElement)).GetString())

        use proposeNineDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    "propose('Numeric cycle nine-hundred-ninety-nine claim should file cleanly.', {team:'ops', cycle:999, threshold:1.1})"
                )
            )

        let proposeNineRow = getSingleArrayResult proposeNineDoc.RootElement
        Assert.Equal("filed", (getRequiredProperty "status" proposeNineRow).GetString())
        let inboxPath = (getRequiredProperty "inboxPath" proposeNineRow).GetString()
        Assert.StartsWith("inbox/ops/999-", inboxPath, StringComparison.Ordinal)

        use contextDoc = JsonDocument.Parse(harness.EvalJson(sprintf "context(%s)" (jsStringLiteral inboxPath)))
        let frontmatter = getRequiredProperty "frontmatter" contextDoc.RootElement
        Assert.Equal("999", (getRequiredProperty "cycle" frontmatter).GetString())

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        Assert.Equal(2, triageDoc.RootElement.GetArrayLength())
        let firstRow = triageDoc.RootElement[0]
        let secondRow = triageDoc.RootElement[1]
        Assert.Equal("999", (getRequiredProperty "cycle" firstRow).GetString())
        Assert.Equal("", (getRequiredProperty "age" firstRow).GetString())
        Assert.Equal("1000", (getRequiredProperty "cycle" secondRow).GetString())

        use beforeDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops', before:999})"))
        let beforeRow = getSingleArrayResult beforeDoc.RootElement
        Assert.Equal("999", (getRequiredProperty "cycle" beforeRow).GetString())
        let inboxRef = (getRequiredProperty "id" beforeRow).GetString()

        use rejectDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "dispose(%s, {action:'reject', reason:'integer cycle archive proof'})" (jsStringLiteral inboxRef)
                )
            )

        Assert.Equal("reject", (getRequiredProperty "action" rejectDoc.RootElement).GetString())
        Assert.StartsWith("inbox/_processed/999/", (getRequiredProperty "archivedPath" rejectDoc.RootElement).GetString(), StringComparison.Ordinal)

    [<Fact>]
    let ``merge keeps integer cycle labels honest`` () =
        let targetPath = "docs/canon/integer-cycle-merge-target.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, targetPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Integer cycle merge target"
status: "active"
source: "canon"
---
Baseline canonical merge target.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    "propose('Integer cycle merge corroboration should preserve its label.', {team:'ops', cycle:999, threshold:1.1})"
                )
            )

        Assert.Equal("filed", (getRequiredProperty "status" (getSingleArrayResult proposeDoc.RootElement)).GetString())

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        let inboxRef = (getRequiredProperty "id" (getSingleArrayResult triageDoc.RootElement)).GetString()

        use mergeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "dispose(%s, {action:'merge', target:%s})" (jsStringLiteral inboxRef) (jsStringLiteral targetPath)
                )
            )

        Assert.Equal("merge", (getRequiredProperty "action" mergeDoc.RootElement).GetString())
        Assert.StartsWith("inbox/_processed/999/", (getRequiredProperty "archivedPath" mergeDoc.RootElement).GetString(), StringComparison.Ordinal)
        Assert.Contains("### Corroboration (ops, cycle 999)", harness.ReadFile(targetPath), StringComparison.Ordinal)

    [<Fact>]
    let ``triage mixed cycle repos group utc rows before integer rows`` () =
        let utcPath = "inbox/ops/2026-05-09T10-00-00Z-utc-claim.md"
        let integerPath = "inbox/ops/999-integer-claim.md"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, utcPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "UTC claim"
status: "pending"
source: "ops"
cycle: "2026-05-09T10-00-00Z"
---
UTC cycle claim.
"""

            writeFile
                (Path.Combine(repoRoot, integerPath.Replace("/", Path.DirectorySeparatorChar.ToString())))
                """
---
title: "Integer claim"
status: "pending"
source: "ops"
cycle: "999"
---
Integer cycle claim.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use triageDoc = JsonDocument.Parse(harness.EvalJson("triage({team:'ops'})"))
        Assert.Equal(2, triageDoc.RootElement.GetArrayLength())
        let firstRow = triageDoc.RootElement[0]
        let secondRow = triageDoc.RootElement[1]
        Assert.Equal(utcPath, (getRequiredProperty "path" firstRow).GetString())
        Assert.Equal("2026-05-09T10-00-00Z", (getRequiredProperty "cycle" firstRow).GetString())
        Assert.False(String.IsNullOrWhiteSpace((getRequiredProperty "age" firstRow).GetString()))
        Assert.Equal(integerPath, (getRequiredProperty "path" secondRow).GetString())
        Assert.Equal("999", (getRequiredProperty "cycle" secondRow).GetString())
        Assert.Equal("", (getRequiredProperty "age" secondRow).GetString())

    [<Fact>]
    let ``triage before rejects mixed cycle repos instead of silently dropping one form`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "inbox", "ops", "2026-05-09T10-00-00Z-utc-claim.md"))
                """
---
title: "UTC claim"
status: "pending"
source: "ops"
cycle: "2026-05-09T10-00-00Z"
---
UTC cycle claim.
"""

            writeFile
                (Path.Combine(repoRoot, "inbox", "ops", "999-integer-claim.md"))
                """
---
title: "Integer claim"
status: "pending"
source: "ops"
cycle: "999"
---
Integer cycle claim.
"""

        use harness = VerifyHarness.Create(seedRepo)

        let assertMixedBeforeError (query: string) =
            use triageDoc = JsonDocument.Parse(harness.EvalJson(query))
            let row = getSingleArrayResult triageDoc.RootElement
            Assert.Contains(
                "cannot page mixed UTC-string and integer cycle inbox items honestly",
                (getRequiredProperty "error" row).GetString(),
                StringComparison.OrdinalIgnoreCase)

        assertMixedBeforeError "triage({team:'ops', before:999})"
        assertMixedBeforeError "triage({team:'ops', before:'2026-05-09T10-00-00Z'})"

    [<Fact>]
    let ``utc cycle behavior stays unchanged`` () =
        let cycle = "2026-05-09T10-00-00Z"

        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "stale-baseline.md"))
                """
---
title: "Stale baseline"
status: "stale"
source: "canon"
---
Historical baseline that should not suppress new proposals.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "propose(%s, {team:'ops', cycle:%s, threshold:1.1})" (jsStringLiteral "UTC cycle claim should still file.") (jsStringLiteral cycle)
                )
            )

        Assert.Equal("filed", (getRequiredProperty "status" (getSingleArrayResult proposeDoc.RootElement)).GetString())

        use beforeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    sprintf "triage({team:'ops', before:%s})" (jsStringLiteral cycle)
                )
            )

        let beforeRow = getSingleArrayResult beforeDoc.RootElement
        Assert.Equal(cycle, (getRequiredProperty "cycle" beforeRow).GetString())
        Assert.False(String.IsNullOrWhiteSpace((getRequiredProperty "age" beforeRow).GetString()))

        use excludedDoc =
            JsonDocument.Parse(
                harness.EvalJson("triage({team:'ops', before:'2026-05-09T09-59-59Z'})")
            )

        Assert.Equal(JsonValueKind.Array, excludedDoc.RootElement.ValueKind)
        Assert.Equal(0, excludedDoc.RootElement.GetArrayLength())

    [<Fact>]
    let ``propose still rejects non-integer numeric cycle values`` () =
        let seedRepo repoRoot =
            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "stale-baseline.md"))
                """
---
title: "Stale baseline"
status: "stale"
source: "canon"
---
Historical baseline that should not suppress new proposals.
"""

        use harness = VerifyHarness.Create(seedRepo)

        use proposeDoc =
            JsonDocument.Parse(
                harness.EvalJson(
                    "propose('Non-integer cycle values should stay rejected.', {team:'ops', cycle:1.5})"
                )
            )

        let row = getSingleArrayResult proposeDoc.RootElement
        Assert.Equal("blocked", (getRequiredProperty "status" row).GetString())
        Assert.Contains("non-negative integer id", (getRequiredProperty "error" row).GetString(), StringComparison.OrdinalIgnoreCase)

    [<Fact>]
    let ``bare inboxDir fails closed when a configured doc dir already contains a nested inbox root`` () =
        use server = EmbeddingServer.Start()
        let repoRoot = Path.Combine(Path.GetTempPath(), sprintf "ks-inbox-root-ambiguity-%s" (Guid.NewGuid().ToString("N")))
        Directory.CreateDirectory(repoRoot) |> ignore

        try
            writeConfig repoRoot server.EmbeddingUrl [| "knowledge-base" |] "inbox" "warn"

            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "canon", "baseline.md"))
                """
---
title: "Baseline"
status: "active"
source: "canon"
---
Baseline canonical knowledge.
"""

            writeFile
                (Path.Combine(repoRoot, "knowledge-base", "inbox", "ops", "2026-05-09T01-00-00Z-existing.md"))
                """
---
title: "Existing pending"
status: "pending"
source: "ops"
cycle: "2026-05-09T01-00-00Z"
---
Existing nested inbox content should force explicit inboxDir configuration.
"""

            let cfg = Config.load repoRoot

            match IndexingWorkflow.rebuild cfg with
            | Ok _ -> failwith "Expected ambiguous nested inbox layout to fail closed"
            | Error error ->
                Assert.Contains("inboxDir 'inbox' is ambiguous", error, StringComparison.OrdinalIgnoreCase)
                Assert.Contains("knowledge-base/inbox", error, StringComparison.OrdinalIgnoreCase)
                Assert.Contains("Set inboxDir", error, StringComparison.OrdinalIgnoreCase)
        finally
            try
                Directory.Delete(repoRoot, true)
            with _ ->
                ()
