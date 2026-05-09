namespace AITeam.KnowledgeSight.Tests

open System
open System.Collections.Concurrent
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Xunit
open AITeam.KnowledgeSight

module ConflictAlignmentTests =

    let private duplicateBody =
        "Knowledge contradiction candidate: frustum culling should fall back to axis aligned bounds when camera FOV exceeds 120 because the planar test misses corner volumes.\n"

    let private writeFile (filePath: string) (content: string) =
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)) |> ignore
        File.WriteAllText(filePath, content.TrimStart(), Encoding.UTF8)

    let private writeConfig (repoRoot: string) (embeddingUrl: string) (completionUrl: string) =
        let config =
            {|
                docDirs = [| "docs"; "inbox" |]
                archiveProcessed = true
                embeddingUrl = embeddingUrl
                completionUrl = completionUrl
                conflictJudgeModel = "test-conflicts"
            |}

        let json = JsonSerializer.Serialize(config, JsonSerializerOptions(WriteIndented = true))
        File.WriteAllText(Path.Combine(repoRoot, "knowledge-sight.json"), json)

    let private requestHasPair (body: string) (leftPath: string) (rightPath: string) =
        body.Contains(leftPath, StringComparison.Ordinal) && body.Contains(rightPath, StringComparison.Ordinal)

    let private mixedVerdictForBody (body: string) =
        if requestHasPair body "docs/canon/active-alpha.md" "docs/canon/active-beta.md" then
            "duplicate", "These two docs restate the same operator claim."
        elif requestHasPair body "docs/canon/active-alpha.md" "inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md" then
            "conflict", "The inbox claim directly disagrees with the active alpha guidance."
        elif requestHasPair body "docs/canon/active-alpha.md" "inbox/ops/2026-05-07T00-01-00Z-pending-delta.md" then
            "compatible", "These docs can both be true in the same operator workflow."
        elif requestHasPair body "docs/canon/active-beta.md" "inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md" then
            "duplicate", "These two docs restate the same operator claim."
        elif requestHasPair body "docs/canon/active-beta.md" "inbox/ops/2026-05-07T00-01-00Z-pending-delta.md" then
            "conflict", "The pending delta wording contradicts the active beta claim."
        elif requestHasPair body "inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md" "inbox/ops/2026-05-07T00-01-00Z-pending-delta.md" then
            "duplicate", "These inbox docs restate the same operator claim."
        else
            invalidOp "Unrecognized judged pair payload in mixed completion route."

    let private buildCompletionResponse (verdict: string) (explanation: string) =
        JsonSerializer.Serialize(
            {| choices =
                [|
                    {| message =
                        {| content =
                            JsonSerializer.Serialize(
                                {| verdict = verdict
                                   explanation = explanation |}) |} |}
                |] |})

    let private findFreePort () =
        let listener = new TcpListener(IPAddress.Loopback, 0)
        listener.Start()
        let port = (listener.LocalEndpoint :?> IPEndPoint).Port
        listener.Stop()
        port

    type private CompletionRequest = {
        Url: string
        Body: string
    }

    type private CompletionServer private (listener: HttpListener, requests: ConcurrentQueue<CompletionRequest>, loopTask: Task, port: int) =
        member _.EmbeddingUrl = sprintf "http://127.0.0.1:%d/v1/embeddings" port
        member _.CompletionUrl = sprintf "http://127.0.0.1:%d/mixed" port

        member _.MixedRequestCount =
            requests.ToArray()
            |> Array.filter (fun request -> String.Equals(request.Url, "/mixed", StringComparison.Ordinal))
            |> Array.length

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

            let requests = ConcurrentQueue<CompletionRequest>()

            let loopTask =
                Task.Run(fun () ->
                    task {
                        try
                            while listener.IsListening do
                                let! context = listener.GetContextAsync()
                                use reader = new StreamReader(context.Request.InputStream, context.Request.ContentEncoding)
                                let! body = reader.ReadToEndAsync()
                                requests.Enqueue({ Url = context.Request.RawUrl; Body = body })

                                try
                                    if String.Equals(context.Request.RawUrl, "/mixed", StringComparison.Ordinal) then
                                        let verdict, explanation = mixedVerdictForBody body
                                        let payload = buildCompletionResponse verdict explanation
                                        let bytes = Encoding.UTF8.GetBytes(payload)
                                        context.Response.StatusCode <- 200
                                        context.Response.ContentType <- "application/json"
                                        context.Response.ContentLength64 <- int64 bytes.Length
                                        do! context.Response.OutputStream.WriteAsync(bytes, 0, bytes.Length)
                                    elif String.Equals(context.Request.RawUrl, "/v1/embeddings", StringComparison.Ordinal) then
                                        use doc = JsonDocument.Parse(body)
                                        let input =
                                            match doc.RootElement.TryGetProperty("input") with
                                            | true, value -> value
                                            | _ -> failwith "embedding request missing input"
                                        let embeddings =
                                            input.EnumerateArray()
                                            |> Seq.map (fun _ ->
                                                {| embedding = [| 1.0f; 0.0f; 0.0f |] |})
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

            new CompletionServer(listener, requests, loopTask, port)

    type private ConflictHarness private (repoRoot: string, server: CompletionServer, engine: Jint.Engine) =
        member _.EvalJson (query: string) = QueryEngine.evalJson engine query
        member _.MixedRequestCount = server.MixedRequestCount

        interface IDisposable with
            member _.Dispose() =
                (server :> IDisposable).Dispose()
                try
                    Directory.Delete(repoRoot, true)
                with _ ->
                    ()

        static member Create() =
            let server = CompletionServer.Start()
            let repoRoot = Path.Combine(Path.GetTempPath(), sprintf "ks-conflicts-tests-%s" (Guid.NewGuid().ToString("N")))
            Directory.CreateDirectory(repoRoot) |> ignore
            writeConfig repoRoot server.EmbeddingUrl server.CompletionUrl

            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "active-alpha.md"))
                $"""
---
title: "Active alpha"
status: "active"
source: "canon"
---
{duplicateBody}
"""

            writeFile
                (Path.Combine(repoRoot, "docs", "canon", "active-beta.md"))
                $"""
---
title: "Active beta"
status: "active"
source: "canon"
---
{duplicateBody}
"""

            writeFile
                (Path.Combine(repoRoot, "inbox", "ops", "2026-05-07T00-00-00Z-pending-gamma.md"))
                $"""
---
title: "Pending gamma"
status: "pending"
source: "ops"
cycle: "2026-05-07T00-00-00Z"
---
{duplicateBody}
"""

            writeFile
                (Path.Combine(repoRoot, "inbox", "ops", "2026-05-07T00-01-00Z-pending-delta.md"))
                $"""
---
title: "Pending delta"
status: "pending"
source: "ops"
cycle: "2026-05-07T00-01-00Z"
---
{duplicateBody}
"""

            let cfg = Config.load repoRoot

            match IndexingWorkflow.rebuild cfg with
            | Error message ->
                (server :> IDisposable).Dispose()
                failwithf "Index build failed in test harness: %s" message
            | Ok (index, chunks) ->
                let engine = QueryEngine.create cfg index chunks
                new ConflictHarness(repoRoot, server, engine)

    let private getRequiredProperty (name: string) (element: JsonElement) =
        match element.TryGetProperty(name) with
        | true, value -> value
        | _ -> failwithf "Missing property '%s'" name

    let private pairSignature (pairElement: JsonElement) =
        (getRequiredProperty "items" pairElement).EnumerateArray()
        |> Seq.map (fun item -> getRequiredProperty "path" item |> fun path -> path.GetString())
        |> Seq.sort
        |> String.concat "|"

    let private getSingleError (element: JsonElement) =
        Assert.Equal(JsonValueKind.Array, element.ValueKind)
        Assert.Equal(1, element.GetArrayLength())
        (getRequiredProperty "error" element[0]).GetString()

    [<Fact>]
    let ``help advertises a compact conflicts core surface`` () =
        let originalOut = Console.Out
        use writer = new StringWriter()

        try
            Console.SetOut(writer)
            Program.printUsage()
        finally
            Console.SetOut(originalOut)

        let output = writer.ToString()
        Assert.Contains("conflicts({scope, threshold})", output)
        Assert.Contains("{pairs:true, judge:true}", output)
        Assert.Contains("compatibility-only", output)
        Assert.DoesNotContain(
            "conflicts({scope, threshold, pairs, judge, verdicts, rollup, profile, profiles, duplicatesOnly, hasConflict, mixedVerdicts, compatibleOnly, conflictOnly, noConflict})",
            output)

    [<Fact>]
    let ``advanced conflicts filters remain supported on the compatibility surface`` () =
        use harness = ConflictHarness.Create()
        use doc =
            JsonDocument.Parse(
                harness.EvalJson(
                    "conflicts({scope:['docs/canon/active-alpha.md','inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md'], pairs:true, judge:true, rollup:true, conflictOnly:true})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(1, results.GetArrayLength())

        let candidate = results[0]
        let pairs = getRequiredProperty "pairs" candidate
        let rollup = getRequiredProperty "rollup" candidate
        let verdictCounts = getRequiredProperty "verdictCounts" rollup

        let signatures =
            pairs.EnumerateArray()
            |> Seq.map pairSignature
            |> Seq.toArray

        Assert.Equal<string[]>(
            [|
                "docs/canon/active-alpha.md|inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md"
            |],
            signatures)

        Assert.Equal(1, pairs.GetArrayLength())
        Assert.Equal(1, (getRequiredProperty "judgedPairs" rollup).GetInt32())
        Assert.Equal(1, (getRequiredProperty "conflict" verdictCounts).GetInt32())
        Assert.Equal(0, (getRequiredProperty "duplicate" verdictCounts).GetInt32())
        Assert.Equal(0, (getRequiredProperty "compatible" verdictCounts).GetInt32())
        Assert.True((getRequiredProperty "hasConflict" rollup).GetBoolean())
        Assert.True((getRequiredProperty "conflictOnly" rollup).GetBoolean())
        Assert.Equal("conflictOnly", (getRequiredProperty "profile" rollup).GetString())
        Assert.Equal(1, harness.MixedRequestCount)

    [<Fact>]
    let ``bare judged rollup keeps the full visible pair set and rollup aligned`` () =
        use harness = ConflictHarness.Create()
        use doc = JsonDocument.Parse(harness.EvalJson("conflicts({pairs:true, judge:true, rollup:true})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(1, results.GetArrayLength())

        let candidate = results[0]
        let pairs = getRequiredProperty "pairs" candidate
        let rollup = getRequiredProperty "rollup" candidate
        let verdictCounts = getRequiredProperty "verdictCounts" rollup

        Assert.Equal(6, pairs.GetArrayLength())
        Assert.Equal(6, (getRequiredProperty "judgedPairs" rollup).GetInt32())
        Assert.Equal(2, (getRequiredProperty "conflict" verdictCounts).GetInt32())
        Assert.Equal(3, (getRequiredProperty "duplicate" verdictCounts).GetInt32())
        Assert.Equal(1, (getRequiredProperty "compatible" verdictCounts).GetInt32())
        Assert.True((getRequiredProperty "mixedVerdicts" rollup).GetBoolean())
        Assert.True((getRequiredProperty "hasConflict" rollup).GetBoolean())
        Assert.Equal("mixedWithConflict", (getRequiredProperty "profile" rollup).GetString())
        Assert.Equal(6, harness.MixedRequestCount)

    [<Fact>]
    let ``hasConflict verdict filtering keeps full-set-first candidate retention while rollup matches visible pairs`` () =
        use harness = ConflictHarness.Create()
        use doc = JsonDocument.Parse(harness.EvalJson("conflicts({pairs:true, judge:true, rollup:true, hasConflict:true, verdicts:['duplicate']})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(1, results.GetArrayLength())

        let candidate = results[0]
        let pairs = getRequiredProperty "pairs" candidate
        let rollup = getRequiredProperty "rollup" candidate
        let verdictCounts = getRequiredProperty "verdictCounts" rollup

        let signatures =
            pairs.EnumerateArray()
            |> Seq.map pairSignature
            |> Seq.toArray

        Assert.Equal<string[]>(
            [|
                "docs/canon/active-alpha.md|docs/canon/active-beta.md"
                "docs/canon/active-beta.md|inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md"
                "inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md|inbox/ops/2026-05-07T00-01-00Z-pending-delta.md"
            |],
            signatures)

        Assert.Equal(3, pairs.GetArrayLength())
        Assert.Equal(3, (getRequiredProperty "judgedPairs" rollup).GetInt32())
        Assert.Equal(0, (getRequiredProperty "conflict" verdictCounts).GetInt32())
        Assert.Equal(3, (getRequiredProperty "duplicate" verdictCounts).GetInt32())
        Assert.Equal(0, (getRequiredProperty "compatible" verdictCounts).GetInt32())
        Assert.False((getRequiredProperty "hasConflict" rollup).GetBoolean())
        Assert.True((getRequiredProperty "noConflict" rollup).GetBoolean())
        Assert.True((getRequiredProperty "duplicateOnly" rollup).GetBoolean())
        Assert.Equal("duplicateOnly", (getRequiredProperty "profile" rollup).GetString())
        Assert.Equal(6, harness.MixedRequestCount)

    [<Fact>]
    let ``empty visible pair sets still omit the candidate after filtering`` () =
        use harness = ConflictHarness.Create()
        use doc = JsonDocument.Parse(harness.EvalJson("conflicts({scope:'inbox/ops', pairs:true, judge:true, verdicts:['compatible'], rollup:true})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(0, results.GetArrayLength())
        Assert.Equal(1, harness.MixedRequestCount)

    [<Fact>]
    let ``profiles filter retains exact visible profile matches on the judged rollup seam`` () =
        use harness = ConflictHarness.Create()
        use doc = JsonDocument.Parse(harness.EvalJson("conflicts({pairs:true, judge:true, rollup:true, profiles:['duplicateOnly','mixedWithConflict']})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(1, results.GetArrayLength())

        let candidate = results[0]
        let rollup = getRequiredProperty "rollup" candidate
        let pairs = getRequiredProperty "pairs" candidate

        Assert.Equal("mixedWithConflict", (getRequiredProperty "profile" rollup).GetString())
        Assert.Equal(6, pairs.GetArrayLength())
        Assert.Equal(6, harness.MixedRequestCount)

    [<Fact>]
    let ``profiles filter keeps verdict-first recomputation and visible shape alignment`` () =
        use harness = ConflictHarness.Create()
        use doc =
            JsonDocument.Parse(
                harness.EvalJson(
                    "conflicts({scope:['docs/canon/active-alpha.md','docs/canon/active-beta.md','inbox/ops/2026-05-07T00-00-00Z-pending-gamma.md','inbox/ops/2026-05-07T00-01-00Z-pending-delta.md'], pairs:true, judge:true, verdicts:['duplicate','compatible'], rollup:true, profiles:['duplicateOnly','noConflictMixed']})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(1, results.GetArrayLength())

        let candidate = results[0]
        let rollup = getRequiredProperty "rollup" candidate
        let pairs = getRequiredProperty "pairs" candidate
        let verdictCounts = getRequiredProperty "verdictCounts" rollup

        Assert.Equal(4, pairs.GetArrayLength())
        Assert.Equal("noConflictMixed", (getRequiredProperty "profile" rollup).GetString())
        Assert.Equal(0, (getRequiredProperty "conflict" verdictCounts).GetInt32())
        Assert.Equal(3, (getRequiredProperty "duplicate" verdictCounts).GetInt32())
        Assert.Equal(1, (getRequiredProperty "compatible" verdictCounts).GetInt32())
        Assert.Equal(6, harness.MixedRequestCount)

    [<Fact>]
    let ``profiles filter omits zero retained visible pair sets before membership evaluation`` () =
        use harness = ConflictHarness.Create()
        use doc = JsonDocument.Parse(harness.EvalJson("conflicts({scope:'inbox/ops', pairs:true, judge:true, verdicts:['compatible'], rollup:true, profiles:['mixedWithConflict']})"))

        let results = doc.RootElement
        Assert.Equal(JsonValueKind.Array, results.ValueKind)
        Assert.Equal(0, results.GetArrayLength())
        Assert.Equal(1, harness.MixedRequestCount)

    [<Fact>]
    let ``profiles filter hard rejects candidate-gate combinations without completion calls`` () =
        use harness = ConflictHarness.Create()
        use doc = JsonDocument.Parse(harness.EvalJson("conflicts({pairs:true, judge:true, rollup:true, noConflict:true, profiles:['duplicateOnly','compatibleOnly']})"))

        let error = getSingleError doc.RootElement
        Assert.Contains("hard-rejects", error)
        Assert.Contains("noConflict:true", error)
        Assert.Equal(0, harness.MixedRequestCount)

    [<Fact>]
    let ``profiles filter rejects dual inputs and invalid entries without completion calls`` () =
        use harness = ConflictHarness.Create()

        use dualInputDoc =
            JsonDocument.Parse(
                harness.EvalJson("conflicts({pairs:true, judge:true, rollup:true, profile:'duplicateOnly', profiles:['duplicateOnly']})"))

        let dualInputError = getSingleError dualInputDoc.RootElement
        Assert.Contains("hard-rejects", dualInputError)
        Assert.Contains("profile", dualInputError)

        use invalidDoc =
            JsonDocument.Parse(
                harness.EvalJson("conflicts({pairs:true, judge:true, rollup:true, profiles:[null,'duplicateOnly','duplicateOnly']})"))

        let invalidError = getSingleError invalidDoc.RootElement
        Assert.Contains("<null>", invalidError)
        Assert.Contains("duplicates:", invalidError)
        Assert.Equal(0, harness.MixedRequestCount)
