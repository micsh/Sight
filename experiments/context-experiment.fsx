// ════════════════════════════════════════════════════════════════════
// Context Management Experiment — progressive disclosure for agents
// ════════════════════════════════════════════════════════════════════
//
// Tests expand/collapse with a self-hosted LLM (Gemma 4 on LM Studio).
// Manages conversation history directly: tracks context budget,
// auto-collapses oldest expansions when budget runs low.
//
// Usage:
//   dotnet fsi experiments/context-experiment.fsx --repo C:\path\to\repo
//
// Requires:
//   - Gemma 4 on LM Studio at localhost:8090
//   - code-sight on PATH with index built for the repo
//

open System
open System.IO
open System.Net.Http
open System.Text
open System.Text.Json
open System.Diagnostics

// ── Config ──

let llmUrl = "http://127.0.0.1:8090/v1/chat/completions"
let llmModel = "google/gemma-4-26b-a4b"
let maxTokens = 1000
let contextBudget = 8000  // max estimated tokens in history before auto-collapse

let repo =
    let idx = fsi.CommandLineArgs |> Array.tryFindIndex (fun a -> a = "--repo")
    match idx with
    | Some i when i + 1 < fsi.CommandLineArgs.Length -> fsi.CommandLineArgs.[i + 1]
    | _ -> Environment.CurrentDirectory

// ── History management ──

type Message = { Role: string; Content: string; TokenEstimate: int }
type ExpandedItem = { RefId: string; Summary: string; TurnIndex: int; TokenCost: int }

let mutable history: Message list = []
let mutable expandedItems: ExpandedItem list = []
let mutable totalTokens = 0
let mutable turnCount = 0

let estimateTokens (s: string) = max 1 (s.Length / 4)

let addMessage role content =
    let est = estimateTokens content
    history <- history @ [{ Role = role; Content = content; TokenEstimate = est }]
    totalTokens <- totalTokens + est

let collapseOldest () =
    match expandedItems with
    | [] -> false
    | oldest :: rest ->
        let marker = sprintf "[Expanded %s]" oldest.RefId
        let collapsed = sprintf "[Collapsed %s: %s]" oldest.RefId oldest.Summary
        let mutable saved = 0
        history <- history |> List.map (fun m ->
            if m.Content.Contains(marker) || m.Content.Contains(sprintf "expand(\"%s\")" oldest.RefId) || m.Content.Contains(sprintf "expand('%s')" oldest.RefId) then
                let newEst = estimateTokens collapsed
                saved <- saved + m.TokenEstimate - newEst
                { m with Content = collapsed; TokenEstimate = newEst }
            else m)
        totalTokens <- totalTokens - saved
        expandedItems <- rest
        eprintfn "  [auto-collapsed %s — saved ~%d tokens, now %d/%d]" oldest.RefId saved totalTokens contextBudget
        true
    
let ensureBudget () =
    while totalTokens > contextBudget && collapseOldest () do ()

// ── Code search via code-sight CLI ──

let runCodeSearch (js: string) =
    try
        let psi = ProcessStartInfo("code-sight", sprintf "search \"%s\" --repo \"%s\"" (js.Replace("\"", "\\\"")) repo)
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError <- true
        psi.UseShellExecute <- false
        psi.CreateNoWindow <- true
        let proc = Process.Start(psi)
        let output = proc.StandardOutput.ReadToEnd()
        proc.WaitForExit(30000) |> ignore
        if output.Trim() = "" then "(no results)" else output.Trim()
    with ex -> sprintf "Error: %s" ex.Message

// ── LLM chat ──

let client = new HttpClient(Timeout = TimeSpan.FromSeconds(120.0))

let chat () = task {
    let messages = ResizeArray()
    for m in history do
        let d = Collections.Generic.Dictionary<string, obj>()
        d.["role"] <- m.Role
        d.["content"] <- m.Content
        messages.Add(d)

    let tool = Collections.Generic.Dictionary<string, obj>()
    tool.["type"] <- "function"
    let fn = Collections.Generic.Dictionary<string, obj>()
    fn.["name"] <- "code_search"
    fn.["description"] <- "Search the code index with JavaScript. Functions: search(q,opts), refs(name,opts), grep(pattern,opts), modules(), files(p?), context(file), expand(id), neighborhood(id,opts), impact(type), imports(file), deps(pattern), similar(id,opts)"
    let params' = Collections.Generic.Dictionary<string, obj>()
    params'.["type"] <- "object"
    let props = Collections.Generic.Dictionary<string, obj>()
    let jsProp = Collections.Generic.Dictionary<string, obj>()
    jsProp.["type"] <- "string"
    jsProp.["description"] <- "JavaScript query"
    props.["js"] <- jsProp
    params'.["properties"] <- props
    params'.["required"] <- [| "js" |]
    fn.["parameters"] <- params'
    tool.["function"] <- fn

    let reqBody = Collections.Generic.Dictionary<string, obj>()
    reqBody.["model"] <- llmModel
    reqBody.["messages"] <- messages.ToArray()
    reqBody.["tools"] <- [| tool |]
    reqBody.["max_tokens"] <- maxTokens
    reqBody.["temperature"] <- 0.1

    let json = JsonSerializer.Serialize(reqBody)
    let content = new StringContent(json, Encoding.UTF8, "application/json")
    let! response = client.PostAsync(llmUrl, content)
    let! body = response.Content.ReadAsStringAsync()
    let doc = JsonDocument.Parse(body)
    let choice = doc.RootElement.GetProperty("choices").[0]
    let msg = choice.GetProperty("message")

    let contentText =
        match msg.TryGetProperty("content") with
        | true, v when v.ValueKind <> JsonValueKind.Null -> v.GetString()
        | _ -> ""
    let reasoning =
        match msg.TryGetProperty("reasoning_content") with
        | true, v when v.ValueKind <> JsonValueKind.Null -> v.GetString()
        | _ -> ""
    let toolCalls =
        match msg.TryGetProperty("tool_calls") with
        | true, tc when tc.GetArrayLength() > 0 ->
            tc.EnumerateArray() |> Seq.map (fun t ->
                let fn = t.GetProperty("function")
                t.GetProperty("id").GetString(),
                fn.GetProperty("name").GetString(),
                fn.GetProperty("arguments").GetString()
            ) |> Seq.toList
        | _ -> []
    let finish = choice.GetProperty("finish_reason").GetString()
    return contentText, reasoning, toolCalls, finish
}

// ── System prompt ──

let systemPrompt = """You are a code exploration assistant with a code_search tool.

Detail levels (cheapest to most expensive):
- search/refs/grep: summaries + signatures (~50 tokens/result) — START HERE
- context(file): file overview (~100 tokens)
- expand(id): full source code (~300 tokens) — use sparingly
- neighborhood(id, {before:2, after:2}): code + neighbors (~500 tokens) — only when needed

Rules:
- Start with summaries. Only expand when the user asks for code or you need to see implementation details.
- Each expansion costs context budget. When budget is low, old expansions get auto-collapsed.
- Combine queries in one call when possible.
- Keep responses concise with file:line references."""

addMessage "system" systemPrompt

// Pre-inject modules overview
let modulesOverview = runCodeSearch "modules()"
addMessage "system" (sprintf "Codebase structure:\n%s" modulesOverview)

// ── Interactive loop ──

eprintfn ""
eprintfn "Context experiment — Gemma 4 + code-sight"
eprintfn "  Repo: %s" repo
eprintfn "  Budget: %d tokens (~%d with system prompt)" contextBudget totalTokens
eprintfn "  Commands: 'collapse R5' to free context, 'budget' to check, 'quit' to exit"
eprintfn ""

let mutable running = true
while running do
    eprintf "You> "
    let input = Console.ReadLine()
    if input = null || input.Trim() = "quit" || input.Trim() = "exit" then
        running <- false
    elif input.Trim() = "" then ()
    elif input.Trim() = "budget" then
        eprintfn "  Budget: %d/%d tokens, %d expanded items, %d messages" totalTokens contextBudget expandedItems.Length history.Length
        for e in expandedItems do
            eprintfn "    %s: ~%d tokens (%s)" e.RefId e.TokenCost e.Summary
    elif input.Trim().StartsWith("collapse ") then
        let refId = input.Trim().Substring(9).Trim().ToUpperInvariant()
        match expandedItems |> List.tryFind (fun e -> e.RefId.ToUpperInvariant() = refId) with
        | Some e ->
            expandedItems <- expandedItems |> List.filter (fun x -> x.RefId <> e.RefId)
            let collapsed = sprintf "[Collapsed %s: %s]" e.RefId e.Summary
            history <- history |> List.map (fun m ->
                if m.Content.Contains(e.RefId) && m.TokenEstimate > estimateTokens collapsed then
                    totalTokens <- totalTokens - m.TokenEstimate + estimateTokens collapsed
                    { m with Content = collapsed; TokenEstimate = estimateTokens collapsed }
                else m)
            eprintfn "  Collapsed %s. Budget: %d/%d" e.RefId totalTokens contextBudget
        | None -> eprintfn "  %s not found. Expanded: %s" refId (expandedItems |> List.map (fun e -> e.RefId) |> String.concat ", ")
    else
        turnCount <- turnCount + 1
        addMessage "user" input
        ensureBudget()

        let mutable responding = true
        while responding do
            let sw = Stopwatch.StartNew()
            let contentText, reasoning, toolCalls, finish = chat() |> Async.AwaitTask |> Async.RunSynchronously
            sw.Stop()

            if reasoning.Length > 10 then
                let preview = reasoning.Replace("\n", " ")
                eprintfn "  [thinking %.1fs: %s...]" sw.Elapsed.TotalSeconds (preview.Substring(0, min 80 preview.Length))

            if toolCalls.Length > 0 then
                addMessage "assistant" (if contentText <> "" then contentText else sprintf "(calling %d tools)" toolCalls.Length)

                for (callId, name, argsJson) in toolCalls do
                    try
                        let args = JsonDocument.Parse(argsJson)
                        let js = args.RootElement.GetProperty("js").GetString()
                        eprintfn "  [%s: %s]" name (if js.Length > 70 then js.Substring(0, 70) + "..." else js)

                        let result = runCodeSearch js

                        // Track expand calls
                        if js.Contains("expand(") then
                            let refMatch = Text.RegularExpressions.Regex.Match(js, @"expand\([""']?(R\d+)")
                            if refMatch.Success then
                                let refId = refMatch.Groups.[1].Value
                                let summary = result.Split('\n') |> Array.tryFind (fun l -> l.Trim().Length > 10) |> Option.defaultValue "(code)"
                                let cost = estimateTokens result
                                expandedItems <- expandedItems @ [{ RefId = refId; Summary = summary.Trim().Substring(0, min 60 summary.Trim().Length); TurnIndex = turnCount; TokenCost = cost }]
                                eprintfn "  [expanded %s: ~%d tokens]" refId cost

                        let truncated = if result.Length > 3000 then result.Substring(0, 3000) + "\n... (truncated)" else result
                        addMessage "tool" truncated
                    with ex ->
                        addMessage "tool" (sprintf "Error: %s" ex.Message)

                ensureBudget()

                if finish = "stop" && contentText <> "" then
                    printfn "\n%s\n" contentText
                    eprintfn "  [budget: %d/%d, expanded: %d]" totalTokens contextBudget expandedItems.Length
                    responding <- false
            else
                if contentText <> "" then
                    addMessage "assistant" contentText
                    printfn "\n%s\n" contentText
                    eprintfn "  [budget: %d/%d, expanded: %d]" totalTokens contextBudget expandedItems.Length
                responding <- false

eprintfn ""
eprintfn "Session stats:"
eprintfn "  Turns: %d" turnCount
eprintfn "  Final context: ~%d/%d tokens" totalTokens contextBudget
eprintfn "  Expanded: %d items" expandedItems.Length
eprintfn "  Messages: %d" history.Length
