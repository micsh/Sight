open System
open System.IO
open AITeam.Sight.Core
open AITeam.KnowledgeSight

let private tryGitLines (workingDir: string) (arguments: string) =
    try
        let psi = System.Diagnostics.ProcessStartInfo("git", arguments)
        psi.WorkingDirectory <- workingDir
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError <- true
        psi.UseShellExecute <- false
        psi.CreateNoWindow <- true
        use proc = System.Diagnostics.Process.Start(psi)
        let output = proc.StandardOutput.ReadToEnd()
        let error = proc.StandardError.ReadToEnd()
        proc.WaitForExit()

        if proc.ExitCode <> 0 then
            Error(if String.IsNullOrWhiteSpace(error) then sprintf "git %s failed" arguments else error.Trim())
        else
            Ok(
                output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                |> Array.map (fun line -> line.Trim().Replace('\\', '/'))
                |> Array.filter (String.IsNullOrWhiteSpace >> not)
            )
    with ex ->
        Error(sprintf "git not available: %s" ex.Message)

let private collectChangedMarkdownDocs (repo: string) (sinceRef: string) =
    let toMarkdownDocs (lines: string[]) =
        lines
        |> Array.filter (fun line -> line.EndsWith(".md", StringComparison.OrdinalIgnoreCase))
        |> Array.distinct

    if not (String.IsNullOrWhiteSpace(sinceRef)) then
        tryGitLines repo (sprintf "diff --name-only %s -- ." sinceRef)
        |> Result.map toMarkdownDocs
    else
        let combineResults (results: Result<string[], string>[]) =
            let errors =
                results
                |> Array.choose (function | Error error -> Some error | _ -> None)

            if errors.Length > 0 then
                Error(errors.[0])
            else
                results
                |> Array.choose (function | Ok lines -> Some lines | _ -> None)
                |> Array.collect id
                |> toMarkdownDocs
                |> Ok

        [|
            tryGitLines repo "diff --name-only -- ."
            tryGitLines repo "diff --cached --name-only -- ."
            tryGitLines repo "ls-files --others --exclude-standard -- ."
        |]
        |> combineResults

let private isIgnoredHealthMentionPath (path: string) =
    path.Contains("node_modules") || path.Contains("bin") || path.Contains("obj")

let private buildHealthDocContentLookup (chunks: DocChunk[]) =
    chunks
    |> Array.groupBy (fun chunk -> chunk.FilePath)
    |> Array.map (fun (docPath, docChunks) ->
        docPath,
        (docChunks
         |> Array.map (fun chunk -> chunk.Content)
         |> String.concat "\n"))
    |> Map.ofArray

let private tryBuildHealthCodeFileLookup (repo: string) =
    try
        let lookup = System.Collections.Generic.Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)

        for filePath in Directory.EnumerateFiles(repo, "*", SearchOption.AllDirectories) do
            if not (isIgnoredHealthMentionPath filePath) then
                let fileName = Path.GetFileName(filePath)
                if not (lookup.ContainsKey(fileName)) then
                    lookup.[fileName] <- filePath

        Some lookup
    with _ ->
        None

let private tryFindHealthCodeFile (lookup: System.Collections.Generic.Dictionary<string, string>) (codeRef: string) =
    match lookup.TryGetValue(codeRef) with
    | true, sourcePath -> Some sourcePath
    | _ -> None

let private ambientFamilyLabel (family: string) =
    match family with
    | "duplicate_active_state" -> "duplicate live status"
    | "chronology_heavy" -> "chronology-heavy closeout detail"
    | "section_compaction" -> "implementation-history / scaffolding residue"
    | "canonical_owner_conflict" -> "canonical owner conflict"
    | "missing_invariant_or_frontier_summary" -> "missing invariant/frontier summary"
    | _ -> family.Replace('_', ' ')

let printUsage () =
    printfn "AITeam.KnowledgeSight — knowledge/doc intelligence for any repo"
    printfn ""
    printfn "Usage:"
    printfn "  knowledge-sight index [--quiet] [--repo <path>]      Build/update index"
    printfn "  knowledge-sight catalog [--repo <path>]              Show topic map"
    printfn "  knowledge-sight search <js> [--json] [--quiet] [--repo <path>]  Run a query"
    printfn "  knowledge-sight eval <js> [--json] [--quiet] [--repo <path>]    Alias for search"
    printfn "  knowledge-sight eval - [--json] [--quiet] [--repo <path>]       Read expression from stdin"
    printfn "  knowledge-sight repl [--repo <path>]                   Interactive mode"
    printfn "  knowledge-sight orphans [--repo <path>]              Find unlinked docs"
    printfn "  knowledge-sight broken [--repo <path>]               Find broken links"
    printfn "  knowledge-sight stale [--repo <path>]                Find active docs drifting from source"
    printfn "  knowledge-sight health [--changed] [--since <gitRef>] [--limit <n>] [--repo <path>]"
    printfn "                                                    All checks: orphans + broken + stale (design defaults)"
    printfn "  knowledge-sight check <text|file> [--repo <path>]    Find novel knowledge in text"
    printfn "  knowledge-sight check <js> --expr [--repo <path>]    Check with JS expression (UDFs available)"
    printfn "  knowledge-sight fn add <name> <body> [options]       Define a reusable function"
    printfn "  knowledge-sight fn list [--verbose] [--json]         List saved functions"
    printfn "  knowledge-sight fn rm <name> [--repo <path>]         Remove a function"
    printfn "  knowledge-sight --help                               Show this help"
    printfn ""
    printfn "Global option:"
    printfn "  --quiet                              Suppress informational stderr diagnostics; warnings/errors stay visible"
    printfn ""
    printfn "Functions:"
    printfn "  Save chains of primitives as reusable functions. Example:"
    printfn "    knowledge-sight fn add deepSearch --params \"q\" \"search(q).concat(similar(search(q)[0]))\""
    printfn "    knowledge-sight search \"deepSearch('auth')\""
    printfn ""
    printfn "  Options for 'fn add':"
    printfn "    --params \"a,b\"          Comma-separated parameter names"
    printfn "    --desc \"description\"    Optional description"
    printfn "    --file <path>           Read function body from a file"
    printfn ""
    printfn "Primitives (available in search expressions):"
    printfn "  search(query, {limit, tag, file, status}) Semantic search across indexed chunks"
    printfn "  catalog({status})                    Topic map of indexed docs"
    printfn "  context(file)                        Overview of a file with sections/links + indexed frontmatter"
    printfn "  expand(refId)                        Expand R# ref to full chunk content"
    printfn "  neighborhood(refId, {before, after}) Surrounding sections around a ref"
    printfn "  similar(refId, {limit, status})      Semantically similar chunks"
    printfn "  grep(pattern, {limit, file, status}) Regex search over chunk content"
    printfn "  mentions(term, {limit, status})      Find term mentions across docs"
    printfn "  files(pattern)                       List indexed files"
    printfn "  backlinks(file, {status})            Incoming links to a file"
    printfn "  links(file, {status})                Outgoing links from a file"
    printfn "  orphans({status})                    Docs with no incoming links"
    printfn "  broken({status})                     Broken links across docs"
    printfn "  placement(content, {limit, status})  Suggest where new content fits"
    printfn "  walk(file, {depth, direction, status}) Traverse the link graph"
    printfn "  changed(gitRef)                      Chunks in files changed since a git ref"
    printfn "  explain(refId)                       Debug: show index metadata + indexed frontmatter for a ref"
    printfn "  saveSession(name)                    Save current ref session as a named snapshot"
    printfn "  loadSession(name)                    Load a previously saved ref session"
    printfn "  sessions()                           List saved sessions"
    printfn "  novelty(text, {threshold, status})   Detect novel knowledge in text"
    printfn "  pinned({tier})                       Return ambient docs for the requested tier"
    printfn "  propose(text, {team, cycle, ...})    File novel claims into the inbox"
    printfn "  triage({team, before, limit})        List pending inbox items awaiting disposition"
    printfn "  dispose(ref, {action, ...})          Promote, merge, or reject one inbox item"
    printfn "  supersede(ref, text, {reason, ...})  Replace one active canonical doc with a versioned sibling"
    printfn "  reverify({scope, apply})             Re-run deterministic verify expressions (scope: exact file / dir / glob; apply:true marks drifting active docs stale)"
    printfn "  conflicts({scope, threshold})         Surface pending+active similarity candidates (core surface); add {pairs:true, judge:true} for judged pairs, advanced filter knobs remain compatibility-only"
    printfn "  prune({scope, olderThanDays, apply}) Preview prune candidates by default; apply:true deletes only initially eligible stale/superseded/deprecated canonical docs"
    printfn "  cluster(dir, {threshold, status})    Cluster docs by similarity"
    printfn "  gaps({scope, min_docs, signal})      Find coverage gaps"
    printfn "  hygiene({profile, limit})           Experimental hygiene report / fast-path compaction shortlist"
    printfn ""
    printfn "Composition helpers:"
    printfn "  pipe(value, fn1, fn2, ...)           Thread value through functions"
    printfn "  tap(value, fn)                       Run fn for side-effects, return value"
    printfn "  mergeBy(key, arr1, arr2, ...)        Union arrays with dedup by key"
    printfn "  print(value)                         Debug output to stderr"
    printfn ""
    printfn "Note: All primitives above (including orphans, broken) are available in"
    printfn "  search/eval expressions and check --expr. The CLI commands (e.g. 'knowledge-sight"
    printfn "  orphans') are shortcuts that call the same primitives."
    printfn ""

let parseArgs (args: string[]) =
    let mutable repo = Environment.CurrentDirectory
    let mutable command = ""
    let mutable verbose = false
    let mutable jsonOut = false
    let mutable quiet = false
    let mutable query = ""
    let mutable fnName = ""
    let mutable fnParams = ""
    let mutable fnDesc = ""
    let mutable fnFile = ""
    let mutable exprMode = false
    let mutable healthChanged = false
    let mutable healthSince = ""
    let mutable healthLimit = 5
    let mutable i = 0
    while i < args.Length do
        match args.[i] with
        | "--repo" when i + 1 < args.Length ->
            repo <- args.[i + 1]
            i <- i + 2
        | "--quiet" ->
            quiet <- true
            i <- i + 1
        | "--help" | "-h" ->
            command <- "help"
            i <- i + 1
        | "index" | "catalog" | "orphans" | "broken" | "stale" | "health" | "repl" ->
            command <- args.[i]
            i <- i + 1
        | "fn" when i + 1 < args.Length ->
            match args.[i + 1] with
            | "add" when i + 2 < args.Length ->
                command <- "fn-add"
                fnName <- args.[i + 2]
                i <- i + 3
                // Parse remaining fn add args
                while i < args.Length do
                    match args.[i] with
                    | "--params" when i + 1 < args.Length ->
                        fnParams <- args.[i + 1]
                        i <- i + 2
                    | "--desc" when i + 1 < args.Length ->
                        fnDesc <- args.[i + 1]
                        i <- i + 2
                    | "--file" when i + 1 < args.Length ->
                        fnFile <- args.[i + 1]
                        i <- i + 2
                    | "--quiet" ->
                        quiet <- true
                        i <- i + 1
                    | "--repo" when i + 1 < args.Length ->
                        repo <- args.[i + 1]
                        i <- i + 2
                    | _ when query = "" ->
                        query <- args.[i]
                        i <- i + 1
                    | _ -> i <- i + 1
            | "list" ->
                command <- "fn-list"
                i <- i + 2
                while i < args.Length do
                    match args.[i] with
                    | "--verbose" | "-v" -> verbose <- true; i <- i + 1
                    | "--json" -> jsonOut <- true; i <- i + 1
                    | "--quiet" -> quiet <- true; i <- i + 1
                    | "--repo" when i + 1 < args.Length -> repo <- args.[i + 1]; i <- i + 2
                    | _ -> i <- i + 1
            | "rm" when i + 2 < args.Length ->
                command <- "fn-rm"
                fnName <- args.[i + 2]
                i <- i + 3
            | _ ->
                command <- "fn-list"
                i <- i + 2
        | "check" when i + 1 < args.Length ->
            command <- "check"
            query <- args.[i + 1]
            i <- i + 2
            while i < args.Length do
                match args.[i] with
                | "--expr" -> exprMode <- true; i <- i + 1
                | "--quiet" -> quiet <- true; i <- i + 1
                | "--repo" when i + 1 < args.Length -> repo <- args.[i + 1]; i <- i + 2
                | _ -> i <- i + 1
        | "search" | "eval" when i + 1 < args.Length ->
            command <- "search"
            query <- args.[i + 1]
            i <- i + 2
            while i < args.Length do
                match args.[i] with
                | "--json" -> jsonOut <- true; i <- i + 1
                | "--quiet" -> quiet <- true; i <- i + 1
                | "--repo" when i + 1 < args.Length -> repo <- args.[i + 1]; i <- i + 2
                | _ -> i <- i + 1
        | "--changed" when command = "health" ->
            healthChanged <- true
            i <- i + 1
        | "--since" when command = "health" && i + 1 < args.Length ->
            healthSince <- args.[i + 1]
            i <- i + 2
        | "--limit" when command = "health" && i + 1 < args.Length ->
            match Int32.TryParse(args.[i + 1]) with
            | true, parsed -> healthLimit <- parsed
            | _ -> ()
            i <- i + 2
        | s when command = "" && not (s.StartsWith("--")) ->
            command <- "search"
            query <- s
            i <- i + 1
        | _ -> i <- i + 1
    repo, command, query, fnName, fnParams, fnDesc, fnFile, verbose, jsonOut, quiet, exprMode, healthChanged, healthSince, healthLimit

[<EntryPoint>]
let main args =
    CliOutput.setQuiet false

    if args.Length = 0 then printUsage(); 0
    else

    let repo, command, query, fnName, fnParams, fnDesc, fnFile, verbose, jsonOut, quiet, exprMode, healthChanged, healthSince, healthLimit = parseArgs args
    CliOutput.setQuiet quiet

    if command = "help" then printUsage(); 0
    else

    let cfg = Config.load repo

    match command with
    | "fn-add" ->
        let body =
            if fnFile <> "" then
                let path = if File.Exists fnFile then fnFile else Path.Combine(repo, fnFile)
                if File.Exists path then File.ReadAllText(path)
                else eprintfn "File not found: %s" fnFile; ""
            else query
        if body = "" then
            eprintfn "No function body provided. Pass it as an argument or use --file <path>."
            1
        else
            let ps = if fnParams = "" then [||] else fnParams.Split(',') |> Array.map (fun s -> s.Trim())
            let fn = { Name = fnName; Params = ps; Body = body; Description = fnDesc }
            match FunctionStore.add repo fn with
            | Ok msg -> printfn "%s" msg; 0
            | Error msg -> eprintfn "Error: %s" msg; 1

    | "fn-list" ->
        match FunctionStore.load repo with
        | Error msg ->
            eprintfn "Error: %s" msg
            1
        | Ok fns ->
            if fns.Length = 0 then
                if jsonOut then printfn "[]"
                else printfn "No functions defined. Use 'knowledge-sight fn add <name> <body>' to create one."
            elif jsonOut then
                let options = System.Text.Json.JsonSerializerOptions(WriteIndented = true)
                let arr = fns |> Array.map (fun f ->
                    dict [ "name", box f.Name; "params", box f.Params; "body", box f.Body; "description", box f.Description ])
                printfn "%s" (System.Text.Json.JsonSerializer.Serialize(arr, options))
            elif verbose then
                printfn "%d function(s) in %s:" fns.Length repo
                printfn ""
                for f in fns do
                    let joined = f.Params |> String.concat ", "
                    let ps = if f.Params.Length = 0 then "()" else sprintf "(%s)" joined
                    printfn "  %s%s" f.Name ps
                    if f.Description <> "" then printfn "    %s" f.Description
                    printfn "    body: %s" f.Body
                    printfn ""
            else
                printfn "%d function(s):" fns.Length
                for f in fns do
                    let joined = f.Params |> String.concat ", "
                    let ps = if f.Params.Length = 0 then "()" else sprintf "(%s)" joined
                    let desc = if f.Description <> "" then sprintf " — %s" f.Description else ""
                    printfn "  %s%s%s" f.Name ps desc
            0

    | "fn-rm" ->
        match FunctionStore.remove repo fnName with
        | Ok msg -> printfn "%s" msg; 0
        | Error msg -> eprintfn "Error: %s" msg; 1

    | "index" ->
        match IndexingWorkflow.rebuild cfg with
        | Ok _ -> 0
        | Error message ->
            eprintfn "%s" message
            1

    | "catalog" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index ->
            let result = Primitives.catalog cfg index Primitives.retrievalDefaultStatuses
            printfn "%s" (Format.formatValue (box result))
            0

    | "orphans" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index ->
            let result = Primitives.orphans cfg index Primitives.orphansDefaultStatuses
            printfn "%d orphaned docs (no incoming links):" result.Length
            for d in result do
                let file = string d.["file"]
                let title = string d.["title"]
                let sections = d.["sections"] :?> int
                printfn "  %s — %s (%d sections)" file title sections
            0

    | "broken" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index ->
            let result = Primitives.broken cfg index Primitives.brokenDefaultStatuses
            printfn "%d broken links:" result.Length
            for d in result do
                printfn "  %s → %s (in %s)" (string d.["from"]) (string d.["target"]) (string d.["section"])
            0

    | "stale" | "health" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index when command = "health" && healthChanged ->
            let ambientLimit = max 1 (min 5 healthLimit)
            let sourceChunks = IndexStore.loadSourceChunks cfg.IndexDir
            let scopeLabel =
                if String.IsNullOrWhiteSpace(healthSince) then "working_tree" else sprintf "since %s" healthSince

            printfn "═══ Knowledge Health Report (changed scope) ═══"
            printfn ""
            printfn "📊 Index: %d chunks, %d links, %d docs with frontmatter" index.Chunks.Length index.Links.Length index.Frontmatters.Count
            printfn ""
            printfn "🧭 Focus-preserving compaction hints"

            match collectChangedMarkdownDocs repo healthSince with
            | Error error ->
                printfn "   Scope unavailable: %s" error
            | Ok changedDocs when changedDocs.Length = 0 ->
                if String.IsNullOrWhiteSpace(healthSince) then
                    printfn "   No changed markdown docs in the working tree. Use --since <gitRef> for an explicit review window."
                else
                    printfn "   No changed markdown docs in scope (%s)." scopeLabel
            | Ok changedDocs ->
                let indexedDocs =
                    index.Chunks
                    |> Array.map (fun c -> Path.GetRelativePath(repo, c.FilePath).Replace("\\", "/"))
                    |> Array.distinct
                    |> Set.ofArray

                let indexedChangedDocs =
                    changedDocs
                    |> Array.filter indexedDocs.Contains

                printfn "   Scope: %s" scopeLabel
                printfn "   Changed markdown docs: %d (%d indexed)" changedDocs.Length indexedChangedDocs.Length
                for path in indexedChangedDocs |> Array.truncate 5 do
                    printfn "   - %s" path
                if indexedChangedDocs.Length > 5 then printfn "   ... and %d more" (indexedChangedDocs.Length - 5)

                if indexedChangedDocs.Length = 0 then
                    printfn "   No indexed changed docs available yet. Run `knowledge-sight index` after creating new docs."
                else
                    let hints = Primitives.ambientCompactionHints index sourceChunks repo indexedChangedDocs ambientLimit
                    if hints.Length = 0 then
                        printfn "   No focus-preserving hints in the changed scope."
                    else
                        for idx in 0 .. hints.Length - 1 do
                            let hint = hints.[idx]
                            let canonicalOwner = string hint.["canonical_owner_or_link"]
                            printfn "   %d. %s" (idx + 1) (string hint.["file"])
                            printfn "      section: %s" (string hint.["section"])
                            printfn "      pattern: %s" (ambientFamilyLabel (string hint.["family"]))
                            printfn "      why: %s" (string hint.["why_this_distracts_future_context"])
                            printfn "      replace with: %s" (string hint.["suggested_replacement_shape"])
                            if canonicalOwner <> "" then
                                printfn "      canonical owner: %s" canonicalOwner
            0
        | Some index ->

            // Staleness check: for each doc with frontmatter, check if related source files
            // are newer than the doc itself.
            // related: can contain doc IDs (ignored here) or file paths/globs.
            let staleResults = ResizeArray<string * string * DateTime * DateTime>()
            let mentionResults = ResizeArray<string * string * string>()
            let staleAllowedStatuses = Set.ofArray Primitives.staleDefaultStatuses

            for kv in index.Frontmatters do
                let docPath = kv.Key
                let fm = kv.Value
                if not (File.Exists docPath) || not (Primitives.matchesDocStatus cfg index staleAllowedStatuses docPath) then () else
                let docMtime = File.GetLastWriteTimeUtc(docPath)

                for rel in fm.Related do
                    // Check if this looks like a file path (has extension or path separator)
                    let isFilePath = rel.Contains(".") && (rel.Contains("/") || rel.Contains("\\") || rel.EndsWith(".fs") || rel.EndsWith(".cs") || rel.EndsWith(".js") || rel.EndsWith(".ts") || rel.EndsWith(".py") || rel.EndsWith(".md"))
                    if isFilePath then
                        // Try to resolve: relative to repo root, or as-is
                        let candidates = [
                            Path.Combine(repo, rel)
                            rel
                        ]
                        match candidates |> List.tryFind File.Exists with
                        | Some sourcePath ->
                            let sourceMtime = File.GetLastWriteTimeUtc(sourcePath)
                            if sourceMtime > docMtime then
                                staleResults.Add(Path.GetFileName docPath, rel, docMtime, sourceMtime)
                        | None -> ()

            // Also: scan doc content for code file mentions (e.g., "Orchestrator.fs", "Program.cs")
            // and check if those files exist and are newer
            let codeFileRegex = System.Text.RegularExpressions.Regex(@"\b(\w+(?:\.\w+)*\.(?:fs|cs|js|ts|py|go|rs))\b", System.Text.RegularExpressions.RegexOptions.Compiled)

            let sourceChunks = IndexStore.loadSourceChunks cfg.IndexDir

            match sourceChunks with
            | None -> ()
            | Some chunks ->
                let docContents = buildHealthDocContentLookup chunks
                let codeFileLookup = tryBuildHealthCodeFileLookup repo

                for KeyValue(docPath, docContent) in docContents do
                    if File.Exists docPath && Primitives.matchesDocStatus cfg index staleAllowedStatuses docPath then
                        let docMtime = File.GetLastWriteTimeUtc(docPath)
                        let codeRefs = codeFileRegex.Matches(docContent) |> Seq.cast<System.Text.RegularExpressions.Match> |> Seq.map (fun m -> m.Groups.[1].Value) |> Seq.distinct |> Seq.toArray

                        for codeRef in codeRefs do
                            let found =
                                match codeFileLookup with
                                | Some lookup -> tryFindHealthCodeFile lookup codeRef
                                | None -> None
                            match found with
                            | Some sourcePath ->
                                let sourceMtime = File.GetLastWriteTimeUtc(sourcePath)
                                if sourceMtime > docMtime then
                                    let daysBehind = (sourceMtime - docMtime).TotalDays
                                    if daysBehind > 1.0 then
                                        mentionResults.Add(Path.GetFileName docPath, codeRef, sprintf "%.0f days behind" daysBehind)
                            | None -> ()

            if command = "health" then
                // Run all checks
                let orphanResult = Primitives.orphans cfg index Primitives.orphansDefaultStatuses
                let brokenResult = Primitives.broken cfg index Primitives.brokenDefaultStatuses
                printfn "═══ Knowledge Health Report ═══"
                printfn ""
                printfn "📊 Index: %d chunks, %d links, %d docs with frontmatter" index.Chunks.Length index.Links.Length index.Frontmatters.Count
                printfn ""
                printfn "🔗 Orphans: %d docs with no incoming links" orphanResult.Length
                for d in orphanResult |> Array.truncate 5 do
                    printfn "   %s — %s" (string d.["file"]) (string d.["title"])
                if orphanResult.Length > 5 then printfn "   ... and %d more" (orphanResult.Length - 5)
                printfn ""
                printfn "💔 Broken links: %d" brokenResult.Length
                for d in brokenResult |> Array.truncate 5 do
                    printfn "   %s → %s" (string d.["from"]) (string d.["target"])
                if brokenResult.Length > 5 then printfn "   ... and %d more" (brokenResult.Length - 5)
                printfn ""
                printfn "⏰ Stale (related source newer than doc): %d" staleResults.Count
                for (doc, source, _, _) in staleResults do
                    printfn "   %s ← %s changed" doc source
                printfn ""
                printfn "📝 Mentions (doc references code that changed since): %d" mentionResults.Count
                for (doc, codeRef, age) in mentionResults |> Seq.truncate 10 |> Seq.toList do
                    printfn "   %s mentions %s (%s)" doc codeRef age
                if mentionResults.Count > 10 then printfn "   ... and %d more" (mentionResults.Count - 10)
                printfn ""
                let total = orphanResult.Length + brokenResult.Length + staleResults.Count + mentionResults.Count

                // Folder density check — count files (not chunks) per directory
                let fileDirCounts =
                    index.Chunks
                    |> Array.map (fun c ->
                        let rel = Path.GetRelativePath(repo, c.FilePath).Replace("\\", "/")
                        let parts = rel.Split('/')
                        let dir = if parts.Length >= 2 then parts.[.. parts.Length - 2] |> String.concat "/" else "."
                        dir, Path.GetFileName c.FilePath)
                    |> Array.distinct
                    |> Array.countBy fst
                let denseDirs = fileDirCounts |> Array.filter (fun (_, count) -> count >= 8) |> Array.sortByDescending snd
                if denseDirs.Length > 0 then
                    printfn "📁 Dense folders (consider splitting into subfolders):"
                    for (dir, count) in denseDirs do
                        printfn "   %s/ — %d docs (use: knowledge-sight search 'cluster(\"%s\")')" dir count dir
                    printfn ""
                let total = total + denseDirs.Length

                if total = 0 then printfn "✅ All clean!"
                else printfn "⚠ %d issues found" total
            else
                // Just stale
                printfn "Stale docs (related source newer than doc): %d" staleResults.Count
                for (doc, source, docTime, srcTime) in staleResults do
                    printfn "  %s ← %s (source: %s, doc: %s)" doc source (srcTime.ToString("yyyy-MM-dd")) (docTime.ToString("yyyy-MM-dd"))
                printfn ""
                printfn "Docs mentioning changed code files: %d" mentionResults.Count
                for (doc, codeRef, age) in mentionResults |> Seq.truncate 20 |> Seq.toList do
                    printfn "  %s mentions %s (%s)" doc codeRef age
                if mentionResults.Count > 20 then printfn "  ... and %d more" (mentionResults.Count - 20)
            0

    | "check" when query <> "" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index ->
            if exprMode then
                // JS expression mode: evaluate through QueryEngine (UDFs available)
                let chunks = IndexStore.loadSourceChunks cfg.IndexDir
                let engine = QueryEngine.create cfg index chunks
                let result = QueryEngine.eval engine query
                // Try to apply check-style formatting if result looks like novelty output
                printfn "%s" result
                0
            else
                // Plain text mode: read text, invoke novelty directly
                let text =
                    if File.Exists query then File.ReadAllText(query)
                    elif File.Exists(Path.Combine(repo, query)) then File.ReadAllText(Path.Combine(repo, query))
                    elif query = "-" then
                        use reader = new StreamReader(Console.OpenStandardInput())
                        reader.ReadToEnd()
                    else query
                let results = Primitives.novelty cfg index cfg.EmbeddingUrl text 0.75 Primitives.retrievalDefaultStatuses
                let novel = results |> Array.filter (fun d -> string d.["status"] = "novel")
                let covered = results |> Array.filter (fun d -> string d.["status"] = "covered")
                let musing = results |> Array.filter (fun d -> string d.["status"] = "musing")
                let offTopic = results |> Array.filter (fun d -> string d.["status"] = "off-topic")

                printfn "═══ Knowledge Check ═══"
                printfn "  %d paragraphs analyzed" results.Length
                let novelLabel = sprintf "  🆕 %d novel (new knowledge to capture)" novel.Length
                let coveredLabel = sprintf "  ✅ %d covered (already in knowledge base)" covered.Length
                let musingLabel = sprintf "  💭 %d musings (discussion, not knowledge)" musing.Length
                let offTopicLabel = sprintf "  ❌ %d off-topic (unrelated to project)" offTopic.Length
                printfn "%s" novelLabel
                printfn "%s" coveredLabel
                printfn "%s" musingLabel
                printfn "%s" offTopicLabel

                if novel.Length > 0 then
                    printfn ""
                    printfn "── Novel knowledge to capture ──"
                    for d in novel do
                        let score = d.["score"] :?> float
                        let signal = d.["signal"] :?> int
                        printfn "  [%.2f sig=%d] %s" score signal (string d.["paragraph"])
                        printfn "       nearest: %s > %s" (string d.["nearDoc"]) (string d.["nearSection"])
                        printfn ""

                if covered.Length > 0 then
                    printfn "── Already covered ──"
                    for d in covered |> Array.truncate 5 do
                        printfn "  [%.2f] %s" (d.["score"] :?> float) (string d.["paragraph"])
                        printfn "       in: %s > %s" (string d.["nearDoc"]) (string d.["nearSection"])
                    if covered.Length > 5 then printfn "  ... and %d more" (covered.Length - 5)
                0

    | "repl" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index ->
            let chunks = IndexStore.loadSourceChunks cfg.IndexDir
            if chunks.IsSome then eprintfn "[loaded %d cached source chunks]" chunks.Value.Length
            let engine = QueryEngine.create cfg index chunks
            eprintfn "knowledge-sight REPL. Type JS queries, 'quit' to exit."
            eprintfn "  search(q,opts), context(file), expand(id), neighborhood(id,opts),"
            eprintfn "  similar(id,opts), grep(pattern,opts), files(p?), catalog(opts),"
            eprintfn "  backlinks(file,opts), links(file,opts), pinned(opts), orphans(opts), broken(opts), gaps(opts),"
            eprintfn "  propose(text,opts), triage(opts), dispose(ref,opts), supersede(ref,text,opts), reverify(opts)"
            eprintfn ""
            let mutable running = true
            while running do
                eprintf "> "
                let line = System.Console.ReadLine()
                if line = null || line.Trim() = "quit" || line.Trim() = "exit" then
                    running <- false
                elif line.Trim() <> "" then
                    printfn "%s" (QueryEngine.eval engine (line.Trim()))
                    printfn ""
            0

    | "search" | _ when query <> "" ->
        match IndexStore.load cfg.IndexDir with
        | None -> eprintfn "No index found. Run: knowledge-sight index"; 1
        | Some index ->
            let chunks = IndexStore.loadSourceChunks cfg.IndexDir
            let engine = QueryEngine.create cfg index chunks
            let actualQuery =
                if query = "-" then
                    use reader = new StreamReader(Console.OpenStandardInput())
                    reader.ReadToEnd().Trim()
                else query
            let result =
                if jsonOut then QueryEngine.evalJson engine actualQuery
                else QueryEngine.eval engine actualQuery
            printfn "%s" result
            0

    | _ ->
        printUsage()
        0
