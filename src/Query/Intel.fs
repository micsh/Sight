namespace AITeam.CodeSight

open System
open System.IO
open System.Threading.Tasks
open GitHub.Copilot.SDK
open Microsoft.Extensions.AI

/// code_intel: dispatches natural language questions to a mini-model with code_search + read_playbook tools.
module Intel =

    /// List available playbook names from a directory.
    let listPlaybooks (playbooksDir: string) =
        if Directory.Exists playbooksDir then
            Directory.GetFiles(playbooksDir, "*.md")
            |> Array.map Path.GetFileNameWithoutExtension
            |> Array.filter (fun n -> n <> "system-prompt" && n <> "TOOL-INTERFACE")
            |> Array.sort
        else [||]

    /// Run code_intel: give mini-model tools and let it pick the right playbook.
    let run (engine: Jint.Engine) (playbooksDir: string) (question: string) (modulesCache: string) (udfSignatures: string) = task {
        let systemPrompt =
            let path = Path.Combine(playbooksDir, "system-prompt.md")
            if File.Exists path then File.ReadAllText(path) else "You are a code intelligence scout."

        let available = listPlaybooks playbooksDir

        let baseDesc = "Query the code index. Functions: search(q,opts), refs(name,opts), grep(pattern,opts), modules(), files(p?), context(file), expand(id), neighborhood(id,opts), impact(type), imports(file), deps(pattern), similar(id,opts), walk(name,opts). Start with modules() for overview."
        let toolDesc = if udfSignatures <> "" then sprintf "%s User-defined functions: %s" baseDesc udfSignatures else baseDesc
        let codeSearchTool = AIFunctionFactory.Create(
            Func<string, string>(fun js ->
                try QueryEngine.eval engine js
                with ex -> sprintf "Error: %s" ex.Message),
            "code_search",
            toolDesc)

        let playbookDescriptions = [
            "orient (what is this codebase?)"
            "plan (how to implement a feature?)"
            "blast (impact of changing something?)"
            "review (architecture and code quality?)"
            "explore (any other question)"
        ]
        let readPlaybookTool = AIFunctionFactory.Create(
            Func<string, string>(fun name ->
                let path = Path.Combine(playbooksDir, sprintf "%s.md" name)
                if File.Exists path then File.ReadAllText(path)
                else sprintf "Playbook '%s' not found. Available: %s" name (available |> String.concat ", ")),
            "read_playbook",
            sprintf "Read a strategy playbook. Available: %s. Pick the one that matches the question." (playbookDescriptions |> String.concat ", "))

        let client = new CopilotClient()
        let sessionId = sprintf "code-sight-intel-%s" (Guid.NewGuid().ToString().Substring(0, 8))

        let userMessage = sprintf "Codebase structure:\n%s\n\nQuestion: %s" modulesCache question

        let config = SessionConfig(
            Model = "gpt-5.4-mini",
            ReasoningEffort = "high",
            SessionId = sessionId,
            Tools = ResizeArray<AIFunction>([codeSearchTool; readPlaybookTool]),
            ExcludedTools = ResizeArray<string>(["task"; "report_intent"; "view"; "grep"; "glob"; "powershell"; "edit"; "create"]),
            SystemMessage = SystemMessageConfig(
                Mode = SystemMessageMode.Replace,
                Content = systemPrompt),
            OnPermissionRequest = PermissionRequestHandler(fun _ _ ->
                Task.FromResult(PermissionRequestResult(Kind = PermissionRequestResultKind.Approved))))

        try
            let! session = client.CreateSessionAsync(config)
            let! response = session.SendAndWaitAsync(MessageOptions(Prompt = userMessage), Nullable(TimeSpan.FromMinutes(3.0)))
            let result = response.Data.Content
            do! session.DisposeAsync().AsTask()
            // Keep session for review — don't delete
            // do! client.DeleteSessionAsync(sessionId)
            return result
        with ex ->
            return sprintf "Error running code_intel: %s" ex.Message
    }
