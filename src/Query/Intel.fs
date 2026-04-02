namespace AITeam.CodeSight

open System
open System.IO
open System.Threading.Tasks
open GitHub.Copilot.SDK
open Microsoft.Extensions.AI

/// code_intel: dispatches natural language questions to a mini-model with code_search tool.
module Intel =

    /// Classify a question into a playbook.
    let classifyPlaybook (question: string) =
        let q = question.ToLowerInvariant()
        if q.Contains("orient") || q.Contains("overview") || q.Contains("what does") || q.Contains("structure") || q.Contains("modules") then "orient"
        elif q.Contains("add") || q.Contains("implement") || q.Contains("where should") || q.Contains("how to") || q.Contains("plan") then "plan"
        elif q.Contains("change") || q.Contains("refactor") || q.Contains("break") || q.Contains("impact") || q.Contains("blast") then "blast"
        else "explore"

    /// Load a playbook file from the playbooks directory.
    let private loadPlaybook (playbooksDir: string) (name: string) =
        let path = Path.Combine(playbooksDir, sprintf "%s.md" name)
        if File.Exists path then File.ReadAllText(path) else ""

    /// Run code_intel: classify → pick playbook → dispatch to mini-model → return brief.
    let run (engine: Jint.Engine) (playbooksDir: string) (question: string) (modulesCache: string) = task {
        let playbookId = classifyPlaybook question
        let systemPrompt =
            let path = Path.Combine(playbooksDir, "system-prompt.md")
            if File.Exists path then File.ReadAllText(path) else "You are a code intelligence scout."
        let playbook = loadPlaybook playbooksDir playbookId

        let codeSearchTool = AIFunctionFactory.Create(
            Func<string, string>(fun js ->
                try QueryEngine.eval engine js
                with ex -> sprintf "Error: %s" ex.Message),
            "code_search",
            "Query the code index. Functions: search(q,opts), refs(name,opts), grep(pattern,opts), modules(), files(p?), context(file), expand(id), neighborhood(id,opts), impact(type), imports(file), deps(pattern), similar(id,opts). Start with modules() for overview.")

        let client = new CopilotClient()
        let sessionId = sprintf "code-sight-intel-%d" (DateTimeOffset.UtcNow.ToUnixTimeSeconds())

        let userMessage = sprintf "Codebase structure:\n%s\n\nPlaybook:\n%s\n\nQuestion: %s" modulesCache playbook question

        let config = SessionConfig(
            Model = "gpt-5.4-mini",
            ReasoningEffort = "high",
            SessionId = sessionId,
            Tools = ResizeArray<AIFunction>([codeSearchTool]),
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
            do! client.DeleteSessionAsync(sessionId)
            return result
        with ex ->
            return sprintf "Error running code_intel: %s" ex.Message
    }
