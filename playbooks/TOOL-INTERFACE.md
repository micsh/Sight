# Code Intelligence — Agent Tool Interface

How the top-level agent (coder/planner) interacts with code intelligence.
How the platform routes between direct queries and mini-agent exploration.

## Top agent sees two tools

### `code_search(js)`
Run a specific query against the code index. For when the agent knows what to look for.

```
code_search('search("delivery engine", {limit: 5})')
code_search('refs("ChecklistTracker", {limit: 10})')
code_search('context("Orchestrator.fs")')
code_search('grep("Result<string, CliError>")')
```

**Implementation**: `WorkflowRunner.eval(js)` — direct Jint execution, returns formatted text.

### `code_intel(question)`
Ask any question about the codebase in natural language. For open-ended exploration.

```
code_intel("What does this codebase do?")
code_intel("Where should I add a new reactor?")
code_intel("What breaks if I change AgentConfig?")
code_intel("How does message delivery work?")
```

**Implementation**: dispatches to a gpt-5.4-mini sub-agent with `code_search` as its tool. The top agent doesn't know a sub-agent exists — it just gets back a brief.

## Top agent system prompt addition

```
## Code intelligence tools

code_search(js) — query the code index directly. Available functions:
  search(query, {limit, kind, file}), refs(name, {limit}), grep(pattern, {limit}),
  modules(), files(pattern?), context(file), expand(id), neighborhood(id, {before, after}),
  impact(type), imports(file), deps(pattern), similar(id, {limit})

code_intel(question) — ask any question about the codebase in natural language.
  Returns a structured brief with file:line references.
  Use for orientation, feature planning, impact analysis, or any exploration.
```

No mention of playbooks, Jint, mini-models, or internal routing.

## Platform routing logic (inside code_intel)

```
1. Classify the question:
   - Contains "what does", "orient", "overview", "structure" → orient playbook
   - Contains "add", "implement", "where should", "how to" → plan playbook  
   - Contains "change", "refactor", "break", "impact", "blast" → blast playbook
   - Anything else → explore playbook

2. Build mini-agent context:
   - System message: system-prompt.md (scout role, governance rules, tool API)
   - User message:
     "Codebase structure:
      [modules() output — auto-injected, cached at index load]
      
      Playbook:
      [selected playbook .md]
      
      Question: [the user's question]"

3. Mini-agent has one tool: code_search(js)
   - Same as what the top agent has, but the mini-agent uses it adaptively
   - Budget: 8-15 calls max (enforced by governance in system-prompt.md)

4. Return mini-agent's final response to the top agent as the tool result
```

## Data flow

```
Top agent: code_intel("Where should I add a new reactor?")
  │
  ├─ Platform classifies → "plan" playbook
  ├─ Platform injects modules() cache + playbook
  │
  ▼
Mini-agent (gpt-5.4-mini):
  │  System: system-prompt.md (scout role, governance)
  │  User: modules output + plan.md + question
  │
  ├─ code_search('search("reactor", {limit:5})')
  ├─ code_search('context("AutomationReactor.fs")')  
  ├─ code_search('similar(R3, {limit:3})')
  ├─ code_search('grep("register.*reactor")')
  │
  ▼ Returns structured brief
  │
Top agent receives:
  "Edit AutomationReactor.fs:8. Follow the pattern in ChecklistReactor.fs:6 (R12).
   Wire in AllPluginsReactorProvider.cs:15. Imports needed: AITeam.Reactors, AITeam.Boards."
```

## What the team needs to build

1. **Tool handler for `code_search`**: calls `WorkflowRunner.eval(js)`, returns string
2. **Tool handler for `code_intel`**: classifies question → picks playbook → builds mini-agent context → dispatches to gpt-5.4-mini with `code_search` tool → returns response
3. **Modules cache**: call `modules()` once at startup, cache the text output
4. **Register both tools** on the agent via `AIFunctionFactory.Create`
