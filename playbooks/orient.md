# Playbook: Orient

Produce a codebase orientation brief for a senior developer.

## Output (exactly this structure)

1. **What it is** — one sentence
2. **Entry points** — 2-3 files where execution starts, with line refs
3. **Key modules** — 4-6 modules, each with a one-line role description
4. **Backbone types** — 3-5 types that are referenced widely
5. **Dependency flow** — one sentence: what depends on what

## Approach

1. `modules()` — **start here**: get the project-level map (names, file counts, top types, summaries). This tells you the landscape in one call.
2. `search` for entry points — 1-2 queries
3. `context()` on 1-2 central files — only the ones that look like orchestration/startup
4. `refs()` on 1-2 candidate backbone types from the modules data — confirm breadth

**You're done when you can fill all 5 sections.** The modules() call gives you most of what you need — you're just confirming details with the remaining calls.
