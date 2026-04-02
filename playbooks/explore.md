# Playbook: Explore

Answer any question about the codebase. No fixed structure — pick the right primitives for the question.

## Primitive selection

| Question pattern | Start with | Then |
|-----------------|------------|------|
| "What does X do?" | `search(X)` | read the summary — expand only if summary is unclear |
| "Where is X?" | `grep(X)` or `search(X)` | `context(file)` for file overview |
| "Who uses X?" | `refs(X)` | matchLines tell you how — expand only the riskiest |
| "What depends on X?" | `deps(X)` + `impact(X)` | done — these are complete answers |
| "How do I add X?" | `search(X)` → `similar()` | `neighborhood()` for one pattern |
| "Show me the code for X" | `search(X)` → `expand(id)` | done |

## Judgment

- Start broad, narrow based on results
- If `search()` misses, try `grep()` with exact terms
- When in doubt, `refs()` is your best tool for understanding coupling
- Combine in JS when useful: `refs("X").filter(r => r.file.includes("Tools"))`
