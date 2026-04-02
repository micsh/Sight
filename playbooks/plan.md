# Playbook: Plan

Produce an implementation plan for the calling agent's feature request.

## Input

The calling agent describes a feature in natural language.

## Output (exactly this structure)

1. **Edit targets** — 1-3 files to modify, in order, with line refs
2. **Pattern to follow** — one existing example the agent should mimic (include ref ID)
3. **Wiring point** — where to register/connect the new code
4. **Dependencies** — what imports are needed, what modules are involved
5. **Risks** — anything tightly coupled or fragile (or "none identified")

## Approach

1. `search(feature_description, {limit:5})` — find semantically related code
2. `context(top_file)` on the most relevant file — see its structure
3. `similar(top_result_id, {limit:3})` — find analogous implementations
4. `grep("register|wire|create.*handler", {limit:5})` — find wiring/registration patterns
5. `neighborhood(best_pattern_id, {before:2, after:2})` — show the pattern in context

**You're done when you can name the files, show one pattern, and describe the wiring.** The agent will read the actual code — you just need to point them to the right place.
