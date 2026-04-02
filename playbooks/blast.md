# Playbook: Blast

Produce an impact assessment for changing a type, function, or module.

## Input

A type name, function name, or module name.

## Output (exactly this structure)

1. **Blast radius** — N files, M direct references
2. **How it's used** — group refs by usage pattern (constructor calls, type annotations, method calls), quote the matchLines
3. **Risk zones** — which references are most fragile
4. **Safe changes** — what you can change without breaking callers
5. **Related types** — anything structurally similar that might need the same change

## Approach

1. `search(name, {limit:1})` — find the definition
2. `refs(name, {limit:30})` — get all references with matchLines (this is your primary data)
3. `impact(name)` — cross-check with type-level references
4. `deps(moduleName)` — module-level dependents
5. If a reference looks risky, `expand(id)` once to see the full context

**You're done when you can list the affected files and categorize the references.** The matchLines from refs() already tell you HOW it's used — don't expand every reference.
