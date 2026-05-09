# KnowledgeSight

Knowledge and documentation intelligence for any repo. Index your markdown docs, then query, analyze, and maintain them with a composable JS expression language.

## Install

**macOS / Linux:**
```bash
curl -fsSL https://raw.githubusercontent.com/micsh/Sightline/main/install-knowledge-sight.sh | bash
```

**Windows (PowerShell):**
```powershell
irm https://raw.githubusercontent.com/micsh/Sightline/main/install-knowledge-sight.ps1 | iex
```

Or download binaries directly from [Releases](https://github.com/micsh/Sightline/releases).

## Quick Start

```bash
# Build the index (scans *.md files)
knowledge-sight index --repo /path/to/repo

# Run a semantic search
knowledge-sight search "search('authentication')"

# Health check — orphans, broken links, stale docs
knowledge-sight health --repo /path/to/repo

# Ambient compaction hints for the current edit loop
knowledge-sight health --changed --repo /path/to/repo
```

## Installation

Requires .NET 10+.

```bash
dotnet build
```

The tool expects a local embedding server at `http://localhost:1234/v1/embeddings` by default (configurable via `knowledge-sight.json`).

## Commands

| Command | Description |
|---------|-------------|
| `index [--quiet] [--repo <path>]` | Build or incrementally update the doc index |
| `catalog [--repo <path>]` | Show a topic map of all indexed docs |
| `search <expr> [--json] [--quiet] [--repo <path>]` | Run a query expression |
| `eval <expr> [--json] [--quiet] [--repo <path>]` | Alias for `search` (semantic clarity for non-search expressions) |
| `repl [--repo <path>]` | Interactive REPL for queries |
| `orphans [--repo <path>]` | Find docs with no incoming links |
| `broken [--repo <path>]` | Find broken links across docs |
| `stale [--repo <path>]` | Find docs drifting from source code |
| `health [--changed] [--since <gitRef>] [--limit <n>] [--repo <path>]` | All checks: orphans + broken + stale + density, with optional changed-scope compaction hints |
| `check <text\|file> [--repo <path>] [--expr]` | Detect novel knowledge in text or a file; `--expr` for JS expression mode |
| `fn add <name> <body> [opts]` | Define a reusable function |
| `fn list [--repo <path>]` | List saved functions |
| `fn rm <name> [--repo <path>]` | Remove a function |
| `--help` | Show help |

Use `--quiet` when a `--json` workflow needs clean combined-stream output. It suppresses
informational stderr diagnostics (progress / execution-truth markers) but still leaves real
warnings and errors on stderr.

## Query Language

Queries are JavaScript expressions evaluated by [Jint](https://github.com/sebastienros/jint). All primitives are available as globals and can be composed freely.

```bash
# Simple search
knowledge-sight search "search('auth')"

# Chain: search then expand the top result
knowledge-sight search "search('auth'); expand(R1)"

# Multi-step: find a file's context, then walk its link graph
knowledge-sight search "context('docs/architecture.md')"
knowledge-sight search "walk('docs/architecture.md', {depth: 3})"

# Combine results
knowledge-sight search "search('auth').concat(grep('JWT'))"
```

### Composition Helpers

In addition to the query primitives, four composition helpers are available as globals:

| Helper | Description |
|--------|-------------|
| `pipe(value, fn1, fn2, ...)` | Thread a value through a series of functions sequentially |
| `tap(value, fn)` | Run `fn` for side-effects (e.g. debugging), return `value` unchanged |
| `mergeBy(key, arr1, arr2, ...)` | Union multiple arrays with dedup by the specified key field |
| `print(value)` | Debug output to stderr (Jint has no `console` object — use `print` instead) |

```js
// Thread search results through similar then expand
pipe(search('auth'), function(r) { return similar(r[0].id) })

// Debug without breaking chains
tap(search('auth'), function(r) { print('found ' + r.length + ' results') })

// Combine results from different primitives with dedup
mergeBy('id', search('auth'), grep('authentication'))
```

### Primitives

| Primitive | Description |
|-----------|-------------|
| `search(query, {limit, tag, file, status})` | Semantic search across indexed chunks |
| `catalog({status})` | Topic map of indexed docs |
| `context(file)` | Overview of a file with sections, backlinks, outlinks, and indexed frontmatter |
| `expand(refId)` | Expand an `R#` ref to full chunk content |
| `neighborhood(refId, {before, after})` | Surrounding sections around a ref |
| `similar(refId, {limit, status})` | Semantically similar chunks |
| `grep(pattern, {limit, file, status})` | Regex search over chunk content |
| `mentions(term, {limit, status})` | Find term mentions across docs |
| `files(pattern)` | List indexed files |
| `backlinks(file, {status})` | Incoming links to a file |
| `links(file, {status})` | Outgoing links from a file |
| `pinned({tier})` | Ambient docs for the requested tier (`grammar` by default) |
| `orphans({status})` | Docs with no incoming links |
| `broken({status})` | Broken links across docs |
| `placement(content, {limit, status})` | Suggest where new content fits |
| `walk(file, {depth, direction, status})` | Traverse the link graph |
| `novelty(text, {threshold, status})` | Detect novel knowledge in text |
| `propose(text, {team, cycle, ...})` | File novel chunks into `inbox/{team}/` with advisory placement + warnings |
| `triage({team, before, limit})` | List pending inbox items awaiting disposition |
| `dispose(refId, {action, ...})` | Promote, merge, or reject one inbox item |
| `supersede(refId, newContent, {reason, by, verify})` | Replace one active canonical doc with a versioned sibling while preserving the old doc for audit |
| `reverify({scope, apply})` | Verify recheck for active canonical docs (`scope` is default-all or stable repo-relative canonical selectors: exact file, dir, glob; `apply:true` only marks drifting active docs stale) |
| `conflicts({scope, threshold})` | Read-only similarity candidates across pending inbox + active canonical docs (core surface; add `{pairs:true, judge:true}` for judged pairs; advanced filters remain compatibility-only) |
| `prune({scope, olderThanDays, apply})` | Preview prune candidates by default; `apply:true` deletes only initially eligible `stale` / `superseded` / `deprecated` canonical docs |
| `cluster(dir, {threshold, status})` | Cluster docs by similarity |
| `gaps({scope, min_docs, signal})` | Find coverage gaps |
| `hygiene({profile, limit})` | Experimental role-aware hygiene report / ranked compaction shortlist |
| `changed(gitRef)` | Chunks in files changed since a git ref |
| `explain(refId)` | Full index metadata, findSource diagnosis, and indexed frontmatter |

### Status defaults

The §5 status rollout is now live on the bounded read/query surface:

- `search`, `similar`, `grep`, `mentions`, `walk`, `backlinks`, `links`, `catalog`, `cluster`, `placement`, and `novelty` accept `{status:[...]}` and default to `['active']`
- blank-status canonical docs still behave as active by default
- inbox docs stay `pending` and are excluded from ordinary reads unless explicitly requested
- `expand(ref)` and `explain(ref)` stay filter-independent explicit-ref escapes
- `files()` and `context()` are intentionally unchanged

Maintenance defaults remain design-specific rather than inheriting plain retrieval defaults:

- `orphans()` defaults to all statuses except `pending`
- `broken()` checks links from `active` + `stale` docs
- `stale` / `health` CLI checks default to active canonical docs only

### Experimental hygiene workflow

The first KnowledgeSight hygiene slice is exposed through the existing `search` / `eval`
surface so we do not prematurely lock a broad CLI API:

```bash
# Human-readable report
knowledge-sight eval "hygiene()"

# Machine-readable report
knowledge-sight eval "hygiene()" --json

# Ranked compaction shortlist for iterative edit loops
knowledge-sight eval "hygiene({profile:'compaction'})"
knowledge-sight eval "hygiene({profile:'compaction', limit:8})" --json
```

The report is role-aware, non-destructive-first, and evidence-backed. It is designed to
surface candidate canonical owners, stale-prone live-status duplication, bounded live-summary
protection, section-level preserve/reduce/review guidance for long-lived design/decision/research
notes plus decision-index registries, and explainable orphan/gap warnings using a bounded action
vocabulary — not to auto-edit docs.

The compaction profile stays on the same experimental `hygiene()` surface. It now takes a bounded
fast path before the broad hygiene pipeline, returns a small ranked shortlist of action-worthy
compaction candidates (duplicate active-state drift, chronology-heavy residue, canonical-owner
conflicts, and section-level compaction candidates), and leaves the broad report-first workflow plus
protected-genre controls intact.

When you run compaction mode with `--json`, stdout remains a pure JSON finding array. Execution truth
for the fast path (profile applied, bounded work, bypassed broad stages, shortlist counts) is emitted
on stderr as `[hygiene/compaction] ...` markers by default so machine verifiers can prove
deployment/work budgeting without polluting JSON rows. Add `--quiet` only when you need clean
combined-stream JSON; warnings/errors still stay on stderr.

For habitual edit-loop use, the ambient surface is `knowledge-sight health --changed`. It keeps plain
`health` unchanged, scopes compaction hints to changed/current docs first (or an explicit `--since`
window), and prints a tiny action-shaped shortlist with file, section, distraction reason, suggested
replacement shape, and canonical owner when available. The deeper `hygiene({profile:'compaction', ...})`
surface remains available for inspection/debugging; ambient mode is the wrapper for normal use.

Heuristic tuning is regression-checked with machine-readable local fixtures kept outside source
control, so reviewability budgets, suppression-ledger accountability, operator action-type
clusters, and legacy-vs-new-family row splits can still be inspected without freezing a broader
public API around those internal probes.

### Session Primitives

| Primitive | Description |
|-----------|-------------|
| `saveSession(name)` | Save current ref IDs and results to `{indexDir}/sessions/{name}.json` |
| `loadSession(name)` | Restore a previously saved session |
| `sessions()` | List all saved sessions |

### Refs

Search results are assigned ref IDs (`R1`, `R2`, ...) that persist across queries. Use `expand(R1)` or `similar(R1)` to drill into previous results. Bare `R123` tokens are auto-quoted to `'R123'` before evaluation (ref-ID shorthand).

## Functions

Save chains of primitives as reusable, named functions. Functions are **per-repo** — each repo has its own set, stored in `knowledge-sight.functions.json` at the repo root.

### Defining Functions

```bash
# Simple function with no parameters
knowledge-sight fn add overview --desc "Quick topic map" "catalog()"

# Function with parameters
knowledge-sight fn add deepSearch --params "q" --desc "Search + similar expansion" \
  "search(q).concat(similar(search(q)[0]))"

# Multi-parameter function
knowledge-sight fn add scopedGrep --params "pattern,file" \
  "grep(pattern, {file: file})"

# Read body from a file (useful for complex functions)
knowledge-sight fn add audit --file audit-fn.js --desc "Full doc audit"
```

### Using Functions

Once defined, functions are available in any `search` expression:

```bash
knowledge-sight search "deepSearch('authentication')"
knowledge-sight search "scopedGrep('TODO', 'docs/roadmap.md')"
```

### Managing Functions

```bash
# List all functions (compact: name, params, description)
knowledge-sight fn list

# Full details including function body
knowledge-sight fn list --verbose

# Machine-readable JSON output
knowledge-sight fn list --json

# Remove a function
knowledge-sight fn rm deepSearch
```

### Options for `fn add`

| Option | Description |
|--------|-------------|
| `--params "a,b"` | Comma-separated parameter names |
| `--desc "..."` | Optional description |
| `--file <path>` | Read function body from a file instead of inline |

### Storage

Functions are stored in `knowledge-sight.functions.json` in the repo root. This file is meant to be committed alongside your docs so the whole team shares the same functions.

> **Note:** Functions are available in `search` and `eval` expressions. The `check` command also supports UDFs when invoked with `--expr` (see below).

## The `eval` Command

`eval` is an alias for `search`. Use it when your expression isn't really a "search" — e.g. `eval "orphans().concat(broken())"`. Behavior is identical.

## Check with `--expr`

By default, `check` runs the plain-text novelty pipeline. The `--expr` flag switches to JS expression mode with full access to primitives and user-defined functions:

```bash
# Plain text — novelty classification (default)
knowledge-sight check "The stanza parser must handle malformed XML."

# Expression mode — full QueryEngine with UDFs
knowledge-sight check "novelty('stanza parser handles malformed XML').filter(function(r) { return r.status === 'novel' })" --expr
```

## Improved Result Formatting

Mixed result arrays (e.g. `orphans().concat(broken())`) now format cleanly. Each result item carries internal type metadata, so the formatter applies deterministic per-item formatting instead of guessing the shape of the entire array. No action needed — this is automatic.

## Configuration

Create a `knowledge-sight.json` in the repo root to customize behavior:

```json
{
  "docDirs": ["docs", "wiki"],
  "exclude": ["node_modules", "bin", "obj", ".git"],
  "indexDir": ".knowledge-sight",
  "embeddingUrl": "http://localhost:1234/v1/embeddings",
  "embeddingBatchSize": 50,
  "completionUrl": "http://localhost:1234/v1/chat/completions",
  "conflictJudgeModel": "judge-mini",
  "inboxDir": "inbox",
  "archiveProcessed": true,
  "requireFields": ["verify", "concept"],
  "requireFieldsMode": "warn",
  "promoteCollision": "suffix"
}
```

All fields are optional. Defaults:

| Field | Default |
|-------|---------|
| `docDirs` | Auto-detected from `.agents`, `docs`, `doc`, `wiki`, `knowledge`, or `.` |
| `exclude` | `node_modules`, `bin`, `obj`, `.git`, `wwwroot`, `dist`, `.code-intel` |
| `indexDir` | `.knowledge-sight` |
| `embeddingUrl` | `http://localhost:1234/v1/embeddings` |
| `embeddingBatchSize` | `50` |
| `completionUrl` | `""` |
| `conflictJudgeModel` | `""` |
| `inboxDir` | `inbox` |
| `archiveProcessed` | `true` |
| `requireFields` | `["verify","concept"]` |
| `requireFieldsMode` | `warn` |
| `promoteCollision` | `suffix` |

`inboxDir` participates in index scans for the minimal v1 bus loop even when `docDirs` is configured as
canonical-only (for example `["docs"]`). That keeps filed inbox claims visible to `triage()` /
`dispose()` without requiring callers to mirror `inboxDir` into `docDirs`. If a repo already keeps inbox
content under a configured doc dir (for example `knowledge-base/inbox`), set `inboxDir` to that explicit
repo-relative path; bare `inboxDir:"inbox"` stays repo-root and now errors when the repo layout makes the
intended inbox root ambiguous.

`requireFieldsMode: "warn"` (default) files novel chunks and returns advisory `warnings` for missing
required fields. `requireFieldsMode: "error"` keeps the same default/known/paragraph-by-paragraph behavior,
but refuses novel chunks missing configured required fields before inbox write/indexing while still
returning advisory `warnings` and `suggestedVerify` when a ranked placement candidate exposes persisted
`related` evidence.

`completionUrl` and `conflictJudgeModel` are only needed for the bounded judged conflicts-pairs path:
`conflicts({pairs:true, judge:true})`, including optional deterministic `verdicts:[...]` filtering and
`rollup:true`, plus the bounded candidate gates on `conflicts({pairs:true, judge:true, rollup:true,
duplicatesOnly:true})`, `conflicts({pairs:true, judge:true, rollup:true, hasConflict:true})`,
`conflicts({pairs:true, judge:true, rollup:true, mixedVerdicts:true})`,
`conflicts({pairs:true, judge:true, rollup:true, compatibleOnly:true})`, and
`conflicts({pairs:true, judge:true, rollup:true, conflictOnly:true})`, and
`conflicts({pairs:true, judge:true, rollup:true, noConflict:true})`. The first cut keeps a fixed 5s
per-pair completion timeout and bounded-errors the whole judged call on missing config, timeout/transport
failure, or malformed model output. Optional `verdicts:[...]`, `rollup:true`, `duplicatesOnly:true`,
`hasConflict:true`, `mixedVerdicts:true`, `compatibleOnly:true`, `conflictOnly:true`, and `noConflict:true` stay purely
deterministic on top of that shipped judged-pair path and do not add extra completion calls.

The bounded write loop now ships four disposition surfaces:

- `propose()` files novel chunks into `inbox/{team}/`; `cycle` accepts either the existing filename-safe UTC form or a non-negative integer id (stored as a canonical decimal string), integer cycles keep blank `triage()` age because they are ids rather than timestamps, mixed-form `triage()` lists group UTC rows before integer rows, and `triage({before:...})` now bounded-errors on mixed UTC/integer inboxes instead of silently paging only one form; `requireFieldsMode: "warn"` keeps warning-only behavior while `requireFieldsMode: "error"` blocks novel chunks missing configured required fields before inbox write
- the propose intake gate stays bounded: terse declarative/prescriptive short claims can still file, while clearly hedged/musing short text still returns `blocked`
- `triage()` lists pending inbox items with `missing` metadata
- `dispose(..., {action:'promote'|'merge'|'reject'})` closes the curator loop
- `merge` is now hardened: explicit targets canonicalize under repo/doc-dir containment, inbox/non-canonical targets are rejected before write, retries dedupe by deterministic merge identity, and concurrent canonical edits fail with conflict instead of clobbering

The bounded `reverify()` foundation remains the active verification boundary:

- `reverify({scope})` remains bounded to the first-cut deterministic sandbox and explicit-scope contract
- `reverify({scope, apply:true})` only marks `drift` results on currently active canonical docs as `status: stale`
- default scope is all active canonical docs carrying `verify`; explicit scope now accepts only stable repo-relative canonical selectors: exact file paths, canonical dirs, glob patterns, and arrays thereof
- the first-cut sandbox allows `grep`, `mentions`, `files`, `backlinks`, `links`, `context`, and deterministic `search()`, plus `pipe` / `tap` / `mergeBy`
- `search()` in verify requires persisted deterministic query-embedding material captured on the write path; legacy/no-cache docs fail explicitly instead of falling back to live embedding HTTP
- `dispose(promote)`, `dispose(merge)`, and `supersede()` now fail closed and roll back their write if deterministic `search()` capture cannot complete for the resulting active canonical doc
- all other embedding-dependent primitives still stay out of the sandbox
- verify results are normalized and hashed into `verify_snapshot`; current `dispose(promote)`, hardened `dispose(merge)`, and bounded `supersede()` now capture both `verify_snapshot` and deterministic verify-time `search()` query embeddings when the expression uses `search()`
- explicit scope still resolves only to active canonical docs carrying `verify`; inbox/pending, stale, superseded, deprecated, and unverified docs remain invalid targets
- `apply:true` never mutates `ok`, `no_snapshot`, or `error`, never rewrites doc body / `verify_snapshot`, and refuses concurrent or busy targets with the same bounded `changed concurrently` conflict used by hardened merge

The bounded `conflicts()` candidate surface is now available as a read-only curator check:

- `conflicts({scope, threshold})` is the compact advertised core and defaults to pending inbox docs plus active canonical docs
- it reuses persisted index embeddings / clustering internals only; no live query embeddings or completion calls
- if indexed docs resolve on read surfaces but the selected conflicts scope has zero usable persisted semantic anchors, `conflicts()` now errors explicitly for semantic unavailability instead of misreporting a scope selector no-match
- default `conflicts()` and `conflicts({pairs:true})` remain unjudged; judged annotations are opt-in only on derived pairs
- optional `pairs:true` additively expands each candidate into deterministic unique unordered doc pairs for inspection; omitted `pairs` keeps the existing cluster-only shape
- explicit `scope` now accepts only stable repo-relative exact file / dir / glob selectors (and arrays thereof)
- selector validation is all-or-nothing: any selector touching unsupported docs or matching zero supported docs bounded-errors the call instead of partial-succeeding
- `judge:true` is supported only with `pairs:true`; `conflicts({judge:true})` bounded-errors to avoid ambiguous cluster-level verdicts
- judged pairs require configured `completionUrl` + `conflictJudgeModel`, run after deterministic pair derivation, and bounded-error the whole call on timeout/transport/schema failures
- advanced judged-filter knobs such as `verdicts`, `profile` / `profiles`, and the existing candidate-gate booleans (`duplicatesOnly`, `hasConflict`, `mixedVerdicts`, `compatibleOnly`, `conflictOnly`, `noConflict`) remain supported for compatibility in this wave; the help surface is smaller, but the parser/semantics are intentionally unchanged
- optional `verdicts:[...]` is supported only with `pairs:true, judge:true`; it filters the visible judged `pairs` array to the retained verdict set, omits candidates whose retained visible pair set is empty, and never makes extra completion calls
- optional `rollup:true` is supported only with `pairs:true, judge:true`; it additively summarizes deterministic facts from that same visible retained judged pair set only (including `judgedPairs`, `verdictCounts`, `mixedVerdicts`, explicit predicate booleans `duplicateOnly`, `hasConflict`, `compatibleOnly`, `conflictOnly`, and `noConflict`, plus one additive `profile` label: `duplicateOnly`, `compatibleOnly`, `conflictOnly`, `noConflictMixed`, or `mixedWithConflict`) so returned `pairs`, returned `rollup`, and additive `profile` stay aligned, and it never makes extra completion calls
- optional `profile:'...'` is supported only with `pairs:true, judge:true, rollup:true`; omission still preserves the shipped path only when the `profile` key is absent, while explicit blank / null / unsupported values bounded-error with no trimming / case-folding / aliases. When present, it retains only candidates whose current visible rollup `profile` exactly matches one supported label (`duplicateOnly`, `compatibleOnly`, `conflictOnly`, `noConflictMixed`, or `mixedWithConflict`). If `verdicts:[...]` is also present, `verdicts:[...]` runs first, visible `rollup` / `profile` recompute from that same retained visible pair set only, zero-retained-pair omission stays as shipped, and only then does the exact profile match run. Outside the bounded lifts below, candidate-gate combinations with `profile:'...'` still reject. In one bounded lift, `conflicts({pairs:true, judge:true, rollup:true, noConflict:true, profile:'noConflictMixed'})` is accepted only when the `verdicts` key is absent: full-set `noConflict:true` retention still runs first, then current visible full-set rollup `profile` membership keeps only retained `profile='noConflictMixed'` candidates, unanimous duplicate-only / compatible-only no-conflict candidates are excluded without hidden verdict filtering, and conflict-bearing candidates stay excluded before profile membership. In another bounded lift, `conflicts({pairs:true, judge:true, rollup:true, hasConflict:true, profile:'mixedWithConflict'})` is accepted only when the `verdicts` key is absent: full-set `hasConflict:true` retention still runs first, then current visible full-set rollup `profile` membership keeps only retained `profile='mixedWithConflict'` candidates, conflict-only candidates are excluded without hidden verdict filtering, and every other `hasConflict:true + profile:'...'` input still rejects. In another bounded lift, `conflicts({pairs:true, judge:true, rollup:true, mixedVerdicts:true, profile:'mixedWithConflict'})` is accepted only when the `verdicts` key is absent: full-set `mixedVerdicts:true` retention still runs first, then current visible full-set rollup `profile` membership keeps only retained `profile='mixedWithConflict'` candidates, retained `profile='noConflictMixed'` candidates and conflict-only candidates are excluded without hidden verdict filtering, and every other `mixedVerdicts:true + profile:'...'` input still rejects. In another bounded lift, `conflicts({pairs:true, judge:true, rollup:true, conflictOnly:true, profile:'conflictOnly'})` is accepted only when the `verdicts` key is absent: full-set `conflictOnly:true` retention still runs first, then current visible full-set rollup `profile` membership keeps only retained `profile='conflictOnly'` candidates, mixed conflict-bearing and no-conflict candidates are excluded before profile membership, and every other `conflictOnly:true + profile:'...'` input still rejects. In another bounded lift, `conflicts({pairs:true, judge:true, rollup:true, duplicatesOnly:true, profile:'duplicateOnly'})` is accepted only when the `verdicts` key is absent: full-set `duplicatesOnly:true` retention still runs first, then current visible full-set rollup `profile` membership keeps only retained `profile='duplicateOnly'` candidates, mixed conflict-bearing and no-conflict candidates are excluded before profile membership, and every other `duplicatesOnly:true + profile:'...'` input still rejects. In another bounded lift, `conflicts({pairs:true, judge:true, rollup:true, compatibleOnly:true, profile:'compatibleOnly'})` is accepted only when the `verdicts` key is absent: full-set `compatibleOnly:true` retention still runs first, then current visible full-set rollup `profile` membership keeps only retained `profile='compatibleOnly'` candidates, mixed conflict-bearing and no-conflict candidates are excluded before profile membership, and every other `compatibleOnly:true + profile:'...'` input still rejects
- optional `profiles:[...]` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose current visible rollup `profile` exactly matches one requested supported label (`duplicateOnly`, `compatibleOnly`, `conflictOnly`, `noConflictMixed`, or `mixedWithConflict`). Validation preserves raw array entry presence before any set conversion or helper reuse: blank entries, null entries, empty arrays, unsupported labels, and duplicate supported labels bounded-error explicitly with no filtering / normalization / dedupe. If `verdicts:[...]` is also present, `verdicts:[...]` still runs first, zero-retained-pair omission still happens before any membership check, visible `rollup` / `profile` still recompute from that same retained visible pair set only, and only then does the exact OR-match run. `profile:'...'` plus `profiles:[...]` still hard-rejects, and every candidate-gate combination with `profiles:[...]` (`duplicatesOnly:true`, `hasConflict:true`, `mixedVerdicts:true`, `compatibleOnly:true`, `conflictOnly:true`, and `noConflict:true`) still hard-rejects so the filter stays fully on the existing visible judged-rollup seam
- optional `duplicatesOnly:true` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose full judged pair set is duplicate-only, preserves surviving candidate cluster context plus visible full judged `pairs` and `rollup` shape exactly as shipped, and bounded-errors outside that explicit path so there is no hidden implicit rollup. In the current bounded lift, `verdicts:[...]` is accepted on this path only for the exact raw `['duplicate']` singleton: full-set `duplicatesOnly:true` retention runs first, then visible duplicate-only filtering runs only on retained duplicate-only candidates, the generic zero-retained visible-pair omission guard remains explicit, and visible `rollup` plus additive `profile='duplicateOnly'` stay derived from that same retained visible duplicate-only pair set only. Raw-shape gating must happen before normalization/reordering so exact empty `[]`, repeated-entry arrays, and broader raw-present arrays stay rejected. Any other `duplicatesOnly:true` plus `verdicts:[...]` set still bounded-errors, and every `hasConflict:true`, `mixedVerdicts:true`, `compatibleOnly:true`, `conflictOnly:true`, and `noConflict:true` combination with `duplicatesOnly:true` still hard-rejects in the first cut
- optional `hasConflict:true` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose full judged pair set includes at least one conflict verdict, preserves surviving candidate cluster context plus visible full judged `pairs` and `rollup` shape exactly as shipped, and bounded-errors outside that explicit path so there is no hidden implicit rollup. In one bounded lift, `profile:'mixedWithConflict'` is accepted on this path only when the `verdicts` key is absent: full-set `hasConflict:true` retention runs first, then visible profile membership keeps only retained mixed conflict-bearing candidates whose current full visible rollup `profile` is exactly `mixedWithConflict`, while conflict-only retained candidates are excluded without hidden verdict filtering or rollup recomputation. In the current verdict-filter lift, `verdicts:[...]` is accepted on this path only for the exact raw `['conflict']`, `['duplicate']`, and `['compatible']` filters plus the exact raw `['conflict','duplicate']`, `['duplicate','conflict']`, `['conflict','compatible']`, `['compatible','conflict']`, `['duplicate','compatible']`, and `['compatible','duplicate']` shapes: full-set `hasConflict:true` retention runs first, then visible filtering runs only on retained conflict-bearing candidates, the generic zero-retained visible-pair omission guard remains explicit, and visible `rollup` stays derived from that same retained visible pair set only. On the exact raw `['conflict']` lane, visible conflict-only filtering must still preserve the conflict-only visible pair set exactly as shipped. On the exact raw `['duplicate']` lane, visible duplicate-only filtering must still run only after full-set conflict-bearing retention, and visible `rollup` plus additive `profile='duplicateOnly'` must derive only from that same retained visible duplicate-only pair set. On the exact raw `['compatible']` lane, visible compatible-only filtering must still run only after full-set conflict-bearing retention, and visible `rollup` plus additive `profile='compatibleOnly'` must derive only from that same retained visible compatible-only pair set. On the exact raw `['conflict','duplicate']` and `['duplicate','conflict']` lanes, visible conflict+duplicate filtering must still derive visible `rollup` plus additive `profile='mixedWithConflict'` only from that same retained visible pair set. On the exact raw `['conflict','compatible']` and `['compatible','conflict']` lanes, visible conflict+compatible filtering must still derive visible `rollup` plus additive `profile='mixedWithConflict'` only from that same retained visible pair set. On the exact raw `['duplicate','compatible']` and `['compatible','duplicate']` lanes, visible duplicate+compatible filtering must still run only after full-set conflict-bearing retention; on the fully mixed retained surface it must derive visible `rollup` plus additive `profile='noConflictMixed'` only from that retained visible duplicate+compatible pair set, and on a dedicated conflict+compatible retained scope it must narrow honestly to compatible-only visible output without erroring. Raw-shape gating must happen before normalization/reordering so exact empty `[]`, repeated-entry arrays, and broader raw-present arrays stay rejected. Any other `hasConflict:true + profile:'...'` or `hasConflict:true + verdicts:[...]` set still bounded-errors, and `duplicatesOnly:true` with `hasConflict:true` still hard-rejects in the first cut
- optional `mixedVerdicts:true` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose full judged pair set spans more than one verdict category, preserves surviving candidate cluster context plus visible full judged `pairs` and `rollup` shape exactly as shipped, and bounded-errors outside that explicit path so there is no hidden implicit rollup. In one bounded lift, `profile:'mixedWithConflict'` is accepted on this path only when the `verdicts` key is absent: full-set `mixedVerdicts:true` retention runs first, then visible profile membership keeps only retained mixed-verdict candidates whose current full visible rollup `profile` is exactly `mixedWithConflict`, while retained `profile='noConflictMixed'` candidates and conflict-only candidates are excluded without hidden verdict filtering or rollup recomputation. In one current verdict-filter lift, `verdicts:[...]` is accepted on this path only for the exact `['conflict']` filter, the exact raw `['duplicate']` and `['compatible']` singletons, and the exact raw `['conflict','duplicate']`, `['duplicate','conflict']`, `['conflict','compatible']`, `['compatible','conflict']`, `['duplicate','compatible']`, and `['compatible','duplicate']` shapes: full-set `mixedVerdicts:true` retention runs first, then visible filtering runs only on retained mixed candidates, zero-retained visible pair sets still omit the candidate before visible rollup reuse/recomputation, and visible `rollup` stays derived from that same retained visible pair set only. On the exact raw `['duplicate']` lane, visible duplicate-only filtering must still run only after full-set mixed retention, and visible `rollup` plus additive `profile='duplicateOnly'` must derive only from that same retained duplicate-only visible pair set. On the exact raw `['compatible']` lane, visible compatible-only filtering must still run only after full-set mixed retention, and visible `rollup` plus additive `profile='compatibleOnly'` must derive only from that same retained compatible-only visible pair set. On the exact raw `['conflict','duplicate']`, `['duplicate','conflict']`, `['conflict','compatible']`, `['compatible','conflict']`, `['duplicate','compatible']`, and `['compatible','duplicate']` lanes, raw-shape gating must happen before normalization/reordering so repeated-entry arrays and broader raw-present arrays stay rejected. In one narrower intersection lift, `conflicts({pairs:true, judge:true, verdicts:['duplicate'], rollup:true, noConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['compatible'], rollup:true, noConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['duplicate','compatible'], rollup:true, noConflict:true, mixedVerdicts:true})`, and `conflicts({pairs:true, judge:true, verdicts:['compatible','duplicate'], rollup:true, noConflict:true, mixedVerdicts:true})` are also accepted: both candidate gates still run full-set-first on the same full judged pair set / existing rollup facts, then visible verdict filtering runs only on retained no-conflict-mixed candidates, and visible `rollup` / additive `profile` recompute only from that same retained visible pair set. Any broader `mixedVerdicts:true` plus `verdicts:[...]` set still bounded-errors. In a separate bounded lift, the bare `hasConflict:true` + `mixedVerdicts:true` intersection is accepted when the `verdicts` key is absent end-to-end, and ten exact raw-present lanes are also accepted on that same intersection: `conflicts({pairs:true, judge:true, verdicts:['conflict'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['duplicate'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['compatible'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['conflict','duplicate'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['duplicate','conflict'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['conflict','compatible'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['compatible','conflict'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['duplicate','compatible'], rollup:true, hasConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['compatible','duplicate'], rollup:true, hasConflict:true, mixedVerdicts:true})`, and `conflicts({pairs:true, judge:true, verdicts:['conflict','duplicate','compatible'], rollup:true, hasConflict:true, mixedVerdicts:true})`. Both gates still stay full-set-first on the same full judged pair set / existing rollup facts before any visible verdict filtering runs. On the shipped bare path, retained candidates keep visible full judged `pairs` plus full visible `rollup` / additive `profile` exactly as shipped. On the exact raw `['conflict']` lane, visible conflict-only filtering runs only after both full-set gates retain mixed conflict-bearing candidates, zero-retained visible pair sets still omit before visible rollup/profile recomputation, and visible `rollup` plus additive `profile='conflictOnly'` derive only from that same retained visible conflict pair set. On the exact raw `['duplicate']` lane, visible duplicate-only filtering runs only after both full-set gates retain mixed conflict-bearing candidates; on the fully mixed retained surface it must derive visible `rollup` plus additive `profile='duplicateOnly'` only from that retained visible duplicate pair set, and on the dedicated conflict+duplicate retained scope it must narrow honestly to duplicate-only visible output without erroring. On the exact raw `['compatible']` lane, visible compatible-only filtering runs only after both full-set gates retain mixed conflict-bearing candidates; on the fully mixed retained surface it must derive visible `rollup` plus additive `profile='compatibleOnly'` only from that retained visible compatible pair set, and on the dedicated conflict+compatible retained scope it must narrow honestly to compatible-only visible output without erroring. On the exact raw `['conflict','duplicate']` and `['duplicate','conflict']` lanes, visible conflict+duplicate filtering runs only after both full-set gates retain mixed conflict-bearing candidates; on the fully mixed retained surface they must derive visible `rollup` plus additive `profile='mixedWithConflict'` only from that retained visible conflict+duplicate pair set, and the shipped ordered lane remains the positive control while the reordered lane proves raw-shape acceptance still happens before normalization/reordering. On the exact raw `['conflict','compatible']` and `['compatible','conflict']` lanes, visible conflict+compatible filtering likewise runs only after both full-set gates retain mixed conflict-bearing candidates and must derive visible `rollup` plus additive `profile='mixedWithConflict'` only from the retained visible conflict+compatible pair set; the shipped ordered lane remains the positive control while the reordered lane proves raw-shape acceptance still happens before normalization/reordering. On the exact raw `['duplicate','compatible']` and `['compatible','duplicate']` lanes, visible duplicate+compatible filtering likewise runs only after both full-set gates retain mixed conflict-bearing candidates; on the fully mixed retained surface they must derive visible `rollup` plus additive `profile='noConflictMixed'` only from that retained visible duplicate+compatible pair set, and on the dedicated conflict+compatible retained scope they must narrow honestly to compatible-only visible output without erroring. On the exact raw `['conflict','duplicate','compatible']` lane, both gates still retain the same fully mixed conflict-bearing candidate set first, then visible verdict filtering keeps the full retained visible pair set unchanged so visible `pairs`, visible `rollup`, and additive `profile='mixedWithConflict'` remain exactly aligned with the shipped bare intersection instead of opening any new reordered triple alias. Any other `mixedVerdicts:true + profile:'...'` or raw-present `verdicts` input on that seam or on the bare `hasConflict:true + mixedVerdicts:true` intersection (including `[]`, repeated arrays, reordered triple arrays, and broader arrays) still bounded-errors. `duplicatesOnly:true` plus `hasConflict:true` / `mixedVerdicts:true` combinations still hard-reject in the first cut
- optional `compatibleOnly:true` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose full judged pair set is unanimously compatible, preserves surviving candidate cluster context plus visible full judged `pairs` and `rollup` shape exactly as shipped, and bounded-errors outside that explicit path so there is no hidden implicit rollup. In the current bounded lift, `verdicts:[...]` is accepted on this path only for the exact raw `['compatible']` singleton: full-set `compatibleOnly:true` retention runs first, then visible compatible-only filtering runs only on retained compatible-only candidates, the generic zero-retained visible-pair omission guard remains explicit, and visible `rollup` plus additive `profile='compatibleOnly'` stay derived from that same retained visible compatible-only pair set only. Raw-shape gating must happen before normalization/reordering so exact empty `[]`, repeated-entry arrays, and broader raw-present arrays stay rejected. Any other `compatibleOnly:true` plus `verdicts:[...]` set still bounded-errors, and every `duplicatesOnly:true`, `hasConflict:true`, `mixedVerdicts:true`, `conflictOnly:true`, and `noConflict:true` combination with `compatibleOnly:true` still hard-rejects in the first cut
- optional `conflictOnly:true` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose full judged pair set is unanimously conflict, preserves surviving candidate cluster context plus visible full judged `pairs` and `rollup` shape exactly as shipped, and bounded-errors outside that explicit path so there is no hidden implicit rollup. In the current bounded lift, `verdicts:[...]` is accepted on this path only for the exact raw `['conflict']` singleton: full-set `conflictOnly:true` retention runs first, then visible conflict-only filtering runs only on retained conflict-only candidates, the generic zero-retained visible-pair omission guard remains explicit, and visible `rollup` plus additive `profile='conflictOnly'` stay derived from that same retained visible conflict-only pair set only. Raw-shape gating must happen before normalization/reordering so exact empty `[]`, repeated-entry arrays, and broader raw-present arrays stay rejected. Any other `conflictOnly:true` plus `verdicts:[...]` set still bounded-errors, and every `duplicatesOnly:true`, `hasConflict:true`, `mixedVerdicts:true`, and `compatibleOnly:true` combination with `conflictOnly:true` still hard-rejects in the first cut
- optional `noConflict:true` is supported only with `pairs:true, judge:true, rollup:true`; it retains only candidates whose full judged pair set contains zero conflict verdicts, preserves surviving candidate cluster context plus visible full judged `pairs` and `rollup` shape exactly as shipped, and bounded-errors outside that explicit path so there is no hidden implicit rollup. In one current bounded lift, `verdicts:[...]` is accepted on this path only when every requested verdict is within `duplicate|compatible`: full-set `noConflict:true` retention runs first, then visible verdict filtering runs, zero-retained visible pair sets omit the candidate before visible rollup reuse/recomputation, and visible `rollup` stays derived from that same retained visible pair set only. Any `verdicts:[...]` set containing `conflict` still bounded-errors. In a separate bounded lift, the bare `noConflict:true` + `mixedVerdicts:true` intersection is accepted only when the `verdicts` key is absent end-to-end: both gates stay full-set-first on the same full judged pair set / existing rollup facts, retained candidates keep visible full judged `pairs` plus full visible `rollup` / additive `profile='noConflictMixed'` exactly as shipped, and outside the exact composition lifts below any other raw-present `verdicts` input on that bare intersection (including `[]` and `['conflict']`) still bounded-errors. In exact composition lifts on that same intersection, `conflicts({pairs:true, judge:true, verdicts:['duplicate'], rollup:true, noConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['compatible'], rollup:true, noConflict:true, mixedVerdicts:true})`, `conflicts({pairs:true, judge:true, verdicts:['duplicate','compatible'], rollup:true, noConflict:true, mixedVerdicts:true})`, and `conflicts({pairs:true, judge:true, verdicts:['compatible','duplicate'], rollup:true, noConflict:true, mixedVerdicts:true})` are accepted: both gates still run full-set-first on the same full judged pair set / existing rollup facts, then visible verdict filtering runs on the retained candidate, zero-retained visible pair sets still omit before visible rollup/profile recomputation, and visible `rollup` plus additive `profile` derive only from that same retained visible pair set. In one smaller bounded lift on that same seam, `conflicts({pairs:true, judge:true, rollup:true, noConflict:true, profile:'noConflictMixed'})` is accepted only when the `verdicts` key is absent: full-set `noConflict:true` retention still runs before profile membership, retained `profile='noConflictMixed'` candidates survive unchanged, unanimous duplicate-only / compatible-only retained candidates are excluded without hidden verdict filtering, and conflict-bearing candidates remain excluded before profile membership. Every other `noConflict:true + profile:'...'` or `noConflict:true + profiles:[...]` input still rejects, and every `duplicatesOnly:true`, `hasConflict:true`, `compatibleOnly:true`, and `conflictOnly:true` combination with `noConflict:true` still hard-rejects in the first cut

The bounded `prune()` surface now has preview and explicit apply lanes:

- `prune({scope, olderThanDays})` remains preview-only/read-only over `stale`, `superseded`, and `deprecated` canonical docs only
- `prune({scope, olderThanDays, apply:true})` deletes only docs that the same call's initial preview evaluation already marks `eligible:true`
- default `olderThanDays` is `30`; explicit `scope` reuses the stable repo-relative exact file / dir / glob selector contract (and arrays thereof)
- preview facts remain explicit on both lanes: `eligible`, `olderThanDays`, `lastModifiedUtc`, `ageDays`, `backlinkCount`, `backlinks`, `backlinkGuardPassed`, and `blockers`
- destructive results are explicit per doc: `deleted`, `blocked`, or `error`, with `previewOutcome` retained for apply-time audit
- eligibility is frozen before deletes run, so blocked docs do not cascade into deletion just because an earlier delete removed a backlink in the same batch
- `active` and `pending` docs never appear as prune candidates in this wave, and there is still no automatic follow-on action path

Still deferred after this wave: broader verify-sandbox expansion for pinned/cached query-embedding support, stale-to-active healing, cluster-level/default-on judged conflicts, and automatic actions.

## Frontmatter

Docs can include YAML frontmatter with a `related` field to link docs to source files. The `stale` and `health` commands use this to detect when source has changed since the doc was last updated.

KnowledgeSight also persists arbitrary top-level frontmatter into the index (bounded to scalar values and
string-list fields), so diagnostic reads like `context(file)` and `explain(refId)` can return indexed
frontmatter without reopening markdown files on the read path.

```markdown
---
related:
  - src/Auth/TokenService.fs
  - src/Auth/LoginHandler.fs
---
```

## Future Ideas

- **findings(query)** — Embedding search over accumulated session findings/insights. Needs a store of past review outputs to search against.
- **bridge(entity)** — Cross-index join between code-sight and knowledge-sight refs. E.g., find code refs for a doc entity or doc coverage for a code symbol.
- **why(refId)** — Reverse-lookup from code chunk to ADR/decision docs that mention the entity.

## Source

[github.com/micsh/Sightline](https://github.com/micsh/Sightline/tree/main/src/KnowledgeSight)
