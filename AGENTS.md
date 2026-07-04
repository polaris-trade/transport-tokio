# Before starting work

- Run `lat search` to find sections relevant to your task. Read them to understand the design intent before writing code.
- Run `lat expand` on user prompts to expand any `[[refs]]` — this resolves section names to file locations and provides context.

# Post-task checklist (REQUIRED — do not skip)

After EVERY task, before responding to the user:

- [ ] Update `lat.md/` if you added or changed any functionality, architecture, tests, or behavior
- [ ] Run `lat check` — all wiki links and code refs must pass
- [ ] Do not skip these steps. Do not consider your task done until both are complete.

---

# What is lat.md?

This project uses [lat.md](https://www.npmjs.com/package/lat.md) to maintain a structured knowledge graph of its architecture, design decisions, and test specs in the `lat.md/` directory. It is a set of cross-linked markdown files that describe **what** this project does and **why** — the domain concepts, key design decisions, business logic, and test specifications. Use it to ground your work in the actual architecture rather than guessing.

# Commands

```bash
lat locate "Section Name"      # find a section by name (exact, fuzzy)
lat refs "file#Section"        # find what references a section
lat search "natural language"  # semantic search across all sections
lat expand "user prompt text"  # expand [[refs]] to resolved locations
lat check                      # validate all links and code refs
```

Run `lat --help` when in doubt about available commands or options.

If `lat search` fails because no API key is configured, explain to the user that semantic search requires a key provided via `LAT_LLM_KEY` (direct value), `LAT_LLM_KEY_FILE` (path to key file), or `LAT_LLM_KEY_HELPER` (command that prints the key). Supported key prefixes: `sk-...` (OpenAI) or `vck_...` (Vercel). If the user doesn't want to set it up, use `lat locate` for direct lookups instead.

# Syntax primer

- **Section ids**: `lat.md/path/to/file#Heading#SubHeading` — full form uses project-root-relative path (e.g. `lat.md/tests/search#RAG Replay Tests`). Short form uses bare file name when unique (e.g. `search#RAG Replay Tests`, `cli#search#Indexing`).
- **Wiki links**: `[[target]]` or `[[target|alias]]` — cross-references between sections. Can also reference source code: `[[src/foo.ts#myFunction]]`.
- **Source code links**: Wiki links in `lat.md/` files can reference functions, classes, constants, and methods in TypeScript/JavaScript/Python/Rust/Go/C files. Use the full path: `[[src/config.ts#getConfigDir]]`, `[[src/server.ts#App#listen]]` (class method), `[[lib/utils.py#parse_args]]`, `[[src/lib.rs#Greeter#greet]]` (Rust impl method), `[[src/app.go#Greeter#Greet]]` (Go method), `[[src/app.h#Greeter]]` (C struct). `lat check` validates these exist.
- **Code refs**: `// @lat: [[section-id]]` (JS/TS/Rust/Go/C) or `# @lat: [[section-id]]` (Python) — ties source code to concepts

# Test specs

Key tests can be described as sections in `lat.md/` files (e.g. `tests.md`). Add frontmatter to require that every leaf section is referenced by a `// @lat:` or `# @lat:` comment in test code:

```markdown
---
lat:
  require-code-mention: true
---
# Tests

Authentication and authorization test specifications.

## User login

Verify credential validation and error handling for the login endpoint.

### Rejects expired tokens
Tokens past their expiry timestamp are rejected with 401, even if otherwise valid.

### Handles missing password
Login request without a password field returns 400 with a descriptive error.
```

Every section MUST have a description — at least one sentence explaining what the test verifies and why. Empty sections with just a heading are not acceptable. (This is a specific case of the general leading paragraph rule below.)

Each test in code should reference its spec with exactly one comment placed next to the relevant test — not at the top of the file:

```python
# @lat: [[tests#User login#Rejects expired tokens]]
def test_rejects_expired_tokens():
    ...

# @lat: [[tests#User login#Handles missing password]]
def test_handles_missing_password():
    ...
```

Do not duplicate refs. One `@lat:` comment per spec section, placed at the test that covers it. `lat check` will flag any spec section not covered by a code reference, and any code reference pointing to a nonexistent section.

# Section structure

Every section in `lat.md/` **must** have a leading paragraph — at least one sentence immediately after the heading, before any child headings or other block content. The first paragraph must be ≤250 characters (excluding `[[wiki link]]` content). This paragraph serves as the section's overview and is used in search results, command output, and RAG context — keeping it concise guarantees the section's essence is always captured.

```markdown
# Good Section

Brief overview of what this section documents and why it matters.

More detail can go in subsequent paragraphs, code blocks, or lists.

## Child heading

Details about this child topic.
```

```markdown
# Bad Section

## Child heading

Details about this child topic.
```

The second example is invalid because `Bad Section` has no leading paragraph. `lat check` validates this rule and reports errors for missing or overly long leading paragraphs.

---

# Memory And Search Protocol (MANDATORY)

All agents (Conductor, subagents, standalone) MUST follow this order before planning, implementation, review, investigation, writing code, or delegating research:

1. Call `agentmemory/memory_recall` with task, file, and module keywords when available.
2. Use `lat locate` or `lat expand` for architecture and design context when `lat.md/` exists.
3. Use Semble for semantic code search: `uvx --from "semble[mcp]" semble search "query" .`.
4. Use exposed `fff-mcp` MCP tools (`fff-grep`, `fff-find_files`, `fff-multi_grep`) for exact/file search.
5. Use `rust-analyzer` for Rust definitions, references, hover, diagnostics.
6. Fall back to regular search/read tools if preferred tools are missing, fail, or lack needed capability. State fallback reason.

Fallback rule: if preferred tool is missing, fails, or lacks needed capability, use regular tools and state reason in response or handoff.

Subagents should try exposed `fff-mcp` tools before fallback. If unavailable, use `rg` or `find` and state reason.

Conductor prompts must repeat memory/search protocol and fallback behavior for subagents.

This duplicates Claude's own global `~/.claude/CLAUDE.md` protocol on purpose — Copilot (VS Code and CLI) has no reliable user-global config inheritance, so this file is the only place Copilot will ever see it.

# Code Comment Rules (MANDATORY — WRITING, NOT REVIEW)

**Every agent writing ANY code, doc comment, or inline comment MUST follow these rules. Violations block merge.**

## Banned In All Comments

NEVER write any of these in code comments, doc comments (`///`, `//!`), or inline comments (`//`):

- `REQ-*` — requirement IDs (e.g. `REQ-P2-001`, `REQ-ARCH-022`)
- `TASK-*` — task IDs (e.g. `TASK-P2-004`)
- `AC-*` — acceptance criteria IDs
- `Phase N` or `Phase X` — phase references
- `milestone Y` — milestone references
- `work unit N` — work unit references
- Em dash `—` (U+2014)

## Allowed

- Cross-crate references: `// see pipeline-sinks::pg::raw`
- Short annotations: `TODO`, `FIXME`, `HACK`, `NOTE`, `WARNING`, `PERF`, `SECURITY`, `BUG`
- `// SAFETY:` blocks with invariant justification

## Why

Spec IDs leak process into permanent code. Git log and PR capture process history. Comments must stand alone post-merge.

## Subagent Relay (MANDATORY)

**Conductor MUST include the full "Banned In All Comments" list above in EVERY subagent handoff packet.** Subagents do not auto-load project instruction files. The handoff packet is their only source of truth for comment rules.

Implement-subagent packet must include:

```
CODE COMMENT RULES (MANDATORY — DO NOT VIOLATE):
NEVER write REQ-*, TASK-*, AC-*, Phase N, milestone Y, work unit N, or em dash (—) in any code comment, doc comment, or inline comment.
Allowed: cross-crate refs (// see crate::module), TODO, FIXME, HACK, NOTE, WARNING, PERF, SECURITY, BUG, SAFETY.
```

Code-review-subagent packet must include:

```
CODE COMMENT AUDIT (MANDATORY):
Flag every REQ-*, TASK-*, AC-*, Phase N, milestone Y, work unit N, and em dash (—) found in code comments. Any hit = NEEDS_REVISION.
```

# Post-Task Checklist (MANDATORY — ALL AGENTS, RUN BEFORE REPORTING DONE)

1. `cargo test --workspace --no-fail-fast` — must pass
2. `cargo clippy --workspace -- -D warnings` — must pass
3. `lat check` — must pass
4. `rg -n 'REQ-|TASK-|AC-' -g '*.rs' -g '!**/target/**'` — must be empty
5. `rg -n '—' -g '*.rs' -g '!**/target/**'` — must be empty
6. Update spec progress in `specs/<task-slug>/tasks.md` if any task changed state, or `../specs/<task-slug>/tasks.md` at the workspace root for cross-module specs
7. Update `lat.md/` if any module/type/function was added, removed, or renamed

If any step fails: fix it. Do NOT skip. Do NOT report done until all pass.

# Commit Message Convention

Use Conventional Commits: `type(scope): subject`.

- Always include a scope for `feat`, `fix`, `refactor`, and `perf` commits.
- Valid types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`.
- Keep header length at 100 characters or less.
- Use lowercase subject style, not start-case, PascalCase, or upper-case.
- Do not suggest merge commits.

The `pr-title` workflow enforces this via `amannn/action-semantic-pull-request` — commits themselves are advisory unless you also install a local `commitlint` hook.

# Unit Test Rules

**Unit tests MUST NOT connect to external services** — databases (PostgreSQL, MSSQL), APIs, or network resources.

- **No real service connections in unit tests** — no DB connections, HTTP clients, external APIs.
- **Use `#[ignore]` for integration tests** — tests requiring real PostgreSQL/MSSQL/network services must be annotated with `#[ignore]` and only run via `cargo test -- --ignored`.
- **Use mockall for mocking** — prefer the [mockall](https://docs.rs/mockall/latest/mockall/) crate for mock implementations of traits and functions.
- **Localhost mock servers acceptable** — tests that bind to `127.0.0.1:0` with ephemeral ports and implement mock protocol servers in-process are acceptable.
- **E2E tests are exempt** — only apply these rules to unit/integration tests, not when the user explicitly asks for e2e tests.
- Run unit tests using `cargo nextest` for faster feedback loops.

When writing new tests:

1. Default to pure unit tests using test doubles/mocks.
2. Add mockall to dev-dependencies if mocking is needed: `mockall = { workspace = true }`.
3. Gate any DB/API tests with `#[ignore]`.
4. Document in test comments when `--ignored` flag is required.
5. **Test behavior, not language features** — do not write tests that verify language semantics (`Option::is_some()`, type casts, serde deserialization, default trait values). Tests should verify project-specific business logic.

