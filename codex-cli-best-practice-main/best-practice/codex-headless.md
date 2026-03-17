# Best Practice: Headless Execution (`codex exec`)

`codex exec` is the current non-interactive entry point for CI, scripts, and
batch workflows.

## Basic Usage

```bash
codex exec "describe the architecture of this project"
codex --profile ci exec "review this repo for stale configuration"
codex exec --json "summarize the changed files"
```

## Recommended CI Profile

```toml
[profiles.ci]
model = "o4-mini"
sandbox_mode = "read-only"
approval_policy = "never"
```

Use `workspace-write` instead of `read-only` only when the CI task is supposed
to generate files in the checkout.

## Useful Flags

| Flag | Purpose |
|---|---|
| `--json` | Stream machine-readable events |
| `-o`, `--output-last-message` | Save the final message to a file |
| `--output-schema` | Validate the final response shape against JSON Schema |
| `--profile` | Apply a named config overlay |

## CI Patterns

### Review
```bash
codex review --base main
```

### Targeted exec run
```bash
codex --profile ci exec \
  "Check README.md, best-practice/, and reports/ for stale Codex CLI schema."
```

### Persist the final message
```bash
codex --profile ci exec \
  -o codex-summary.md \
  "Summarize the user-facing risks in this diff."
```

## Anti-Patterns

- Running CI with `danger-full-access` when a read-only job is enough
- Leaving headless runs on `on-request`, which can stall unattended execution
- Teaching retired config snippets in CI examples
