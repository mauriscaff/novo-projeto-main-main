# Best Practice: Sandbox Modes

Codex CLI currently exposes three sandbox modes: `read-only`,
`workspace-write`, and `danger-full-access`.

## Sandbox Modes

| Mode | File Access | Network | Best for |
|---|---|---|---|
| `read-only` | Read-only project access | Blocked | Reviews, audits, CI analysis |
| `workspace-write` | Read/write inside the workspace | Blocked | Local development and doc/code edits |
| `danger-full-access` | Unrestricted filesystem access | Allowed | Fully trusted automation that needs network or installs |

## Configuration

```toml
sandbox_mode = "workspace-write"

[profiles.review]
sandbox_mode = "read-only"

[profiles.trusted]
sandbox_mode = "danger-full-access"
```

## Recommended Defaults

### `read-only`
Use for review-style work, CI analysis, and anything that should never mutate
the checkout.

### `workspace-write`
Use for day-to-day development. It preserves the most useful editing workflows
while still blocking outbound network access.

### `danger-full-access`
Use only when you truly need unrestricted network and filesystem access. Pair it
with a deliberate approval choice and a trustworthy environment.

## Practical Matrix

| Task | Recommended Sandbox |
|---|---|
| Code review | `read-only` |
| Refactoring or content generation | `workspace-write` |
| External API calls or package installs | `danger-full-access` |
| Docs QA in CI | `read-only` |

## Anti-Patterns

- Using `danger-full-access` for ordinary editing tasks
- Leaving CI jobs on a writable sandbox when they only need analysis
- Documenting the retired `full`, `network-only`, or `off` terminology as current guidance
