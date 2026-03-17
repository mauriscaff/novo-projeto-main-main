# Best Practice: Approval Policies

Approval policies control when Codex asks before executing model-generated
actions. In the current CLI the supported values are `untrusted`,
`on-request`, and `never`.

## Policy Levels

| Policy | Behavior | Best for |
|---|---|---|
| `untrusted` | Auto-runs only trusted read-style commands; asks for the rest | New repos, audits, reviews |
| `on-request` | Model decides when it should ask | Everyday development |
| `never` | Never asks; failures come straight back to the model | Non-interactive runs and tightly controlled automation |

## Configuration

```toml
approval_policy = "on-request"
```

## Recommended Combinations

```toml
[profiles.conservative]
sandbox_mode = "read-only"
approval_policy = "untrusted"

[profiles.development]
sandbox_mode = "workspace-write"
approval_policy = "on-request"

[profiles.ci]
sandbox_mode = "read-only"
approval_policy = "never"
```

## Guidance

### `untrusted`
Use this when you want Codex to stay in review mode. It is the safest option for
unknown codebases and security-sensitive work.

### `on-request`
Use this as the default in active development. It preserves momentum while
still letting the model escalate when a command deserves explicit approval.

### `never`
Use this only when the surrounding sandbox or environment already provides the
safety boundary. This is the right default for headless CI jobs and tightly
scoped automation.

## Anti-Patterns

- Treating `never` as a general-purpose local default
- Using `danger-full-access` and `never` together without a real containment boundary
- Leaving review and CI tasks on the same approval profile
- Teaching retired values like `suggest`, `auto-edit`, or `full-auto`
