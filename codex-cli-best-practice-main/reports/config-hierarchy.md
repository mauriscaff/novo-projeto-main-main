# Report: Codex CLI Configuration Hierarchy

## Overview

The current repository layout uses `.codex/config.toml` for project settings and
`~/.codex/config.toml` for user defaults. CLI flags and `-c key=value`
overrides sit above both.

## Configuration Sources

```
1. CLI flags and `-c key=value`
2. .codex/config.toml
3. ~/.codex/config.toml
4. Built-in defaults
```

## Merge Behavior

Settings merge by key:

```toml
# ~/.codex/config.toml
model = "o4-mini"
approval_policy = "untrusted"

# .codex/config.toml
approval_policy = "on-request"

# Effective result:
# model = "o4-mini"
# approval_policy = "on-request"
```

Profiles overlay the merged base config:

```toml
model = "o4-mini"
sandbox_mode = "workspace-write"
approval_policy = "on-request"

[profiles.ci]
sandbox_mode = "read-only"
approval_policy = "never"
```

With `--profile ci`, the model stays `o4-mini` but sandbox and approval switch
to the CI values.

## Related Hierarchies

### AGENTS.md
Instruction files still follow the directory tree and get concatenated from the
repo root downward.

### Skills
Project skills live under `.agents/skills/` and user skills live under
`~/.agents/skills/`.

### Agents
Agent registrations live in `.codex/config.toml` under `[agents.<name>]`.
Detailed role configs can live in `.codex/agents/*.toml`.

## Environment Variables

Use `$VAR` interpolation in config:

```toml
[mcp_servers.github]
env = { GITHUB_TOKEN = "$GITHUB_TOKEN" }
```

## Practical Recommendations

1. Keep shared defaults in `.codex/config.toml`
2. Keep personal preferences in `~/.codex/config.toml`
3. Use profiles for repeatable context switches
4. Use `-c` for one-shot overrides instead of inventing extra local config files

## Common Pitfalls

- Mixing the retired single-file project schema with the current `.codex/config.toml` layout
- Forgetting that `[profiles.<name>]` overrides only the keys it sets
- Registering an agent role file without a matching `[agents.<name>]` entry
