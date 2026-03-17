# Best Practice: config.toml

Codex CLI uses TOML configuration with a current, project-scoped layout. Keep
the shared file in `.codex/config.toml`, keep personal defaults in
`~/.codex/config.toml`, and use CLI flags or `-c key=value` for one-off
overrides.

## Config File Locations

| Location | Scope | Purpose |
|---|---|---|
| `~/.codex/config.toml` | Global | Personal defaults across projects |
| `.codex/config.toml` | Project | Team-shared defaults, profiles, MCP, agents |
| CLI flags / `-c` | Invocation | One-off overrides for a single run |

**Precedence**: CLI flags and `-c` overrides > project config > user config.

## Basic Configuration

```toml
# .codex/config.toml
model = "o4-mini"
sandbox_mode = "workspace-write"
approval_policy = "on-request"

[profiles.review]
model = "o3"
sandbox_mode = "read-only"
approval_policy = "on-request"
```

## Named Profiles

Profiles live under `[profiles.<name>]`:

```toml
[profiles.conservative]
sandbox_mode = "read-only"
approval_policy = "untrusted"

[profiles.development]
sandbox_mode = "workspace-write"
approval_policy = "on-request"

[profiles.ci]
model = "o4-mini"
sandbox_mode = "read-only"
approval_policy = "never"
```

Activate with `codex --profile ci exec "review this repo"`.

## Current Layout Patterns

### Shared Project Defaults
Commit only the settings the team agrees on:

```toml
model = "o4-mini"
sandbox_mode = "workspace-write"
approval_policy = "on-request"
```

### MCP Servers
Declare shared integrations in the same file:

```toml
[mcp_servers.github]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "$GITHUB_TOKEN" }
```

### Agents
Register agents under `[agents.<name>]` and optionally point them at dedicated
role files:

```toml
[agents.backend-dev]
description = "Handles backend implementation tasks"
config_file = "agents/backend-dev.toml"
```

```toml
# .codex/agents/backend-dev.toml
model = "o4-mini"
skills = ["api-conventions", "error-handling"]
```

### One-Off Overrides
Use the CLI instead of inventing extra local config files:

```bash
codex -c model=\"o3\" -c approval_policy=\"never\" exec "summarize this diff"
```

## Anti-Patterns

- Documenting retired Codex config schema from pre-`.codex/config.toml` releases
- Hardcoding secrets instead of using `$ENV_VAR` expansion
- Putting agent registration outside `.codex/config.toml`
- Mixing unrelated concerns into one profile instead of creating focused profiles
