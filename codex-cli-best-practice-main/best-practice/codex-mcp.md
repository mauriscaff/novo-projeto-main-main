# Best Practice: MCP (Model Context Protocol)

Codex CLI uses `[mcp_servers.<name>]` in `.codex/config.toml` for MCP
integrations, and it can also run as an MCP server via `codex mcp-server`.

## MCP Server Configuration

```toml
[mcp_servers.filesystem]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "."]

[mcp_servers.github]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "$GITHUB_TOKEN" }
```

## Agent-Scoped MCP Servers

Keep MCP access narrow by attaching servers to specific agents:

```toml
[agents.data-analyst]
description = "Works with repository and database context"
config_file = "agents/data-analyst.toml"
```

```toml
# .codex/agents/data-analyst.toml
model = "o4-mini"
mcp_servers = ["filesystem", "github"]
```

## Codex as an MCP Server

```bash
codex mcp-server
```

Consumer example:

```json
{
  "mcpServers": {
    "codex": {
      "command": "codex",
      "args": ["mcp-server"]
    }
  }
}
```

## Security Guidance

1. Use `$ENV_VAR` references for secrets
2. Scope MCP servers per agent instead of making every server globally available
3. Prefer `workspace-write` or `read-only` when a server does not need network
4. Switch to `danger-full-access` only when the MCP workflow truly requires network access

## Anti-Patterns

- Documenting the retired MCP table naming from older Codex releases
- Hardcoding tokens in config
- Giving every agent the same MCP surface area
- Using the retired one-flag MCP server syntax instead of `codex mcp-server`
