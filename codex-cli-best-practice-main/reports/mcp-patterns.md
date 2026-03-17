# Report: MCP Patterns in Codex CLI

## Overview

The current MCP surface in Codex CLI has two halves:

1. Codex consuming external MCP servers via `[mcp_servers.<name>]`
2. Codex exposing itself via `codex mcp-server`

## Client-Side MCP

Declare servers in `.codex/config.toml`:

```toml
[mcp_servers.github]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "$GITHUB_TOKEN" }
```

## Agent-Scoped MCP

Attach only the servers an agent needs:

```toml
[agents.release-manager]
description = "Handles release coordination"
config_file = "agents/release-manager.toml"
```

```toml
# .codex/agents/release-manager.toml
model = "o4-mini"
mcp_servers = ["github"]
```

This keeps the tool surface narrow and makes agent behavior easier to reason
about.

## Codex as an MCP Server

Start Codex in server mode with:

```bash
codex mcp-server
```

Client example:

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

## Sandbox Interaction

| Sandbox | MCP impact |
|---|---|
| `read-only` | Local-only servers can work; networked servers will be constrained |
| `workspace-write` | Local servers and editing workflows work; outbound network stays blocked |
| `danger-full-access` | Use when the MCP workflow genuinely needs network access |

## Main Risks

- Outdated docs using the retired MCP table naming instead of the current one
- Broad MCP exposure across every agent
- Hardcoded credentials
- Server-side examples using the retired one-flag MCP syntax instead of `codex mcp-server`
