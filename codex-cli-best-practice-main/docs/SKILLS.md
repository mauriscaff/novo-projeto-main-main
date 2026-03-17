# Skills System Reference

Skills are reusable instruction packages that extend Codex CLI's capabilities. They follow the open **SKILL.md standard**, making them portable and shareable across projects.

## SKILL.md File Format

Skills live in `.agents/skills/<name>/SKILL.md`. Each skill is a Markdown file with YAML frontmatter:

```markdown
---
name: my-skill
description: When to invoke this skill — used for auto-discovery
argument-hint: "[file-path]"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
model: o4-mini
---

# My Skill Instructions

Detailed instructions for what the skill should do when invoked...
```

## Frontmatter Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | Directory name | Display name and `/slash-command` trigger |
| `description` | string | — | Purpose description; used for auto-discovery ranking |
| `argument-hint` | string | — | Autocomplete hint shown after `/name` (e.g., `[issue-number]`) |
| `disable-model-invocation` | bool | `false` | Prevents automatic invocation; must be explicitly called |
| `user-invocable` | bool | `true` | If `false`, hidden from `/` menu (background knowledge only) |
| `allowed-tools` | string | — | Comma-separated tools allowed without permission prompts |
| `model` | string | Inherited | Model to use: `o4-mini`, `o3`, `gpt-4.1` |
| `context` | string | — | Set to `fork` to run in isolated subagent context |
| `agent` | string | `general-purpose` | Subagent type when `context: fork` |
| `hooks` | object | — | Lifecycle hooks scoped to this skill |

## String Substitutions

Skills support dynamic variable injection:

| Variable | Expands To |
|---|---|
| `$ARGUMENTS` | Full argument string passed after the skill name |
| `$0` | First positional argument |
| `$1`, `$2`, ... | Subsequent positional arguments |

**Example**: If user types `/deploy staging v2.1`, then `$ARGUMENTS` = `staging v2.1`, `$0` = `staging`, `$1` = `v2.1`.

## Built-in Skills

Codex CLI ships with several built-in skills prefixed with `$`:

### $plan
Structured planning skill. Creates a step-by-step plan before executing complex tasks. Automatically invoked when tasks appear multi-step.

### $skill-creator
Meta-skill that generates new SKILL.md files. Invoke with `/skill-creator` and describe what the skill should do.

### $web-search
Web search capability. Fetches and processes web content to answer questions requiring current information.

## Discovery Paths

Codex CLI discovers skills from multiple locations, in priority order:

1. **Project skills**: `./.agents/skills/` in the current project (scanned up to repo root)
2. **User skills**: `~/.agents/skills/` for personal cross-project skills
3. **Built-in skills**: Shipped with Codex CLI (`$plan`, `$skill-creator`, etc.)

When multiple skills share a name, the most local version wins (project > user > built-in).

## Skill Patterns

### User-Invocable Skill (Slash Command)
```yaml
---
name: deploy
description: Deploy the application to a target environment
argument-hint: "[environment] [version]"
allowed-tools: Bash, Read
---
```
User triggers with `/deploy production v2.0`.

### Agent-Preloaded Skill (Background Knowledge)
```yaml
---
name: code-standards
description: Team coding standards and conventions
user-invocable: false
---
```
Loaded into agent context via `[agents.<name>]` role configuration in `.codex/config.toml` and a companion TOML file:

```toml
# .codex/config.toml
[agents.backend-dev]
description = "Handles backend development tasks"
config_file = "agents/backend-dev.toml"
```

```toml
# .codex/agents/backend-dev.toml
model = "o4-mini"
skills = ["code-standards"]
```

Never shown in `/` menu.

### Forked Skill (Isolated Execution)
```yaml
---
name: security-audit
description: Run security analysis in isolated context
context: fork
agent: security-reviewer
allowed-tools: Bash, Read, Grep, Glob
---
```
Runs in a separate subagent context to avoid polluting the main conversation.

## Example: Complete Skill

```markdown
---
name: pr-review
description: Review a pull request and provide structured feedback
argument-hint: "[pr-number]"
allowed-tools: Bash, Read, Grep, Glob
model: o4-mini
---

# PR Review Skill

Review pull request #$0 and provide structured feedback.

## Steps
1. Fetch the PR diff using `gh pr diff $0`
2. Read all changed files for full context
3. Analyze for: correctness, security issues, performance, style
4. Output a structured review with severity ratings

## Output Format
Use this template:
- **Critical**: Issues that must be fixed
- **Warning**: Issues that should be addressed
- **Suggestion**: Optional improvements
- **Praise**: What was done well
```
