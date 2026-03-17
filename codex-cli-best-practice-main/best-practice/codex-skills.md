# Best Practice: Skills

Skills are the primary mechanism for extending Codex CLI with reusable, composable instruction packages. They follow the SKILL.md open standard.

## Two Skill Patterns

### 1. User-Invocable Skills (Slash Commands)

Triggered explicitly by the user via `/skill-name`:

```yaml
---
name: deploy
description: Deploy to target environment
argument-hint: "[env] [version]"
allowed-tools: Bash, Read
---
```

**Best for**: Workflows the user triggers on demand (deploy, review, generate).

### 2. Agent-Preloaded Skills (Background Knowledge)

Defined as regular skills, then attached to an agent through the current
agent configuration model:

```toml
# .codex/config.toml
[agents.api-developer]
description = "Builds and reviews HTTP APIs"
config_file = "agents/api-developer.toml"
```

```toml
# .codex/agents/api-developer.toml
model = "o4-mini"
skills = ["api-conventions", "error-handling"]

prompt = """
Work on backend APIs for this project.
"""
```

**Best for**: Domain knowledge that agents need but users never invoke directly.

## Frontmatter Guidelines

### Write Descriptive `description` Fields
The description drives auto-discovery. Be specific about when the skill should be used:

```yaml
# Good: specific trigger condition
description: Review TypeScript files for type safety issues and suggest fixes

# Bad: vague, matches too many contexts
description: Help with TypeScript
```

### Scope `allowed-tools` Tightly
Only grant tools the skill actually needs:

```yaml
# Good: minimal permissions
allowed-tools: Read, Grep, Glob

# Bad: overly permissive
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, WebFetch
```

### Choose the Right Model
Use cheaper models for simple tasks:

```yaml
# Fast analysis tasks
model: o4-mini

# Complex reasoning tasks
model: o3
```

## Skill Organization

```
.agents/skills/
  deploy/
    SKILL.md          # Main skill instructions
  pr-review/
    SKILL.md
  code-standards/     # Agent-preloaded, not user-invocable
    SKILL.md
  security-audit/
    SKILL.md
```

### Naming Conventions
- Use kebab-case for directory names: `pr-review`, not `prReview`
- Keep names short but descriptive
- Avoid generic names like `helper` or `utils`

## String Substitutions

Use `$ARGUMENTS` for the full argument string, `$0` for the first positional arg:

```markdown
---
name: fix-issue
argument-hint: "[issue-number]"
---

# Fix Issue Skill

Fetch issue #$0 from GitHub and implement a fix:
1. Run `gh issue view $0` to read the issue
2. Analyze the reported problem
3. Implement and test the fix
```

## Composing Skills

### Skill Chains via Commands
Use a command to orchestrate multiple skills:

```markdown
<!-- commands/full-review.md -->
1. Invoke /security-audit on the changed files
2. Invoke /pr-review for code quality
3. Combine findings into a single report
```

### Agent + Skills
Agents with preloaded skills get domain expertise without user intervention:

```toml
# .codex/config.toml
[agents.backend-dev]
description = "Handles backend development tasks"
config_file = "agents/backend-dev.toml"
```

```toml
# .codex/agents/backend-dev.toml
model = "o4-mini"
skills = ["api-conventions", "database-patterns", "error-handling"]
```

## Anti-Patterns

| Anti-Pattern | Fix |
|---|---|
| Putting all instructions in AGENTS.md | Extract into focused skills |
| Skills longer than 100 lines | Split into multiple skills or use linked docs |
| Skills that do everything | One skill, one responsibility |
| Forgetting `user-invocable: false` for knowledge skills | Always set for agent-only skills |
| Hardcoding values that vary by environment | Use `$ARGUMENTS` for dynamic values |
