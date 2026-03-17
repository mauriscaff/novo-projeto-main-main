# Best Practice: AGENTS.md

The `AGENTS.md` file is the primary instructions file for Codex CLI — providing project-level context and behavioral directives.

## File Naming and Fallbacks

Codex CLI searches for instructions in this order:
1. `AGENTS.md` (preferred)
2. `CODEX.md` (alias)

Use `AGENTS.md` for new projects.

## Sizing: Keep It Under 150 Lines

The single most impactful best practice: **keep AGENTS.md concise**.

- Lines beyond ~150 are increasingly likely to be ignored or truncated
- Long files dilute important instructions with noise
- The model processes instructions better when they are focused

**If you exceed 150 lines**, extract detailed content into:
- Skill files (`skills/<name>/SKILL.md`) for specialized procedures
- Separate docs files referenced by path
- Agent-preloaded skills for domain-specific knowledge

## Hierarchy and Override Mechanism

AGENTS.md files follow directory hierarchy:

```
/repo/AGENTS.md              # Root-level instructions
/repo/packages/api/AGENTS.md # Package-specific overrides
/repo/packages/web/AGENTS.md # Package-specific overrides
```

**Loading behavior**:
- Codex walks up from the working directory, loading each AGENTS.md it finds
- More specific (deeper) files take precedence over general (higher) ones
- All files are concatenated into context, with deeper files appearing later

## Recommended Structure

```markdown
# AGENTS.md

## Repository Overview
One paragraph describing what this project is and does.

## Key Components
Brief descriptions of major subsystems, with file paths.

## Critical Patterns
Non-obvious conventions the model MUST follow.

## Workflow Rules
Build, test, lint commands. Deployment patterns.

## Do NOT
Explicit anti-patterns to avoid.
```

## Anti-Patterns

| Anti-Pattern | Why It Fails | Fix |
|---|---|---|
| Dumping entire API docs | Exceeds line limit; dilutes focus | Link to docs; use skills |
| Repeating obvious conventions | Wastes lines on things the model knows | Only document the non-obvious |
| Long code examples | Eats line budget fast | Keep examples under 10 lines |
| Vague instructions ("be careful") | Not actionable | Be specific: "Always run `npm test` before committing" |
| Contradictory rules | Model picks one arbitrarily | Audit for conflicts |

## Monorepo Strategy

For monorepos, use a layered approach:

1. **Root `AGENTS.md`**: Shared conventions (git workflow, CI commands, coding standards)
2. **Package `AGENTS.md`**: Package-specific build commands, architecture decisions, testing patterns
3. **Skills**: Extract complex procedures (deployment, migration) into skills that any AGENTS.md can reference

This keeps each file short while maintaining comprehensive coverage.

## Truncation Behavior

When AGENTS.md exceeds the model's processing capacity:
- Content at the end of the file is most likely to be truncated
- Put the most critical instructions at the top
- Use clear section headers so the model can scan structure even if details are lost
- Test by asking the model to repeat instructions from different sections
