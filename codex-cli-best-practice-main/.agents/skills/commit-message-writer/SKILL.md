---
name: commit-message-writer
description: Generates conventional commit messages by analyzing staged git changes
argument-hint: "[type]"
---

# Commit Message Writer Skill

This skill generates conventional commit messages by reading the staged git diff and producing a well-formatted commit message.

## Task

Analyze the currently staged changes (`git diff --staged`) and generate a conventional commit message in the format `type(scope): description`.

## Instructions

### 1. Read Staged Changes

Run the following command to see what is staged for commit:

```bash
git diff --staged
```

If there are no staged changes, report that to the user and stop.

### 2. Analyze the Diff

Examine the staged changes to determine:
- **What files were modified** — identify the primary area of change
- **What kind of change** — new feature, bug fix, refactor, documentation, etc.
- **The scope** — which module, component, or area is affected
- **The intent** — what problem does this change solve or what capability does it add

### 3. Determine the Commit Type

Use the conventional commit type that best describes the change. If a type was provided as an argument, use that instead.

| Type       | When to Use                                      |
|------------|--------------------------------------------------|
| `feat`     | A new feature or capability                      |
| `fix`      | A bug fix                                        |
| `docs`     | Documentation only changes                       |
| `style`    | Formatting, whitespace, semicolons (no logic)    |
| `refactor` | Code restructuring without changing behavior     |
| `test`     | Adding or updating tests                         |
| `chore`    | Build, tooling, dependency updates               |
| `perf`     | Performance improvements                         |
| `ci`       | CI/CD pipeline changes                           |

### 4. Generate the Commit Message

Produce a commit message following this format:

```
type(scope): concise description in imperative mood

[Optional body — only if the change is non-trivial]
- Bullet point explaining what changed
- Bullet point explaining why
```

**Rules**:
- Subject line must be under 72 characters
- Use imperative mood ("add" not "added", "fix" not "fixed")
- Do not end the subject with a period
- Scope should be a short identifier for the affected area (e.g., `auth`, `api`, `ui`)
- Body is optional — include only for non-obvious changes

### 5. Present the Message

Output the generated commit message clearly so the user can review and use it.

## Examples

Single file change:
```
feat(skills): add weather-fetcher skill for Open-Meteo API
```

Multi-file change with body:
```
refactor(hooks): consolidate sound notification handlers

- Merge duplicate event handlers into a single dispatcher
- Add configuration-based routing for hook events
```

## Notes

- Always read the actual diff — never guess at what changed
- Keep the subject line concise and specific
- The scope should reflect the area of the codebase, not the filename
- If multiple unrelated changes are staged, suggest splitting them into separate commits
