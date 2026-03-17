# Report: Headless CI/CD with Codex CLI

## Overview

The current headless path is `codex exec`, with `codex review` available for
non-interactive code review flows.

## Recommended CI Profile

```toml
[profiles.ci]
model = "o4-mini"
sandbox_mode = "read-only"
approval_policy = "never"
```

This keeps unattended runs deterministic and prevents approval prompts from
blocking the job.

## Current Command Surface

```bash
codex --profile ci exec "review this repo for stale docs"
codex review --base main
codex --profile ci exec --json "summarize the changed files"
codex --profile ci exec -o codex-summary.md "write a short QA summary"
```

## GitHub Actions Pattern

```yaml
- name: Run Codex docs QA
  run: |
    codex --profile ci exec \
      "Check README.md, best-practice/, and reports/ for stale Codex CLI schema."
  env:
    OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
```

## Operational Guidance

- Prefer `read-only` CI unless the job is supposed to write files
- Use `never` for unattended execution
- Use `--json` or `--output-schema` when downstream tooling needs structure
- Prefer `codex review --base <branch>` when you want a diff-focused review

## Limitations

- Headless runs cannot stop for approval prompts
- Network access remains blocked under `read-only` and `workspace-write`
- Prompt quality still determines output quality; broad prompts create noisy CI
