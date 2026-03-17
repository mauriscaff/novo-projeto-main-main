# Report: Approval Policies in Codex CLI

## Overview

Approval policy and sandbox mode are separate controls. Approval policy decides
when Codex asks; sandbox decides what the process can actually do.

## Current Policies

### `untrusted`
Only trusted read-style commands run without asking. This is the safest choice
for unfamiliar codebases and code review sessions.

### `on-request`
The model decides when it should ask. This is the best default for local
development because it balances momentum with review.

### `never`
Codex never asks for approval. Failures are returned directly to the model.
Use this for unattended automation or environments that already have strong
external containment.

## Recommended Pairings

| Workflow | Sandbox | Approval |
|---|---|---|
| Security review | `read-only` | `untrusted` |
| Local development | `workspace-write` | `on-request` |
| CI analysis | `read-only` | `never` |
| Trusted networked automation | `danger-full-access` | `never` |

## Practical Notes

- `never` is the right fit for headless jobs because no one is there to approve
- `on-request` is the best default for interactive work
- `untrusted` is a good onboarding mode for new repos or new team members

## Common Mistakes

- Teaching retired policies like `suggest`, `auto-edit`, or `full-auto`
- Using `never` as the everyday local default without a good reason
- Pairing `danger-full-access` and `never` casually
