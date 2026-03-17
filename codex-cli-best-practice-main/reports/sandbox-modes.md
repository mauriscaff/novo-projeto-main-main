# Report: Sandbox Modes in Codex CLI

## Overview

The current sandbox names are `read-only`, `workspace-write`, and
`danger-full-access`.

## Behavior Summary

| Mode | Filesystem | Network | Typical use |
|---|---|---|---|
| `read-only` | No persistent writes | Blocked | Reviews, CI analysis |
| `workspace-write` | Writes allowed in the workspace | Blocked | Day-to-day editing |
| `danger-full-access` | No sandbox restriction | Allowed | Trusted tasks that need installs or APIs |

## Security Implications

### `read-only`
Best containment for analysis-only work. Use it whenever the task should not
change the checkout.

### `workspace-write`
Best interactive default. It allows useful edits while still blocking outbound
network access.

### `danger-full-access`
Reserve this for workflows that genuinely need unrestricted access. It should be
an explicit, conscious choice.

## Recommended Pairings

| Scenario | Sandbox | Approval |
|---|---|---|
| Docs QA in CI | `read-only` | `never` |
| Local development | `workspace-write` | `on-request` |
| Networked automation | `danger-full-access` | `never` or `on-request` |

## Migration Risk

The biggest QA problem in docs-heavy repos is continuing to teach the retired
`full`, `network-only`, and `off` vocabulary after the CLI has moved on.
