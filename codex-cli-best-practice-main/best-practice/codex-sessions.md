# Best Practice: Session Management

Codex CLI supports persistent sessions that can be resumed, forked, and managed. This enables iterative workflows where context carries across multiple interactions.

## Session Basics

Every Codex CLI interaction creates a session with a unique ID. Sessions store:
- Conversation history (prompts and responses)
- Tool call results
- File context accumulated during the session

## Resume a Session

Pick up exactly where you left off:

```bash
# Resume the most recent session
codex --resume

# Resume a specific session by ID
codex --resume session_abc123
```

**When to resume**:
- You were interrupted and want to continue a task
- You need to give follow-up instructions based on previous work
- A long-running task timed out and you want to continue

## Fork a Session

Create a new session branching from an existing one's context:

```bash
# Fork from the most recent session
codex --fork

# Fork from a specific session
codex --fork session_abc123
```

**When to fork**:
- You want to explore an alternative approach without losing the original
- You need the same context but for a different task
- Multiple team members want to continue from the same checkpoint

### Resume vs Fork

| Behavior | `--resume` | `--fork` |
|---|---|---|
| Original session | Extended | Unchanged |
| New session created | No | Yes |
| Context preserved | Yes | Yes (copy) |
| History shared | Same session | Independent from fork point |

## Session History

List and manage past sessions:

```bash
# List recent sessions
codex sessions list

# View session details
codex sessions show session_abc123

# Delete a session
codex sessions delete session_abc123
```

## Workflow Patterns

### Iterative Development

```bash
# Start a task
codex "implement the user authentication module"
# ... Codex works, then you review ...

# Continue with refinements
codex --resume "add rate limiting to the login endpoint"
# ... builds on previous context ...

# Add tests
codex --resume "write tests for the auth module you just created"
```

### Exploratory Branching

```bash
# Initial implementation
codex "design the database schema for the blog feature"

# Try approach A
codex --fork "implement using PostgreSQL with JSONB columns"

# Try approach B (from same starting point)
codex --fork "implement using normalized tables with foreign keys"
```

### Team Handoff

```bash
# Developer A starts the task
codex "analyze the performance bottleneck in the API layer"
# Shares session ID: session_perf_001

# Developer B continues with different focus
codex --fork session_perf_001 "implement the caching solution you identified"
```

## Session Hygiene

### Clean Up Old Sessions
Sessions accumulate over time. Periodically clean up:

```bash
# Delete sessions older than 30 days
codex sessions prune --older-than 30d
```

### Name Important Sessions
When starting a session you plan to resume, note the session ID. Consider saving it:

```bash
# Save session ID for later
codex "start the migration" 2>&1 | tee migration-session.log
```

## Combining Sessions with Profiles

```bash
# Start analysis in review mode
codex --profile review "review the PR changes"

# Continue in dev mode to implement fixes
codex --profile dev --resume "fix the issues you identified"
```

## Limitations

- Session context is subject to the model's context window — very long sessions may lose early context
- Forked sessions are independent — changes in one do not appear in the other
- Sessions store conversation history, not file state — if files changed externally between resume, Codex re-reads them

## Anti-Patterns

- **Resuming sessions after major file changes**: The session's context may reference outdated code. Fork instead and let Codex re-read files.
- **Never cleaning up sessions**: Old sessions consume storage. Prune regularly.
- **Using resume when you want a fresh start**: If the previous session went in a wrong direction, start fresh rather than resuming.
- **Over-relying on session context**: If a resumed session seems confused, start a new one. Fresh context often works better than stale history.
