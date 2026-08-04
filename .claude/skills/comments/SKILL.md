---
name: comments
description: Use when writing or editing comments in any file (Python, YAML, Terraform, shell, etc.). Enforces the project's rules — no historical notes, no issue/PR numbers, keep them plain and sparse, and use "see also" links to related files.
---

# Comment conventions

Follow these rules whenever you add or edit a comment in any file — code, YAML,
Terraform, shell, config, anything.

## Rules

1. **No historical / "changed X" comments.** Comments describe the code as it is
   now, not how it got here. Never write things like `# changed to fix ...`,
   `# was previously ...`, `# updated to ...`, `# removed the old ...`. Git
   history is the record of change — the comment is not.

2. **No issue or PR numbers.** Do not put `#123`, `PR #45`, ticket IDs, or links
   to issues/PRs in comments. If context is needed, explain the *reason* in
   plain words instead of pointing at a tracker.

3. **Keep comments plain and sparse.** Prefer no comment to a redundant one.
   Don't restate what the code already says. Only comment when the *why* isn't
   obvious from the code. When you do comment, use plain language and the fewest
   words that carry the point — no dense paragraphs.

4. **Use "see also" links to related files.** When understanding this code
   depends on another file, add a short pointer to it rather than duplicating
   the explanation. Reference a real path in the repo.

## Before writing a comment, ask

- Is this describing history or a change? → delete it.
- Does it name an issue/PR/ticket? → remove the reference, keep only the reason if it matters.
- Does it just restate the code? → delete it.
- Is the *why* non-obvious? → a short, plain comment is warranted.
- Does it depend on another file? → add a `See also: <path>` pointer.

## Examples

Avoid:

```python
# Changed in #78 — sequences now reset via setval after copy (was a no-op before)
def reset_sequences(self): ...
```

Prefer:

```python
# COPY leaves target sequences at their start, so reset them to the source values.
# See also: pg_cyan/sequences.py
def reset_sequences(self): ...
```

Avoid (YAML):

```yaml
# updated fsGroup to 999 to fix PVC perms per ticket ABC-12
securityContext:
  fsGroup: 999
```

Prefer (YAML):

```yaml
# Non-root pod needs group ownership of the managed-disk PVC.
securityContext:
  fsGroup: 999
```
