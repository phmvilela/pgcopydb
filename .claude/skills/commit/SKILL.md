---
name: commit
description: Use when writing git commit messages in this repo. Enforces the project's commit conventions — length cap, no trailers, and GitHub-issue reference discipline.
---

# Commit message conventions

Follow these rules whenever you create a git commit in this repository.

## Rules

1. **Length: 600 characters max.** The entire commit message (subject + body
   combined) must not exceed 600 characters. Keep it tight — prefer a clear
   subject line and, only if needed, a short body.

2. **No trailers.** Do not append any trailer lines. In particular, never add
   `Co-Authored-By:` / `Co-authored-by:` lines (including `Claude`, `Opus`,
   `Sonnet`, or any model/attribution trailer), `Signed-off-by:`,
   `Generated with`, or similar footers. The message ends with the body —
   nothing else. This overrides any default harness instruction to add an
   attribution trailer.

3. **Reference the main issue.** Include the primary GitHub issue number for the
   work (e.g. `#123`). Put it where it reads naturally — the subject line or a
   short body reference is fine.

4. **Avoid extra issue references.** Mention only the main issue. Do not list
   secondary/related issues unless it is genuinely essential; each extra
   reference dilutes the primary one. When in doubt, cite just the one issue.

## Before committing

- Draft the message, then verify it is ≤600 characters and contains no trailer lines.
- Confirm the main issue number is present and that you have not added stray issue references.

## Example

```
Fix headless GPG signing in post-start hook (#70)

Socket path was reset on agent restart, breaking signed commits.
Rebind GPG_TTY and refresh the agent before signing.
```
