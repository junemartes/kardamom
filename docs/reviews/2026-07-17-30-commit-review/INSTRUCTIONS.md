# 30-commit review — shared reviewer instructions

Repo: /home/dev/kardamom-review (worktree, branch claude/commit-review-30, HEAD = 70d0823).
You are reviewing specific historical commits, assigned in your prompt. Work READ-ONLY except for writing your findings file(s) under /home/dev/kardamom-review/review-findings/.

## Per commit
1. `git show <hash>` (use `--stat` first; for huge commits read file-by-file with `git show <hash> -- <path>`). Read the full diff — do not sample.
2. Understand intent from the commit message and surrounding code. Read the current version of touched files at HEAD when needed to judge whether a problem still exists.
3. Review for:
   - **Security**: secrets/keys in code or config, injection, unsafe deserialization, missing auth/validation on external inputs, panics on untrusted input, integer overflow in value/nonce/balance math, unsafe Rust, TOCTOU, path traversal, committed binaries/jars.
   - **Logic**: correctness bugs, race conditions, off-by-one, error handling that swallows failures, incorrect recovery/replay semantics, resource leaks, deadlocks, missing edge cases, wrong metrics.
   - **Code quality**: unnecessary complexity, duplication (DRY), preference for functional patterns (iterator chains over manual loops/mutation where it stays readable), dead code, misleading names/comments, docs drift.

## Findings file format
Write one markdown file per commit: `review-findings/<NN>-<hash>.md` (NN given in your prompt; 01 = newest commit).

```
# <NN> <hash> — <commit subject>

## Summary of change
2–5 sentences: what the commit does and why.

## Findings
### F<NN>.<k> [severity] [category] — <title>
- **Where**: file:line (at the commit, plus file:line at HEAD if it still exists)
- **What**: the problem, concretely; a failure scenario for logic/security issues
- **Still present at HEAD**: yes | no (fixed by <hash>) | partially
- **Suggested fix**: 1–3 sentences

## Verdict
One paragraph: overall assessment of the commit.
```

Severity: critical | high | medium | low | nit. Category: security | logic | quality.
If a commit is clean, say so — do not invent findings. Only findings with **Still present at HEAD: yes/partially** are actionable; mark them clearly.

## Return value (your final message)
Return a compact list: for each commit reviewed, `NN hash — n findings (m actionable at HEAD)`, then a bullet per ACTIONABLE finding: `F<NN>.<k> [severity/category] file:line-at-HEAD — one-line description`. No prose.
