# Fix-phase instructions (shared)

Repo: /home/dev/kardamom-review, branch claude/commit-review-30 (based on origin/main @ 70d0823).
You own ONE work-package (WP) from review-findings/SUMMARY.md. Other agents are editing other files in the SAME worktree concurrently.

Hard rules:
- Only modify files within your WP's "Owns:" list (plus adding new files under those paths, and Cargo.toml of an owned crate only if strictly required — note it in your report if you do).
- Do NOT run any git command that mutates state (no commit/add/stash/checkout). `git rm --cached` is allowed ONLY where your WP explicitly covers removing tracked artifacts.
- Do NOT reformat or drive-by-edit unrelated code. Match existing idiom; prefer simple, DRY, functional style where natural.

Process:
1. Read your WP section in review-findings/SUMMARY.md, then the referenced per-commit findings files (NN-*.md) for full context on each finding.
2. Fix every finding in your WP. Implement the findings file's "Suggested fix" unless you determine it's wrong — then implement the correct minimal fix and explain.
3. Severity guides depth: H/M findings deserve real fixes (including tests where the finding is a logic bug — add a regression test if the crate has a natural place for one). L/N may be minimal (comment/doc/config corrections).
4. If a fix is genuinely unsafe/infeasible in this pass (needs a design decision, cross-WP change, or history rewrite), mark it DEFERRED with a concrete reason — don't half-fix.
5. Verify: `cargo check -p <owned crates>` (and `cargo test -p <crate> <focused filters>` for crates where you changed logic). The shared target dir means builds may wait on a lock — that's fine. For Java (WP-J): use the gradle wrapper if present (`cluster/sealer-service/gradlew`); if the toolchain is unavailable, say so explicitly.
6. Write `review-findings/fixes-<WP>.md`: per finding — FIXED (files touched, 1-2 sentence what/why) or DEFERRED (reason). Include verification results (exact commands + pass/fail).

Return (final message): compact list — `F<NN>.<k> FIXED|DEFERRED — one line`, plus verification status. No prose.
