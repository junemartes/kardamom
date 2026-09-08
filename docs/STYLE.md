# Code style

These rules apply to every Rust crate in this workspace. They also apply to the Java
sealer where the rule is not Rust-specific. Check new code against all of them before
you open a pull request. `just style` runs the mechanical checks. The judgment rules
have no linter: read the diff against this list.

## Rules

| id | rule |
|---|---|
| R1 | A comment is self-contained. It states the invariant, the safety argument, or the protocol rule in present tense. It carries no history: no issue numbers, no phase names, no "used to", no spec document names. A comment on self-explanatory code is deleted. |
| R2 | A function over 100 code lines is split into helpers. One between 51 and 100 is split case by case: split it when it has more than one concern or a repeated shape. |
| R3 | A file has at most 500 code lines, blank and comment-only lines excluded. Inline test modules move to a sibling file first. |
| R4 | No manual `drop(x)`. A helper function or a block scope ends the value. A channel sender that must close to signal EOF lives in one named helper. |
| R5 | No mutex where a channel works. No channel or mutex where ownership works. Every sync primitive that stays has a cross-thread reason or a measured performance reason. |
| R6 | No dynamic dispatch. Use generics. A wiring seam gets one more associated type on the wiring trait, and builders return named structs, not `impl Trait`. Where the set of implementations is closed, use an enum with one variant per case. |
| R7 | No pile of type parameters on one item. Group the bounds into a supertrait with associated types. |
| R8 | No `pub` field or item that nothing outside needs. Binaries default to `pub(crate)`. |
| R9 | No defensive checks in a function body, and no `debug_assert!` guards. Parse once at the boundary into a typed value so the type carries the guarantee. |
| R10 | Functional style over imperative loops and mutable accumulators, where the result is clearer and the iteration order of a hash-fed collection does not change. |
| R11 | `clippy::pedantic` passes. Group method arguments into structs. No `allow(clippy::too_many_arguments)`. Every `allow` carries a one-line reason. |
| R12 | Safe arithmetic everywhere it costs no performance: `checked_*` with an error for values from the wire, a header, a config, a clock, or a growing counter; `saturating_*` or `wrapping_*` only where that is the meaning, with a comment; `try_from` for narrowing casts. Plain arithmetic only on a proven hot path with the bound stated. |
| R13 | A value that must not be zero is a `NonZero*` type, or a newtype around one, parsed once at its boundary. No `.max(1)` fixups. No `assert!(x > 0)`. |
| R14 | Less code. Two places with the same shape become one helper: exact copies, the same steps on different types, hand-rolled standard functions, forwarding wrappers, repeated test fixtures. |
| R15 | Methods, not standalone functions. Attach behavior to a struct with `impl`. Builder methods that return `Self` are fine. Pass a method's inputs as struct state, not as loose parameters. |
| R16 | No nested control flow around a loop, in either direction. A loop inside another loop, an `if`, an `else`, or a `match` arm counts as nested, and so does an `if`, `else`, or `match` inside a loop body. Prefer iterators: the loop becomes an iterator chain (`filter`, `map`, `filter_map`, `try_for_each`, and so on). If a loop must stay, its body is one step: one call to a named helper, and the dispatch on its result (`return`, `break`, or `continue`). Everything else moves into the helper. |

## Writing

Write comments, doc comments, commit messages, and pull request text in Simplified
Technical English: short sentences, active voice, present tense, simple words, one
instruction per sentence.

## Behavior preservation during refactors

- The only intended behavior changes in a style refactor are documented defect fixes,
  each with a test.
- Never change the iteration order over a map or set that feeds a hash, a state root, a
  write set, or a wire encoding.
- Keep early-return and side-effect order when a loop becomes an iterator chain.
- Keep drop order for Aeron runtimes and cluster guards. Keep lock scopes. Keep the
  point at which a channel sender closes.
- Keep log message text and fields, metric names, and CLI flag names.

## Mechanical checks

`just style` runs, for the whole workspace with all features:

1. `cargo clippy --all-targets -- -D warnings -W clippy::pedantic`
2. `cargo clippy` with `too_many_lines` at threshold 100 (the mandatory R2 bound)
3. `cargo check` with `-W unreachable_pub`
4. `cargo fmt --check`
5. A grep that fails on `debug_assert!`, `.max(1)`, `Box<dyn`, and
   `allow(clippy::too_many_arguments)` in production code

The judgment rules (R1, R5, R6, R7, R9, R10, R14, R15, R16) are reviewed by reading.

## Audit

The audit of 2026-09-07 applied these rules to the whole workspace. Its report, per-crate
findings, and status files are under `docs/reviews/2026-09-07-code-quality-audit/`.
