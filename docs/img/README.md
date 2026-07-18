# Diagrams

These JPGs (architecture + per-actor state diagrams, referenced from the
top-level `README.md` and `docs/failure-modes.md`) are **generated, not
hand-drawn**: they are rendered by matplotlib scripts (`arch_diagram.py`,
`state_diagrams.py`) that are not currently checked into the repo. To update
a diagram, regenerate it from the script rather than editing the raster —
and prefer committing the generating script alongside any future re-render
so the editable source lives here too.

Known trade-off (accepted): raster-only sources mean each re-render adds the
full image size to git history (~1 MB total today).
