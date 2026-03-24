# TODOs

## Re-import stress test for module-state binding

- **What:** Add a follow-up stress test that imports the extension multiple times in one process and verifies module-owned type state stays isolated.
- **Why:** The current plan widens the architecture to module-owned state but intentionally skips explicit repeated-import or multi-module-object verification.
- **Pros:** Closes the largest remaining review gap and gives direct evidence that the redesign solved the architecture problem it set out to solve.
- **Cons:** Adds test complexity and may require awkward import mechanics or a dedicated subprocess harness.
- **Context:** `python/python.zig` is being redesigned away from process-global type pointers toward module-owned state. The current implementation plan keeps subprocess lifecycle coverage and negative-path tests, but it explicitly defers repeated-import coverage.
- **Depends on / blocked by:** The module-state redesign in `docs/plans/2026-03-24-limited-abi-remediation.md` landing first.
