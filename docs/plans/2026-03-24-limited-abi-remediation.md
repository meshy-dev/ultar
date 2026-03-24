# Ultar Limited ABI Remediation Implementation Plan

> **Execution:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.
> **Plan Review:** Before execution, dispatch a subagent to review this plan using the appropriate review skill.
> **Important:** This plan is not implementation code. Do not write code or heavy pseudocode in this document.

**Goal:** Remove non-compliant Limited-API object/type access from Ultar's Python binding and replace it with documented Python 3.11 abi3-compatible lifecycle handling using module-owned type state.

**Architecture:** Keep the current Zig extension structure and abi3 packaging intact, but redesign the binding around module-owned state rather than process-global type pointers. The implementation should move from single-phase, global-type initialization to a clean module-state design with module-associated heap types, a small explicit `ModuleState` helper layer, and documented lifecycle handling for allocation, partial initialization, teardown, and test coverage.

**Tech Stack:** Zig, CPython Limited API / Stable ABI (`Py_LIMITED_API=0x030b0000`), setuptools abi3 wheel packaging

---

## What Already Exists

- `python/python.zig` already contains the full binding surface: type definitions, alloc/dealloc paths, row wrapping, and ownership comments.
- `build.zig` and `python/setup.py` already agree on `_native.abi3` and `cp311` wheel tagging; they are verification targets, not redesign targets.
- `python/tests/test_dataloader.py` and `python/tests/loader_script.lua` already define the current public behavior and Lua-side loading shape.
- Previous commits in this repo already show lifecycle fragility in this area (`087626b`, `4ce71c3`), so teardown and ownership paths deserve extra rigor.

## Architecture Diagram

```text
Before
======

process globals
  |
  +--> DataLoaderType
  +--> LoadedRowType
           |
           v
    wrapOwnedRow() allocates rows

After
=====

module object
  |
  v
+-------------------+
| ModuleState       |
| - DataLoaderType  |
| - LoadedRowType   |
+-------------------+
   |            |
   |            +--> wrapOwnedRow() resolves LoadedRowType from module state
   |
   +--> module init creates module-associated heap types

instance lifecycle
  DataLoader.__new__ -> native loader init -> iteration -> LoadedRow alloc
            |                                  |
            +------ partial-init cleanup ------+
                                               |
                                  clear/dealloc destroy exactly once
```

## Not in Scope

- Broad Stable-ABI cleanup outside `python/python.zig` - this plan stays focused on the Ultar binding.
- Packaging refactors in `build.zig` or `python/setup.py` unless implementation uncovers a concrete abi3 mismatch.
- Explicit repeated-import or multiple-module-object verification in this change - deferred to a follow-up TODO.
- Repo-local fixture reuse from `test_data/` - this plan uses generated temporary fixtures instead.

### Task 1: Convert module initialization to module-owned state

**Outcome:** `python/python.zig` no longer relies on process-global type pointers and instead creates module-associated heap types stored in explicit module state.

**Why it exists:** The current binding combines a Limited-API violation with process-global type state, and the review chose a full module-state redesign rather than a narrow patch.

**Files:**
- Modify: `python/python.zig`

**Implementation Notes:**
- Replace the current single-phase, global-type setup with module-owned state and module-associated type creation.
- Add one small explicit `ModuleState` struct and tiny lookup helpers; do not introduce a broader context abstraction.
- Remove `pyType()` and any equivalent direct `PyObject` layout access while doing the init refactor.
- Update the top-of-file ownership/object-hierarchy comment in `python/python.zig` so it matches the new module-state design.
- Add an inline ASCII diagram comment near module init and type/state ownership if the final code still spans multiple non-obvious steps.

**Verification:**
- Run: `grep -n "ob_type\|pyType\|Py_TYPE" python/python.zig`
- Expect: no direct object-header access remains in `python/python.zig`

**Open Questions / Risks:**
- Module-state redesign implies import/init changes, so partial-init cleanup of the module and its types must be explicit and easy to audit.

### Task 2: Align heap-type lifecycle and partial-init rules

**Outcome:** `DataLoader` and `LoadedRow` follow documented Limited-API-compatible allocation, partial-init, clearing, and destruction rules under the new module-state design.

**Why it exists:** Fixing `ob_type` access alone is not enough if the surrounding heap-type lifecycle still assumes CPython internals or uses a deallocation pattern that depends on hidden layout details.

**Files:**
- Modify: `python/python.zig`

**Implementation Notes:**
- Revisit both heap type specs in `python/python.zig`, but only enable GC where the object graph requires it.
- `LoadedRowObject` owns a Python reference to `parent`, so if it is GC-tracked then it must define `Py_TPFLAGS_HAVE_GC`, `tp_traverse`, and `tp_clear`, with documented ordering around clear/dealloc.
- `DataLoaderObject` still needs correct heap-type destruction semantics even if it does not join GC.
- Make partial-init rules explicit: Python-owned fields must be zeroed before fallible work, and clear/dealloc paths must tolerate partially initialized instances.
- If a custom `tp_dealloc` remains, make the documented heap-type rule explicit: free instance-owned resources, call the correct free path for the type, and then `Py_DECREF` the heap type object.
- Use `PyObject_GC_UnTrack()` only for types that actually set `Py_TPFLAGS_HAVE_GC`.
- Preserve the default `tp_free` unless the docs require otherwise; do not introduce a custom free slot as part of this remediation.
- Preserve the existing native ownership invariants: `DataLoaderObject.loader` is destroyed once, and `LoadedRowObject.row` is reclaimed once before the parent reference is released.

**Verification:**
- Run: `zig build python-bindings -Doptimize=ReleaseSafe`
- Expect: `_native.abi3` still builds successfully with no new Python C-API compile errors
- Run: `python -m build --wheel --no-isolation python/`
- Expect: wheel build still emits an abi3 wheel tagged for `cp311`

**Open Questions / Risks:**
- The exact boundary between `tp_clear`, `tp_finalize`, and `tp_dealloc` should be validated against Python 3.11 docs during implementation so cleanup remains correct under both refcount-driven destruction and GC.
- GC tracking semantics depend on how the type is allocated through `tp_alloc`; avoid double-tracking or mixing GC and non-GC free paths.
- Module-state lookup should stay explicit and small; if the implementation starts adding many helpers, stop and simplify.

### Task 3: Add regression coverage for binding lifecycle behavior

**Outcome:** The Python binding has repeatable regression coverage that exercises object creation and teardown in a way that can catch interpreter-crashing lifecycle bugs.

**Why it exists:** abi3 compliance bugs often hide in destruction and cleanup paths, so the implementation needs a regression check beyond a compile-only change.

**Files:**
- Create: `python/tests/test_limited_abi.py`
- Modify: `python/tests/test_dataloader.py` only if existing helpers are genuinely worth reusing

**Implementation Notes:**
- Require at least one subprocess-style regression that imports the extension, constructs and tears down objects repeatedly, and asserts clean process exit; this is the only reliable way to catch some teardown crashes.
- Generate temporary fixtures for the tests rather than depending on a machine-specific dataset.
- Generate the temporary fixture once per test module or session and reuse it across subprocess checks.
- Add explicit negative-path tests for constructor failure and teardown-adjacent error paths, not just happy-path iteration.
- Focus coverage on import, object construction, row wrapping, reference release, and repeated cleanup or `gc.collect()` scenarios that would surface invalid deallocation behavior.
- Do not add repeated-import or multi-module-object coverage in this change; that follow-up is tracked in `TODOS.md`.

**Verification:**
- Run: `python -m pytest python/tests/test_limited_abi.py -q`
- Run: `python -m pytest python/tests/test_dataloader.py -q`
- Expect: the binding imports, iterates, and tears down cleanly without crashes or refcount-related failures, and the subprocess regression exits successfully

**Open Questions / Risks:**
- Temp-fixture generation must stay shared and cheap, or the test suite will become slow enough that people stop running it.

### Task 4: Re-check the remaining Limited-API surface in the binding

**Outcome:** The post-fix binding has a short documented list of any remaining Python C-API usages that were reviewed and intentionally left in place.

**Why it exists:** The original issue surfaced through one wrapper, but the same file contains other boundary-touching APIs that should be explicitly reviewed before calling the abi3 work done.

**Files:**
- Modify: `python/python.zig`
- Review: `build.zig`
- Review: `python/setup.py`

**Implementation Notes:**
- Review the built-in type checks, thread-state APIs, and allocation paths in `python/python.zig` against Python 3.11 Limited-API documentation.
- Only change packaging files if the remediation reveals a mismatch with the already-declared `cp311` abi3 target.
- Keep the result small and grep-able so future audits can see which APIs were intentionally retained.
- Record the approved remainder in this plan document rather than in a source-code audit comment.

**Verification:**
- Run: `zig build python-bindings -Doptimize=ReleaseSafe && python -m build --wheel --no-isolation python/`
- Expect: the extension and wheel packaging still agree on `_native.abi3` and `cp311-abi3`

**Open Questions / Risks:**
- Some APIs may be part of the Stable ABI even if they are not commonly used in limited-API examples; implementation should distinguish "documented and acceptable" from "happens to compile today".

**Reviewed remainder after implementation:**
- Built-in type checks remain intentionally in place via `PyObject_IsInstance(..., &PyUnicode_Type / &PyLong_Type / &PyDict_Type)` because those built-in type objects are documented Stable-ABI surfaces for the Python 3.11 floor.
- GIL handoff around native loader construction remains intentionally in place via `PyEval_SaveThread` / `PyEval_RestoreThread`; both are documented Stable-ABI thread-state APIs and the work done between them avoids Python object access.
- Module-owned state remains intentionally implemented via `PyModule_GetState`, `PyModuleDef_Init`, `PyType_FromModuleAndSpec`, `PyType_GetModuleState`, and `PyModule_AddObjectRef`; these are the documented multi-phase/module-state APIs for the 3.11 limited-API target.
- Heap-type allocation and teardown remain intentionally implemented via `PyType_GetSlot` for `tp_alloc` / `tp_free`; future refactors should preserve the documented `tp_free` lookup plus heap-type `Py_DECREF` pattern rather than reintroducing direct object-header access.
- Packaging declarations remain aligned without further changes: `python/python.zig` keeps `Py_LIMITED_API=0x030b0000`, `build.zig` builds and copies `_native.abi3`, and `python/setup.py` keeps `py_limited_api=True` with wheel tag `cp311`.

### Rollout Notes

**Order of execution:** Complete lifecycle remediation in `python/python.zig` before adding or updating tests, then rerun the packaging path.

**Success criteria:**
- No direct `PyObject` header access remains in the binding.
- No process-global type pointers remain in the binding.
- The extension still builds as `_native.abi3` for the existing `cp311` target.
- The revised object lifecycle passes Python-level smoke coverage without teardown crashes.
- Remaining Python C-API calls in the binding have been consciously reviewed against Limited-API documentation.

**Recommended review focus:** Use `plan-eng-review` in a subagent to pressure-test the GC/deallocation assumptions before implementation starts.
