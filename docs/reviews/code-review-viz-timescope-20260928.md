# Code review — Historical Core timescopes in visualization (issue #9)

- **Date:** 2026-09-28
- **Fixed point:** `136ea6e` (branch `aws-workflow` HEAD before the change)
- **Diff:** `git diff 136ea6e...HEAD` — 7 files, 455 insertions, 24 deletions
- **Commits:** `7829c8a` (Show historical Core timescopes in visualization (#9))
- **Reviewed artifacts:** `viz/data.py` (`ACTIVE_UNIT_PREDICATE`, `units_query` docstring), `viz/config.py` (timescope default comment), `tests/test_viz_timescope.py` (new integration module), `tests/test_viz_data.py` (timescope/marts/membership seams), `tests/test_marts.py` (`TestCurrentDatePivots`), `tests/conftest.py` (`drop_core_tables`), `docs/adr/0003-marts-as-stored-wide-pivots.md` (Outcome section)
- **Spec source:** issue #9, *Show historical Core timescopes in visualization* (parent #1, blocked by #4); `CONTEXT.md` **Active unit** (line 54) and **Historical unit** (line 57); `docs/adr/0003-marts-as-stored-wide-pivots.md`
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`, `docs/adr/*`, prior reports in `docs/reviews/` (notably `code-review-source-snapshot-slice-20260927.md` and `code-review-viz-iconlayer-loader-20260924.md`)
- **Verification:** `.venv/bin/python -m pytest` — **461 passed**; `python -m compileall -q viz tests etl` clean

Both axes were run as parallel sub-agents over the same diff, then their findings
were actioned. This report records what they said and what was done about it.

## Standards

**Hard violations: none.** The predicate matches `CONTEXT.md`'s **Active unit**
definition exactly (commissioning on/after `from`, on/before `to`; decommissioning
null or on/after `to`; membership irrelevant), and **Historical unit** /
**Source membership** are used per glossary. The new module's module-level
`ENGINE = create_engine(os.environ["DATABASE_URL"])` is safe: `tests/conftest.py`
redirects `DATABASE_URL` at import time, before test modules import — same pattern
as `tests/test_marts.py`.

**Test-pattern conformance: yes.** Module docstring, module-scoped
`_core_with_probes` (drop → load both kinds → insert probes → teardown drop)
mirrors `tests/test_marts.py`'s `_loaded_core`; the `_staged_sources` dependency,
unique `reference_id` probes and teardown all follow the established shape.
Test-only helpers (`_probe_frame`, `_probe_labels_returned`, `_as_date`,
`_insert_probes`) are each used by multiple tests — justified infrastructure, not
speculative.

**ADR amendment placement: right place, judgement call.** The decision is a direct
consequence of ADR 0003's kept constraint ("the future dashboard ticket must
aggregate from core tables"), and the ADR already anticipated that ticket. A
separate ADR would fragment a two-sentence decision; the predicate semantics
already live in `CONTEXT.md`'s Active unit definition, so no glossary gap.

### Baseline smells (judgement calls) — all actioned

1. **Duplicated Code — `tests/test_viz_timescope.py` `_drop_core()`** repeated
   `tests/test_marts.py` logic-for-logic, with the kind tuples inlined. The
   source-snapshot review demanded one home for duplication.
   → **Fixed**: extracted to `drop_core_tables(engine)` in `tests/conftest.py`;
   both modules call it, and `test_marts.py`'s now-unused `GENERATOR_KIND` /
   `STORAGE_KIND` constants were removed.
2. **Duplicated Code / redundant coverage — 9 of 10** tests in
   `TestIntervalEndpoints` / `TestDecommissioningDate` were logical subsets of
   `test_returns_exactly_the_units_active_in_the_interval`, each re-running the
   full fetch to assert one label's membership.
   → **Fixed**: collapsed into two `(table, label)` parametrizations —
   `test_a_unit_active_in_the_interval_is_returned` and
   `test_a_unit_outside_the_interval_is_excluded`.
3. **Speculative Generality (minor) — `_source_memberships` yielded `run_id`**;
   no test consumed it.
   → **Fixed**: the fixture no longer yields.
4. **Mysterious Name (minor) — `PROBE_KIND`** mapped table → *energy source*
   (`{"generators": "wind", ...}`); the values aren't kinds.
   → **Fixed**: renamed `PROBE_SOURCE`.
5. **Comment accuracy — the `PROBE_DATES` comment** said "every probe is
   commissioned inside the interval except the two…"; there are three outside.
   → **Fixed**: the comment names all three.

## Spec

**(a) Missing / partial — two findings, both actioned.**

1. **The membership-absence criterion was only partially proven.** Spec: *"Source
   membership absence does not remove a retained Core unit from historical
   queries."* The original `test_a_retained_unit_without_source_membership_is_returned`
   asserted on probe `"inside"` — a freshly inserted row that was never in any
   Source snapshot, not a retained unit (`CONTEXT.md:57`: *"A Core unit retained
   across Source snapshot changes"*). The `_source_memberships` fixture inserted
   one membership for a loaded fixture unit, but no assertion ever observed that
   unit (the `area_names=(PROBE_STATE,)` filter excluded it from every probe
   frame). The fixture was decorative.
   → **Fixed**: added `test_a_loaded_core_unit_absent_from_source_membership_is_returned`,
   which reads a real fixture-loaded Core unit's dates straight from
   `core.generators` and asserts the timescope returns that exact
   (commissioning, decommissioning) pair — a unit that is a member of no Source
   snapshot here, so only the Core timescope can return it.
2. **Criterion 6 "historical retained units"** — same gap.
   → **Fixed** with the above; the probe-level test is kept as the
   both-kinds/endpoint coverage.

**(b) Scope creep: none.** The ADR 0003 "Outcome" section documents the decision;
   the comment and mart-pinning test additions map to criteria 5 and 6.

**(c) Implemented but wrong — one finding, actioned.**

- **Most probes passed against the old predicate**; only `after_to` exclusion and
  the exact-set equality pinned the new upper bound. `at_to` proved inclusivity
  but not that the bound is *required*.
  → **Fixed**: `test_returns_exactly_the_units_active_in_the_interval` is the
  primary assertion and is verified to fail against the old predicate (7 failures
  when the bound is reverted), so the bound is load-bearing, not decorative.
- **The membership fixture could not fail for the reason the test claimed** (no
  retained unit was ever exercised against it).
  → **Fixed** with finding (a)1 above.

**Remaining partial coverage (accepted, not a defect):** criterion 4's
"geometry fields" is covered as `longitude`/`latitude` — the unit projection
carries no `geometry` column (geometry lives in `service.boundaries`), and
`district` is not asserted. Criterion 5's DB half (the marts really are
materialized views) is already covered by the existing
`test_three_materialized_views_exist` in `tests/test_marts.py`; the added tests
pin the current-date rule that makes them unusable as a substitute.

## Summary

- **Standards:** 0 hard violations; 5 baseline smells, all actioned.
- **Spec:** 2 missing/partial findings and 1 wrong-implementation finding, all
  actioned; no scope creep.
- **Worst issue per axis:** Standards — the duplicated Core drop (fixed);
  Spec — the membership-absence criterion resting on a probe rather than a
  retained unit (fixed).
