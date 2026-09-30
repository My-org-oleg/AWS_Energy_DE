# Code review — ingestion contract and bootstrap foundation (issue #2)

- **Date:** 2026-09-25
- **Fixed point:** `584c594` — uncommitted working tree
- **Diff:** `git diff HEAD` plus untracked `etl/ingestion.py` and
  `tests/test_ingestion.py`
- **Reviewed artifacts:** `etl/ingestion.py`, `etl/__main__.py`,
  `etl/db_utils.py`, `requirements.txt`, `tests/test_ingestion.py`
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`,
  ADR 0004, ADR 0007, and neighboring ETL/test conventions
- **Spec:** issue #2, “Establish the event-orchestration contract and explicit
  bootstrap foundation,” with parent issue #1 as design context

## Standards

- **High — prepared metadata is not written in this ticket.**
  `source_memberships` and the new `loaded_files` columns are created but have
  no writer in this diff. This is required foundation work; later ingestion
  and Boundary tickets own the writes.
- **Medium — apparent ADR 0004 conflict.** The partial S3 identity index is
  unique only for non-null S3 identities, so local forced reloads remain
  unaffected. Parent issue #1 explicitly supersedes the old global
  append-only assumption for S3 identities.
- **Medium — duplicated `loaded_files` DDL.** `etl/ingestion.py` and
  `etl/db_utils.py` both owned the table shape. **Fixed:** `db_utils` is now
  the single owner and bootstrap calls its idempotent schema helper.
- **Medium — bootstrap reporting bypasses the existing report convention.**
  `BootstrapResult` and hand-written CLI output do not use `ReportBase` and
  `summary()`. This is a judgement call rather than a documented hard rule.
- **Medium — duplicated source vocabulary.** `SOURCE_KEYS` re-listed the six
  sources instead of reusing `SOURCE_NAMES` from `etl.config`. **Fixed.**
- **Medium — missing module and command docstrings.** Existing tests and CLI
  commands usually have them; no comments will be added because the active
  implementation instructions prohibit comments.
- **Judgement calls:** unused SNS dependency, repeated terminal-state set,
  unannotated three-tuple, redundant `metadata_ready`/`available` fields, and
  string-valued `StageResult.outcome`.

## Spec

- **Implemented incorrectly — immutable identity.** `S3ObjectId` accepted the
  `"null"` version sentinel even though the concrete S3 adapter rejected it.
  **Fixed:** the public value object now rejects it and has a regression test.
- **Missing/partial — fatal startup gate.** Reassessed and closed: this issue
  explicitly does not start the production worker. `worker_start_allowed=False`
  is the public startup decision, the integration test asserts it, and the
  bootstrap command exits non-zero, preventing a chained worker startup.
- **Implemented incorrectly — boundary keys.** This finding does not apply:
  parent issue #1 explicitly fixes the S3 keys as
  `boundaries/level-0.gpkg` through `level-3.gpkg`; local filenames are the
  pre-AWS workflow.
- **Missing/partial — SNS behavior.** SNS is a replaceable injected adapter,
  but no publication behavior is implemented or asserted. Deterministic-file
  SNS behavior is explicitly assigned to blocked issue #7, so this ticket only
  establishes the seam.

## Summary

- Standards: 6 reported concerns plus 5 judgement calls; 2 actionable items
  fixed (`loaded_files` ownership and source vocabulary). Worst resolved:
  two owners for one database-table shape.
- Spec: 3 reported concerns and 1 context correction; immutable identity was
  fixed, while the startup gate was confirmed by the explicit decision plus
  non-zero CLI exit. SNS behavior remains correctly deferred to issue #7.
