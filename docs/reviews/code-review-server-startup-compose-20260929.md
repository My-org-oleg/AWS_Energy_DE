# Code review — Server startup bootstrap + safe server Compose stack (issue #8)

- **Date:** 2026-09-29
- **Fixed point:** `4e20fc2` (branch `aws-workflow` HEAD) — uncommitted working tree
- **Diff:** `git diff 4e20fc2` plus the untracked `docs/adr/0009-server-startup-bootstrap-and-redrive.md`
- **Reviewed artifacts:** `etl/ingestion.py` (`StartupResult`, `run_startup`, `_apply_boundary_releases`, `_enqueue_current_sources`, `_release_ledgered`, `_has_run`, `_has_row`, `_s3_event_body`, `redrive`, `SQSAdapter.send_message` + `Boto3SQSAdapter.send_message`, `PipelineProcessor.process_boundaries` `run_id_factory`), `etl/__main__.py` (`startup` + `redrive` commands, `_echo_check_report`), `compose.yaml`, `build_compose.yaml`, `scripts/smoke_etl_container.sh` (server-contract section), `tests/test_ingestion.py` (15 new tests, `RecordingSQS`, helpers), `docs/containerization.md`, `docs/remote-deploy.md`, `.env.example`, `AWS.md`, `README.md`, `docs/adr/0009-server-startup-bootstrap-and-redrive.md`
- **Spec:** issue #8, *Run operator startup and the safe server Compose stack* (parent #1), blocked by #2/#5/#7 (all closed)
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/adr/*` (notably 0004, 0007), `TechnicalSpecification.md`, prior reports in `docs/reviews/`
- **Verification:** `.venv/bin/python -m pytest` — **475 passed**; `python -m compileall -q etl viz tests scripts docker` clean; `docker compose -f compose.yaml config` / `-f build_compose.yaml config` / `-f local_compose.yaml config` render and the server-contract greps pass

Both axes were run as parallel sub-agents over the same diff, then their findings were
actioned. This report records what they said and what was done about it.

## Standards

1. **High — hard breach (security best practice) — `etl/ingestion.py` `redrive`** The reviewer flagged the `key` filter as SQL injection via string concatenation. **False positive on inspection**: only the constant fragment `" AND object_key = :key"` is concatenated; the operator value is bound (`parameters["key"] = key`), and the sole f-string interpolation is the `SERVICE_SCHEMA` module constant. → The bind was renamed `:key` → `:object_key` anyway — the parameter name now matches the column, which is what made the hunk look suspicious.
2. **Medium — Duplicated Code — `etl/ingestion.py` `run_startup`** The early return and the `except` handler built the same refused `StartupResult` twice. → **Fixed**: extracted `_refused(report)`; the `bootstrap()` call got its own try/except returning a `metadata_ready=False` refusal, so a database-down startup fails with the clean report instead of a traceback (the worker is still refused either way).
3. **Medium — Duplicated Code — `etl/ingestion.py` `_release_ledgered` / `_has_run`** Two identical `SELECT 1 FROM service.<table> WHERE bucket/object_key/object_version_id` probes. → **Fixed**: one `_has_row(engine, table, object_id)` helper; both table names are internal constants.
4. **Medium — Duplicated Code — `etl/__main__.py`** The `startup` command's report head duplicated the `bootstrap` command's block verbatim. → **Fixed**: extracted `_echo_check_report(title, result)`; `startup` adds its release/enqueue lines after it.
5. **Low — Speculative Generality (accepted) — `run_id_factory`** Single caller, but ADR 0009 justifies it and the docstring explains the two sources of the ledger's run id. Kept; the factory was simplified to `lambda _: str(uuid.uuid4())` since it ignores its argument.
6. **Low — Divergent Change — `docs/remote-deploy.md`** The clone URL was rewritten `Khvostenko-OV/Energy_DE` → `My-org-oleg/AWS_Energy_DE`. → **Kept**: this repo's origin is the fork, and the deployment doc should point operators at the repo they are deploying.
7. **Low — judgement call, not actioned — `run_startup` except path** On a mid-enqueue failure the report says `enqueued=()` even if some sends succeeded. Fail-closed is ADR 0009's intent, the error is logged, and redelivery is idempotent (a duplicate message finds the first run succeeded and is skipped), so the understating report is safe.
8. **Low — smoke script parsing** The `published_ports` / `pipeline_block` greps are brittle, but smoke scripts are crude by convention here and CI has no lint gate. Kept.

## Spec

1. **Medium — criterion 7 partial** *"Compose configuration and startup-order smoke tests verify the deployment contract."* The smoke verified compose configuration only; startup order was delegated to `tests/test_ingestion.py` by a comment. → **Fixed**: the smoke's server-contract section now also asserts the compose-level startup order (the `pipeline` service's `depends_on: db: condition: service_healthy` block), with the behavioural order (bootstrap before worker, fatal-Boundary refusal) cross-referenced to the pytest seam, which verifies it against real PostGIS with fake AWS adapters.
2. **Minor nit — `run_startup` error visibility** The `except Exception` logged the failure but the CLI output did not show the detail. → **Fixed** with the separate `bootstrap()` try/except (finding 2 above); the refusal path is now a clean report.
3. **Verified correct (no action needed)**: worker refusal for missing *and* invalid Boundary; no Ingestion run created by the bootstrap (synthetic UUIDs in the `loaded_files` ledger, a plain UUID column with no FK per `db_utils.py`); known versions not re-enqueued in any of the four states (`_has_run` matches any run row; the worker keeps rows for succeeded/stale/terminal/retryable-DLQ); redrive covers failed + DLQ (both `RETRYABLE`) with `--key` narrowing; server stack drops the seed volume and the db port while `local_compose.yaml` keeps both; no scope creep found.

**Summary:** Standards — 8 findings (1 hard false positive, 4 fixed, 3 judgement calls); Spec — 3 findings (1 partial criterion fixed, 1 nit fixed, rest verified correct). Worst per axis: Standards — the duplicated refused-result blocks (fixed); Spec — criterion 7's startup-order smoke gap (fixed).
