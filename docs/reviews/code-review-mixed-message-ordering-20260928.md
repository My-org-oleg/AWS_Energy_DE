# Code review — Mixed-message ordering and the SQS worker (issue #6)

- **Date:** 2026-09-28
- **Fixed point:** `b827368` (branch `aws-workflow` HEAD) — uncommitted working tree
- **Diff:** `git diff b827368`
- **Reviewed artifacts:** `etl/ingestion.py` (`SQSAdapter.receive_message`,
  `Boto3SQSAdapter`, `Boto3SNSAdapter`, `LONG_POLL_SECONDS`/`IDLE_POLL_SECONDS`,
  `run_worker`), `etl/__main__.py` (`_client`/`_sqs_adapter`/`_sns_adapter`, the
  `worker` command), `tests/test_ingestion.py` (`ScriptedSQS`, `ThrottledSQS`,
  `FakeSqsClient`, `_queue_sources`, and the issue #6 section), `CONTEXT.md`
  (**Queue message**), `TechnicalSpecification.md` (Event-driven worker),
  `AWS.md` (The message contract)
- **Spec:** issue #6, *Process mixed SQS messages in ingestion order* (parent #1),
  with the scope boundary set by its blocked-by sibling #5 (already merged) and by
  #7 (retries, DLQ, SNS, visibility) and #8 (startup/Compose), both open.
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`,
  `docs/adr/*`, `TechnicalSpecification.md`, prior reports in `docs/reviews/`
- **Verification:** `.venv/bin/python -m pytest` — **419 passed**; `pyflakes`
  shows no new findings; `python -m compileall -q etl tests viz` clean

## Standards

1. **Medium — the three new worker tests re-derived a narrower schema harness
   instead of the `source_pipeline` fixture** (`tests/test_ingestion.py:2746`,
   `:2799`, `:2833`): each opened with its own `service_test_<uuid>` schema and a
   `try/finally` drop, while `source_pipeline` already provisions all five
   schemas and monkeypatches every module constant. AGENTS.md's hermetic-suite
   rule and the file's "use the real PostGIS schema" convention point the other
   way.
2. **Low — the glossary term collided with the one word the glossary cannot
   qualify.** A bare **Message** sat next to *Ingestion run*, whose `_Avoid_`
   list already says "SQS message"; the ubiquitous language owned the most
   generic noun in the domain. `docs/agents/domain.md` was satisfied literally,
   but the canonical term should be the precise one.
3. **Judgement — `process_next_message` was speculative generality:** public, one
   caller, no test. `code-review-source-snapshot-slice-20260927.md:68` kept a
   public seam because a test called it; nothing did.
4. **Judgement — `_records_message` became a Middle Man** after the `_message`
   extraction: a pass-through whose only content is a comprehension.
5. **Judgement — duplicated `import boto3` / `boto3.client(...)`** in
   `etl/__main__.py`: the diff added a third and fourth copy of the `_s3_adapter`
   shape.
6. **Judgement — `QueuedSQS` was a Mysterious Name:** it is not a queue but a
   scripted receive list, and its docstring had to say so. The casing also split:
   `FakeSQS`/`QueuedSQS` against `SqsMessage`/`FakeSqsClient`.
7. **Judgement — the `engine, s3, sqs, sns, processor` Data Clump** now travels
   three signatures deep; `process_one_message` already carried it.
8. **Judgement — comments** were added in `etl/ingestion.py` and the test file.
   The file's convention is comment-heavy, so this is consistent; noted because
   `code-review-ingestion-bootstrap-20260925.md:32` records the comment
   prohibition as the reason docstrings were added there.

Clean: the section banner, the `_s3_record`/`_message` extraction (real
duplication removal), `read_failures` injection, test naming and docstring shape,
the CLI command docstring, the poll-constant placement and comment, and all three
doc edits. No ADR conflict — 0004 and 0007 are untouched by the loop.

## Spec

### (a) Missing or partial

1. **"successful and terminally skipped records are safe to redeliver" — the
   terminal half is unreachable.** `RunState.TERMINAL` is only ever *read*
   (`etl/ingestion.py:797`, `:925`, `:1050`), never written: `_record_failure` is
   called solely with `RETRYABLE` (`:880`) and `STALE` (`:888`), and the new
   failure tests pin exactly that. Deferring terminalisation to #7 is right, but
   the docs must not claim it.
2. **The new docs assert a state the code cannot produce.** "terminally skipped"
   is presented as current behaviour in `AWS.md`, `TechnicalSpecification.md`,
   `CONTEXT.md` and the CLI help, while `_claim` never returns it. A message
   holding a permanently invalid file is documented as deletable and in fact is
   not.
3. **"uses the exact raw table or tables produced by that record" was untested
   inside a mixed message.** The ordering test asserted stage *names* only
   (`("transform", "bio")`, `("load", "generators")`); the per-record raw-table
   linkage was pinned only by a pre-existing source-only test, so criterion 4 and
   the final criterion's "per-table results" were half demonstrated in the mixed
   path this issue owns.
4. **Minor:** the worker tests feed one record per message, so "decodes all S3
   records" is proven by the adapter test plus the pre-existing
   `process_one_message` tests, not by `run_worker` itself.

### (b) Not asked for

5. **SNS plumbing is #7's, and forced on operators now.** `Boto3SNSAdapter` is
   new, untested, and never used: `sns` is threaded through `run_worker` but
   appears only as an unused parameter downstream. Meanwhile `--topic-arn` is
   `required=True`, so the worker refuses to start until an alerting topic is
   configured that nothing publishes to. `IDLE_POLL_SECONDS`/`--max-messages`
   are unrequested but harmless.

### (c) Implemented but questionable

6. **The loop is crash-fragile.** `run_worker` wrapped no exception handling and
   the CLI caught only `KeyboardInterrupt`, so an exception from
   `sqs.delete_message` — or from a receive — killed a worker that is meant to
   run constantly.
7. **The ack rule is not safe yet.** Because deterministic failures stay
   retryable, `--max-messages` bounds *receives*, not acknowledgements, so such a
   message redelivers until #7 classifies it. Expected, and #7's remit.

## Resolutions

| # | Finding | Resolution |
|---|---------|------------|
| S1 | Hand-rolled schema harness in the worker tests | Fixed. All three now use `source_pipeline`; the scratch-schema boilerplate is gone. |
| S2 | Bare **Message** in the glossary | Fixed. The term is **Queue message**, with the short form the docs use named in the definition. |
| S3 | `process_next_message` speculative | Fixed. Inlined into the loop; the public surface is now `run_worker` only, and the loop gained a test of its own. |
| S4 | `_records_message` Middle Man | Accepted. It still names a domain shape (one record per Source) and has six call sites; deleting it would spread the comprehension into every one. |
| S5 | Duplicated `boto3.client` factories | Fixed. One `_client(service)` helper. |
| S6 | `QueuedSQS` name | Fixed. Renamed `ScriptedSQS`; the added `ThrottledSQS` sits beside it. |
| S7 | `engine, s3, sqs, sns, processor` clump | Accepted. Bundling it in a context type would change `process_one_message`'s signature and every caller and test of it, which is issue #5's merged API. |
| S8 | Comments in the diff | Accepted. Consistent with `etl/ingestion.py` and the test file, which are comment-heavy by convention. |
| P1, P2 | TERMINAL unreachable; docs claim it | Accepted and made honest. The Spec and AWS docs now state that every failure is retryable today and that terminal classification arrives with #7. |
| P3 | Per-record raw table untested in a mixed message | Fixed. The ordering test now asserts the Source's `transform` result names the raw table its own `extract` wrote, and that the table exists. |
| P4 | `run_worker` decodes one record per test | Accepted. Covered by the adapter test plus the pre-existing multi-record `process_one_message` tests. |
| P5 | SNS adapter is #7's, `--topic-arn` required | Accepted. `process_one_message` (merged in #2) already requires an `SNSAdapter`, so the worker cannot run without one; a no-op adapter today would be a second speculative abstraction, and #7 fills this one in. The cost is that operators configure the topic before it is used. |
| P6 | Crash-fragile loop | Fixed. `run_worker` logs an unexpected failure, pauses, and takes the next message; the failed message was not acknowledged, so SQS redelivers it. Covered by `test_run_worker_survives_a_failed_receive`. |
| P7 | Retryable determinism redelivers | Accepted; issue #7. Recorded in the docs and on the issue. |

## Summary

- **Standards:** 8 findings (1 medium, 1 low, 6 judgement calls). 5 fixed, 3
  accepted with reasons.
- **Spec:** 7 findings (2 partial, 1 missing coverage, 1 scope creep, 2
  questionable implementation, 1 minor). 3 fixed with tests, 4 accepted with
  reasons — the two axes' worst findings were the docs overstating the terminal
  state and the loop dying on an unexpected error, both now closed.
