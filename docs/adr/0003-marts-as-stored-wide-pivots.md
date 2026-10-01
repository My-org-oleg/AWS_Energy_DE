# Marts are Postgres materialized views: three stored wide pivots at state grain

The three marts — `marts.installation_counts`, `marts.generation_capacity`, `marts.storage_capacity` — are Postgres `MATERIALIZED VIEW`s at state grain, refreshed on demand by the CLI rather than managed by pipeline code. Generation pivots are keyed by energy source; the storage pivot by `source_type`. Only active units contribute, so the counts pivot counts active units too, keeping all three marts consistent.

"Active" is the one rule in `CONTEXT.md` with its interval collapsed to the current date, and both of its bounds apply: `commissioning_date <= CURRENT_DATE AND (decommissioning_date IS NULL OR decommissioning_date >= CURRENT_DATE)`. A plant published before it starts running is data and stays in Core, but it is not installed yet; a unit switched off today has not ended before today, which is what the inclusive `>=` says. The old rule carried only the second bound, so a generator starting next year counted as installed.

A NULL `commissioning_date` excludes a unit, since `NULL <= CURRENT_DATE` is not true. That is deliberate — a unit with no commissioning date has not been shown to be installed, and the visualization excludes it for the same reason — but it is a real narrowing from the old one-sided rule for any row missing the date, so it is written down here rather than left to be discovered in a count.

This is the visualization's timescope rule with its interval collapsed to today, not a verbatim copy of `viz.data.ACTIVE_UNIT_PREDICATE`. That predicate spans `commissioning_date BETWEEN :from AND :to`, so evaluating it at a single day would mean commissioned *exactly* today; the marts ask the different and intended question — commissioned at any point up to today. The two agree for every unit commissioned in the past, which is every real one, so the map and the pivots resolve the same set.

The spec v2 titles this section "Creating Materialized Views" and lists exactly these three pivots, so Postgres materialized views satisfy it literally and remove a pipeline-owned refresh manager. Consequence, kept: the stored shape cannot answer date-range or decommissioned-inclusion queries, so the future dashboard ticket must aggregate from core tables with filters rather than read the pivots. Rejected pipeline-managed stored tables (a refresh manager we'd have to write) and a single narrow unit-grain fact mart (over-flexible versus the spec's literal pivots).

## Outcome (issue #9)

The visualization reads Core directly under one timescope predicate, `viz.data.ACTIVE_UNIT_PREDICATE`: commissioned inside the selected interval (on/after `from`, on/before `to`) and still running at `to` (decommissioning date null or on/after `to`). Both Core kinds are fetched under that one rule, and the marts are untouched — they stay current-date pivots, so a historical interval is answered by Core and never by the stored views. Source membership is deliberately not joined: a unit a newer Source snapshot dropped stays in Core and stays Active by its dates.

## Editing a pivot's SQL reaches an existing database

A materialized view is created once and refreshed thereafter, so a change to the pivot SQL above does not reach a database that already holds the view: it goes on executing the old rule, and reconciliation — which compares the stored cells against the SQL in `etl/marts.py` — reports drift on every build with nothing anyone can do about it.

So `_create_marts` keeps `marts.definition_fingerprints`, one row per pivot holding a SHA-256 of the SQL it was created from (whitespace collapsed first, so reformatting is not read as a change of meaning). A pivot whose recorded fingerprint differs from the current one is dropped and recreated in one transaction, and the row is rewritten; the replacement costs nothing a refresh would not, because a pivot holds no data of its own.

A view with *no* recorded fingerprint counts as stale rather than unknown. That is every database built before this table existed, and it is exactly the case that needs repairing — recording the current fingerprint over an unverified definition would leave the next build matching and never looking again. The digest is of the source SQL, not of `pg_get_viewdef`, because Postgres prints its own re-quoted normalisation of whatever it was handed and the two never match as text.

These fingerprints live in `marts`, not in `service`, which is the one deliberate exception to ADR 0007's rule and is recorded there: the table describes a view in this schema and is meaningless without it.
