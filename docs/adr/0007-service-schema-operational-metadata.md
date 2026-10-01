# Operational metadata lives in a dedicated `service` schema, not the versioned raw datalake

The extract stage produces two kinds of non-versioned housekeeping data alongside the versioned raw tables: the `loaded_files` load log (load signatures guarding re-extraction) and the level-coded `boundaries` reference layer the transform stage joins against. These live in a dedicated `service` schema (`service.loaded_files`, `service.boundaries`) rather than inside the raw layer, keeping the versioned datalake purely for source records.

## Context

ADR 0004 describes raw as append-only versioned tables and, originally, also placed the boundary reference layer in that schema (`raw.boundaries`). During implementation the boundary reference layer and the `loaded_files` log were moved into a separate schema (first named `serv`, renamed `service`). The raw-versioning semantics — append-only per-source tables, never dropped, load-signature guard — are untouched; only the housekeeping tables moved. The specs and glossary never followed, so every doc still pointed at `raw.boundaries` while the code wrote `service.boundaries`. The review convention (files win, note the discrepancy) and subsequent reviews (multiple code reviews flagged the doc drift) mean the decision itself is sound but the documentation must catch up.

## Decision

Extraction writes the load log and the boundary reference layer into a dedicated `service` schema:

- `service.loaded_files` — the append-only load-signature log (see ADR 0004).
- `service.boundaries` — the single non-versioned, level-coded reference layer (`country_iso='DEU'`, `name`, `level` 0=country outline / 1=states+EEZ / 2=regions / 3=districts, `area` in km² via PostGIS). A `geojson` column holds the geometry pre-simplified to WGS-84 GeoJSON at load (`ST_SimplifyPreserveTopology(geometry, 0.001)`, issue #31), backfilled idempotently on every boundaries run so the viz app reads stored geometry instead of re-simplifying per rerun.

`service` is operational metadata, not a pipeline data layer: the data model stays four layers (raw / staging / core / marts); `service` is the side-car the pipeline reads from and writes bookkeeping to. The transform stage spatial-joins against `service.boundaries`; the extract stage records load signatures in `service.loaded_files`.

The line is *the pipeline's own bookkeeping*, not "everything that is not unit data". Two later records extend the schema on the same reasoning, and one of them deliberately does not use it:

- `service.ingestion_runs` / `stage_results` / `source_memberships` — the event-driven deployment's processing ledgers (ADR 0009). Bookkeeping about the pipeline's own work, so `service`.
- `service.bootstrap_alerts` — the blocked-startup alert claims (ADR 0009). Bookkeeping about the deployment's own state, so `service`.
- `marts.definition_fingerprints` — which pivot SQL each stored view was created from (ADR 0003). This is *not* `service`: it describes a view in the `marts` schema and is meaningless without the view, so it lives beside the views it describes, is dropped and recreated with them, and `drop_all_pipeline_data.sql` clears it with the rest of that schema. Recorded here because it is the one place this ADR's rule is deliberately not followed, and a later reader comparing the two ADRs should not have to rediscover the exception.

## Consequences

- The docs must say `service.boundaries` / `service.loaded_files` everywhere an earlier doc said `raw.boundaries` — this ADR supersedes that part of ADR 0004.
- Boundary files remain reference data, loaded once (not versioned). Current extract re-creates `service.boundaries` on each boundary run; ADR 0004's load-if-not-exists guard is documented intent but not yet implemented — a known discrepancy, recorded here rather than silently assumed.
- A future reader sees no undocumented `service` schema in the codebase: its purpose is recorded here, in the glossary, and in the spec's data-layer description.