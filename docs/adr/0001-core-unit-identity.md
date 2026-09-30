# Unit identity: reference_id in staging, serial key in core, snapshot-authoritative updates

Superseded in part by issue #36 (event-driven Source snapshots). `CONTEXT.md` is the authority where this ADR and it disagree; the freshness gate and the old synthetic payload are both withdrawn.

Staging identity derives from `reference_id` (canonicalized `energy_source` + `reference_id`). Rows without a `reference_id` get a synthetic hash built from canonical Energy source, `location`, `x_coordinates` and `y_coordinates` rounded to six decimals, and `geo_accuracy`. Capacity, Reference Date, and commissioning date are **not** part of the synthetic payload: an identity must not move when a unit's data changes, or the same physical unit would be re-inserted on every update.

Core carries no natural key as its identity; it replaces whatever staging identity a row arrived with by a serial integer `unit_id`, and keeps a data-integrity unique index on `(energy_source, reference_id)` where `reference_id` is present, which is not the update key. The synthetic hash is therefore not stored in core, but it is *recomputed* on each load from the row's `location` property, coordinates, and geo accuracy, because that is how a null-`reference_id` staging row is matched back to its core row.

Loads match an incoming staging row to core by that record identity — `reference_id` when present, the recomputed synthetic hash otherwise — and a complete Source snapshot is authoritative:

- matched row present in the snapshot: `UPDATE` in place (surrogate `unit_id` stays stable, dimensional property links are refreshed);
- row absent from the snapshot: retained untouched, so historical units survive;
- unmatched row: insert;
- `bad_quality` row: creates no new Core row.

`reference_date` no longer gates the update. It is provenance carried on the row, not an ordering rule: a snapshot is a complete statement of the Source at delivery time, so a row it carries is the current truth even when its Reference Date is older than what Core already held. The former "skip if not fresher" behaviour is withdrawn — it made Core depend on arrival order and let a stale re-ingestion win or lose arbitrarily. The old `reference_id` + `reference_date` update key is withdrawn with it, because provenance and identity must not be coupled.

Core rows with a null `reference_id` must carry a `location` property. Without it the synthetic identity cannot be reconstructed, and a load must fail loudly rather than silently treat the unit as unmatched and insert a duplicate.

Geometry is carried as data, never identity. Core retains the `geometry` point (`geometry(Point, 4326)`) and additionally stores `longitude` / `latitude` for serving; spec v2.3 briefly removed the point from core and was amended back to keep it.

Still rejected: delete+insert-on-update (regenerates `unit_id`, orphaning property links and administrative bookkeeping).
