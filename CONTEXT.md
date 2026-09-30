# Renewable Energy Installations in Germany

Geospatial registry of renewable energy generation and storage units in Germany, enriched with administrative and maritime boundaries for analyzing installed capacity by energy source and region. Rows move through four PostGIS layers — raw (versioned datalake), staging (enriched + quality-gated), core (consolidated, serial-keyed), marts (active-unit pivots).

## Language

### Units

**Unit**:
A single renewable energy installation in the registry, either a generator or a storage unit. Every unit has a stable identity in the staging layer derived from its source dataset.
_Avoid_: Plant, system, installation, record

**Generator**:
A unit that produces electricity: Bio, Gas, Hydro, Solar, or Wind.
_Avoid_: Power plant

**Storage**:
A unit that stores energy (Battery, Pumped storage, Hydrogen storage) rather than producing it.
_Avoid_: Storage unit, Energy storage

**Energy source**:
The canonical type of a unit: Bio, Gas, Hydro, Solar, Wind, or Storage. Canonicalized from the varied labels in source datasets (e.g. "Bioenergy", "Solar Energy", "Energy Storage").
_Avoid_: Bioenergy, Solar Energy, Wind Energy, Hydropower (as values — keep only as original source attributes)

**Source dataset**:
The single homogeneous collection of units belonging to one Energy source, published as one GPKG. Its source label is carried in each unit's `energy_source` value and canonicalized during ingest.
_Avoid_: Source file, energy source

**Source snapshot**:
The complete published contents of one Source dataset at one point in time, rather than an incremental change set. It is authoritative for both the values and Source membership of every unit it contains.
_Avoid_: Delta, patch

**Source membership**:
A unit's presence in a particular complete Source snapshot, including rows that fail quality checks. It is lineage metadata and does not determine whether the unit is Active; a newer snapshot ending membership does not deactivate the Core unit.
_Avoid_: Active, decommissioned

**Storage type**:
The technology class of a storage unit: Battery, Pumped storage, Hydrogen storage.

**Installed capacity**:
Nameplate output of a generator, in kilowatts (kW).
_Avoid_: Nominal power, nameplate rating

**Storage capacity**:
Usable energy storage capacity of a storage unit, in kilowatt-hours (kWh).

**Commissioning date**:
The date a unit was put into operation.

**Decommissioning date**:
The date a unit was taken out of operation; null means no decommissioning date is recorded.

**Active unit**:
For a selected interval from `from` through `to`, a unit whose commissioning date is on or after `from` and on or before `to`, and whose decommissioning date is null or on or after `to`. Source membership does not affect Activity. Only Active units contribute to mart pivots for the selected interval.

**Historical unit**:
A Core unit retained across Source snapshot changes. Snapshot absence alone does not make it inactive; it remains Active until its dates say otherwise. Historical units remain visible for audit and visualization.
_Avoid_: Deleted unit

**Geo accuracy**:
The precision of a unit's coordinates: 1 = exact location of the facility, 2 = centre of the municipality (imprecise). Geo accuracy 2 is a plain column, never a quality flag.
_Avoid_: Accuracy, coordinate precision

**Reference ID**:
The identifier of the record in its original source dataset. Unique within and across all source datasets. Used to build the staging unit_id, not the core identity.

**Reference date**:
The full-source timestamp of a record. It is descriptive source metadata, not the freshness authority for an incremental load; Source snapshot publication order determines which values are current.

**Synthetic identity**:
A generated staging unit_id for units lacking a Reference ID (39 solar rows), derived only from stable unit attributes: Energy source, location, rounded coordinates, and geo accuracy. It is persisted across snapshots; a collision or ambiguity fails the snapshot. Staging-only: Core replaces it with a serial key and never flags it.

### Geography

**Boundaries**:
The single level-coded `service.boundaries` table of administrative and maritime polygons (0 country outline, 1 states + EEZ, 2 regions, 3 districts) used to assign each unit its state, region, and district by spatial join.

**Boundary release**:
A new published version of the polygons at one boundary level. It replaces every polygon at that level and requires every unit's administrative geography to be rederived. Levels published in one message are applied together as one atomic batch, followed by a single geography rebuild of staging, Core, and the marts.
_Avoid_: Partial boundaries, full boundary rebuild

**Geography rebuild**:
The single pass that rederives every unit's state, region, and district after a Boundary release: re-enrich each Source's staging table, then every Core unit of both kinds from its own geometry, then the state-dependent collisions and the marts. It runs once per release batch, whatever the number of levels it carried.
_Avoid_: Enriching only the changed level, rebuilding from staging

**State**:
A Bundesland (federal state) or, for offshore units, the sea/EEZ area they fall in. A unit that joins to no boundary row keeps a null state, is flagged `collision`, and is reported under the "outside" bucket in the marts.
_Avoid_: Land, Region

**Region**:
A Regierungsbezirk (administrative region).

**District**:
A Landkreis (administrative district).

**Offshore**:
A wind unit located at sea, enriched against the EEZ state layer rather than onshore boundaries.

**Coordinates**:
The WGS-84 longitude/latitude pair of a unit. Carried as `x_coordinates` / `y_coordinates` in raw and staging, and as `longitude` / `latitude` in core, where it rides alongside the retained `geometry` point.
_Avoid_: Position, Lat/lng

### Data stages

**Raw version**:
A dated snapshot table `raw.<source>_<YYYYMMDD>_<n>` produced by one extract load. Every load appends a new version; versions are never dropped or overwritten.

**Ingestion run**:
One processing lifecycle for one immutable S3 object version or local source file. It records the input identity, current stage, attempts, outcome, and any terminal error.
_Avoid_: Load signature, SQS message

**Queue message**:
One SQS message — the event-driven worker's unit of work, called a *message* in the docs and the code. It carries every S3 record the queue batched for it; those records are processed independently but in one order — Boundary records as a single release first, then Source records — and the message is acknowledged only once every record is successful, terminally skipped, or stale. Anything retryable is left for redelivery, which repeats only the unsettled records.
_Avoid_: Ingestion run, batch, file

**Rejected object version**:
An object version whose own content fails validation — a non-homogeneous Source GPKG, a row the transform cannot give an identity to, a Boundary level published under the wrong key. Because the version is immutable, re-reading it can only reach the same verdict, so its Ingestion run is *terminal* rather than retryable: the operator uploads a new version, which is a new run. The worker announces it once, on SNS. Deliberately narrow: only failures that are a property of the bytes, never a property of the environment.
_Avoid_: Failed run, rejected file, broken file, poison message

**Delivery attempt**:
One handing of a message to the worker, counted by SQS (`ApproximateReceiveCount`) rather than by the worker, so a redelivery after a crash counts as an attempt. An Ingestion run has at most five of them, matching the queue's `maxReceiveCount`; the fifth leaves the run *retryable* and the message destined for the DLQ, which is alarmed once by the DLQ alarm instead of by the worker.
_Avoid_: Attempt counter, try

**Load signature**:
The immutable identity of successfully extracted input. For S3 ingestion it is `(bucket, object_key, version_id)`; for local ingestion it is `(filename, filesize, modified_at)`. A signature is logged only after extraction verification succeeds; duplicate input is skipped unless a local run is forced with `-f`.
_Avoid_: Message ID, filename alone

**Raw**:
The extract layer: versioned per-source unit tables with secondary attributes folded into a `secondary_attributes` jsonb column. Records only — the load-signature log and the boundary reference layer live in the Service schema (see Service).

**Service**:
The operational-metadata schema, deliberately separate from the versioned raw datalake: `ingestion_runs` for processing lifecycles, the `loaded_files` success log (see Load signature), `source_memberships` for Source lineage, and the non-versioned level-coded `boundaries` reference layer used by the transform spatial joins. Non-versioned by design; only the unit tables are versioned.

**Unit key**:
The staging `unit_id`, persisted under that column name in `source_memberships.unit_key` so lineage and staging can be joined without qualifying which Core table the unit belongs to.

**Input kind**:
What an Ingestion run was handed: `source` or `boundary`. It is derived from the accepted S3 key, not the file content, and is recorded on the run for reporting; which processor handles an object is decided by the accepted-key set and the processor the worker is given.

**Accepted key**:
The fixed S3 key layout an object must land on to be ingested: six Source keys (`sources/{solar,storage,bio,wind,gas,hydro}.gpkg`) and four Boundary levels (`boundaries/level-{0,1,2,3}.gpkg`). The set is the worker's contract (`etl.ingestion.ACCEPTED_KEYS`), and Terraform declares the same list so the bucket policy can name the objects; `tests/test_terraform_config.py` fails if the two lists drift apart.
_Avoid_: filename, prefix

**Staging**:
The transform layer: raw rows enriched with state, region, and district via spatial joins, keyed by a natural `unit_id`, quality-gated by `bad_quality`, and with the whitelisted secondary attributes decomposed into normalized properties (the rest staying in `secondary_attributes`). Staging carries both the `geometry` point and explicit `x_coordinates` / `y_coordinates`.

**Core**:
The consolidated layer: `generators` and `storages`, each unit appearing exactly once, holding a serial surrogate key, the reduced `secondary_attributes` jsonb, the retained `geometry` point plus `longitude` / `latitude`, and collision flags. Core rows are updated in place and never deleted.

**Marts**:
The aggregation layer: three Postgres materialized views (installation counts, generation capacity, storage capacity) at state grain, computed from active units only.

**Properties**:
Normalized (name, value) attribute pairs of a unit, held per unit-kind — `generator_properties` for generators, `storage_properties` for storages — linked to the unit through `generator_units_properties` / `storage_units_properties` (ADR 0006). Also the home of quality annotations (`bad_quality`, `collision`, `close_to`).
_Avoid_: Parameters

**Secondary attributes**:
The raw/staging/core jsonb column holding a unit's source attributes that have no fixed column. Only the 14 whitelisted keys — `biomass_type`, `fuel_type`, `technology`, `reference_source`, `solar_type`, `note`, `location`, `alignment`, `inclination`, `hydro_type`, `inflow_type`, `manufacturer`, `rotor_diameter`, `hub_height` — are decomposed into `properties` in staging; the remaining keys (e.g. `biogas_unit`, `chp_unit`, `area_id`, `turbine_type`) stay in the json through staging and core. Decomposed keys are removed from the json, never duplicated.
_Avoid_: Properties (column name — now a table, not a column)

### Quality

**Bad quality**:
A staging flag on a record failing a transform-level check (installed_capacity ≤ 0 or null; decommissioning_date before commissioning_date; coordinates conflicting with geometry). The failing record remains a member of its Source snapshot but is excluded from Core, and a `bad_quality` property link carries the newline-joined descriptions. State-null is deliberately not a staging check — it is a load-stage collision.
_Avoid_: Error, anomaly

**Collision**:
A core flag on a unit flagged by a load-level check: two `geo_accuracy = 1` units less than 10 m apart, a unit the spatial join left outside every boundary (state null, reason "outside location"), an onshore-labelled unit inside the sea, or a storage with `storage_capacity ≤ 0` or null. Collision rows remain in core, annotated by property links (`collision`, and `close_to` naming the neighbouring unit).
_Avoid_: Issue, error, anomaly

**Close-to**:
The property link value naming the other unit's id when a unit is part of a close-location collision.