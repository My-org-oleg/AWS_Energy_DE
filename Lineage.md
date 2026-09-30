# Lineage and data layers

A single picture of how a row travels from a published GPKG in S3 to a number on
the dashboard, what lives in each schema, and where the AWS pieces sit. Written
from the code and `TechnicalSpecification.md`; where the spec and the code
disagree, the code wins and the discrepancy is called out (see
[Discrepancies](#5-discrepancies)).

- **Lineage** — which key identifies a record at each hop
- **Data layers** — the five schemas, their tables and their keys
- **AWS integration** — the event path, the container stack, CI/CD
- **What is left to the account** — the steps that need the AWS account or a
  human, not another line of code here

Physical schema names default to `raw`, `stage`, `core`, `service`, `marts`
(`etl/config.py`; each is overridable per environment). The layer is called
*Staging*; the schema it lands in is `stage`.

---

## 1. End-to-end lineage

```mermaid
flowchart LR
    subgraph aws["AWS / datalake"]
        S3[("S3 datalake bucket")]
        SQS[["SQS queue<br/>(+ DLQ)"]]
        SNS["SNS alert<br/>(email)"]
        CW["CloudWatch Logs"]
    end

    subgraph worker["ETL worker (python -m etl)"]
        ING["ingestion.process_one_message<br/><i>run, claim, stage results</i>"]
    end

    subgraph cli["CLI pass (python -m etl run-all)"]
        EX["extract"]
        TR["transform"]
        LD["load"]
        MT["marts<br/>refresh + verify"]
    end

    subgraph db["PostGIS database"]
        RAW[("raw<br/>versioned datalake")]
        SVC[("service<br/>operational metadata")]
        STG[("stage<br/>staging")]
        CORE[("core<br/>consolidated")]
        MARTS[("marts<br/>3 materialized views")]
    end

    VIZ["viz app<br/>Streamlit + PyDeck"]
    READER[["viz_reader<br/>read-only role"]]

    S3 -- "s3:ObjectCreated" --> SQS
    SQS --> ING
    ING -- "GetObject (versioned)" --> S3
    ING --> EX
    EX --> RAW
    EX --> SVC
    TR --> RAW
    SVC -. "service.boundaries" .-> TR
    TR --> STG
    TR --> SVC
    LD --> STG
    LD --> CORE
    LD --> SVC
    MT --> CORE
    MT --> MARTS
    VIZ --> READER
    READER --> CORE
    READER --> SVC
    ING -. "on failure" .-> SNS
    ING -. "logs" .-> CW
    classDef store fill:#eef,stroke:#557
    class RAW,SVC,STG,CORE,MARTS store
```

Read it as: **S3 is the only input**, `service` holds the bookkeeping and the
reference geography, `raw` keeps an append-only history, `stage` is the working
copy, `core` is the consolidated truth, `marts` is the pre-aggregated serving
shape, and the `viz_reader` role is the only door the app has into the database.

---

## 2. Data layers

### 2.1 Schema map

```mermaid
flowchart TB
    subgraph rawL["raw — append-only versioned datalake (ADR 0004)"]
        R1["bio_YYYYMMDD_n"]
        R2["gas_YYYYMMDD_n"]
        R3["hydro_YYYYMMDD_n"]
        R4["solar_YYYYMMDD_n"]
        R5["wind_YYYYMMDD_n"]
        R6["storage_YYYYMMDD_n"]
    end

    subgraph svcL["service — operational metadata (ADR 0007)"]
        S1["loaded_files<br/>load-signature log"]
        S2["boundaries<br/>level 0-3 polygons"]
        RUNS["ingestion_runs<br/>one per S3 object version"]
        RESULTS["stage_results<br/>per stage, per attempt"]
        MEMB["source_memberships<br/>snapshot lineage"]
    end

    subgraph stgL["stage — verified working copy"]
        T1["bio, gas, hydro,<br/>solar, wind, storage"]
        T2["&lt;source&gt;_properties"]
        T3["&lt;source&gt;_units_properties"]
    end

    subgraph coreL["core — consolidated units (serial keys)"]
        C1["generators<br/>(bio, gas, hydro, solar, wind)"]
        C2["storages"]
        C3["generator_properties<br/>storage_properties"]
        C4["generator_units_properties<br/>storage_units_properties"]
    end

    subgraph martsL["marts — stored wide pivots (ADR 0003)"]
        M1["installation_counts"]
        M2["generation_capacity"]
        M3["storage_capacity"]
    end

    R1 -->|"extract: newest version"| T1
    R2 --> T1
    R3 --> T1
    R4 --> T1
    R5 --> T1
    R6 --> T1
    S2 -.->|"spatial join"| T1
    S1 -.->|"load signature: skip what is loaded"| R1
    T1 --> T2
    T1 --> T3
    T1 -->|"load: snapshot is authoritative"| C1
    T1 --> C2
    C1 --> C3
    C1 --> C4
    C2 --> C3
    C2 --> C4
    C1 -->|"REFRESH MATERIALIZED VIEW"| M1
    C2 --> M1
    C1 --> M2
    C2 --> M2
    C2 --> M3
    RUNS -.->|"run_id"| RESULTS
    RUNS -.->|"run_id"| MEMB
    MEMB -.->|"unit_key → staging unit_id"| T1
```

### 2.2 raw — versioned datalake

One table per **extract**, never overwritten: `raw.<source>_<YYYYMMDD>_<n>`, the
counter restarting each day. Same shape for all six sources plus two extra
columns for storage (`storage_type`, `storage_capacity`); `secondary_attributes`
holds the jsonb of everything not decomposed.

| Column | Type | Notes |
|---|---|---|
| `energy_source` | str | routed from the column, never the filename (`AWS.md` §2) |
| `installed_capacity` | float | kW |
| `commissioning_date` / `decommissioning_date` | date | |
| `storage_type`, `storage_capacity` | str, float | storage only |
| `x_coordinates`, `y_coordinates` | float | WGS-84 |
| `geo_accuracy` | int | 1 = exact, 2 = approximate |
| `geometry` | point | WGS-84, PostGIS |
| `reference_id`, `reference_date` | str, timestamp | identity and as-of date in the source |
| `secondary_attributes` | jsonb | folded attributes (ADR 0006 era: raw carries them inline) |

### 2.3 service — operational metadata

Not part of the datalake; deliberately separate (ADR 0007).

| Table | Key | Written by | Purpose |
|---|---|---|---|
| `loaded_files` | append-only, no unique constraint | extract | load signature `(filename, filesize, modified_at)`; a re-extract is skipped unless `-f` |
| `boundaries` | non-versioned, `level` 0-3 | extract (bootstrap) / boundaries release | `country_iso`, `name`, `level`, `area` km², `geometry` multipolygon, `geojson` (pre-simplified for the app) |
| `ingestion_runs` | `run_id`; unique `(bucket, object_key, object_version_id)` | ingestion | one processing lifecycle: `input_kind` (`source`\|`boundary`), `state` (pending, running, succeeded, retryable, terminal, stale), `attempt_count`, `object_last_modified` |
| `stage_results` | unique `(run_id, attempt, target, stage)` | ingestion | per-stage outcome, `row_count`, `error`, `details` jsonb, for stages processing / extract / transform / load / marts / bootstrap |
| `source_memberships` | `(run_id, unit_key)` | ingestion | which units a snapshot carried, bad-quality ones included — lineage, **not** an active flag |

```mermaid
erDiagram
    INGESTION_RUNS ||--o{ STAGE_RESULTS : "run_id"
    INGESTION_RUNS ||--o{ SOURCE_MEMBERSHIPS : "run_id"
    SOURCE_MEMBERSHIPS }o--|| STAGING : "unit_key"
    STAGING ||--o{ GENERATORS : "reference_id or synthetic identity"
    STAGING ||--o{ STORAGES : "reference_id or synthetic identity"
    GENERATORS ||--o{ GENERATOR_UNITS_PROPERTIES : unit_id
    GENERATOR_UNITS_PROPERTIES }o--|| GENERATOR_PROPERTIES : prop_id
    STORAGES ||--o{ STORAGE_UNITS_PROPERTIES : unit_id
    STORAGE_UNITS_PROPERTIES }o--|| STORAGE_PROPERTIES : prop_id
    BOUNDARIES }o--|| STAGING : "spatial join sets state, region, district"
    BOUNDARIES }o--|| CORE : "spatial join sets state, region, district"
    GENERATORS }o--|| MART_INSTALLATION_COUNTS : "pivot energy_source"
    GENERATORS }o--|| MART_GENERATION_CAPACITY : "pivot energy_source"
    STORAGES }o--|| MART_STORAGE_CAPACITY : "pivot source_type"

    INGESTION_RUNS {
        uuid run_id PK
        str bucket
        str object_key
        str object_version_id
        str input_kind
        str state
        int attempt_count
    }
    STAGE_RESULTS {
        bigserial stage_result_id PK
        uuid run_id FK
        int attempt
        str target
        str stage
        str outcome
        int row_count
    }
    SOURCE_MEMBERSHIPS {
        uuid run_id FK
        str energy_source
        str unit_key FK
        str reference_id
        bool bad_quality
    }
    BOUNDARIES {
        str country_iso
        str name
        int level
        float area
        geometry geometry
        str geojson
    }
```

### 2.4 stage — the verified working copy

`stage.<source>` for the six sources, plus a per-source property dimension and
link table (ADR 0006 splits the dimension by unit kind in core; in staging it is
per source). Identity is the canonical `energy_source` + `reference_id`; a row
without a `reference_id` gets a synthetic hash of source, `location`,
coordinates rounded to six decimals, and `geo_accuracy` (ADR 0001) — capacity
and dates are deliberately **not** part of it, so identity cannot move when a
unit's data changes.

Added on top of raw: `unit_id` (pk), `country_iso`, `state`, `region`,
`district` (from `service.boundaries`), `bad_quality`.

| Table | Key | Purpose |
|---|---|---|
| `bio`, `gas`, `hydro`, `solar`, `wind`, `storage` | `unit_id` | the enriched, quality-gated snapshot |
| `<source>_properties` | `param_id`; unique `(name, value)` | decomposed attributes |
| `<source>_units_properties` | `(unit_id, param_id)` | link |

A staging table present in the database means *verified* — a failed snapshot
transform drops its tables rather than leaving a partial copy for the next load
(issue #4).

### 2.5 core — consolidated units

`core.generators` and `core.storages` carry a serial `unit_id` plus a data-integrity
unique index on `(energy_source, reference_id)`; a complete Source snapshot is
authoritative (ADR 0001):

- **in** the snapshot → `UPDATE` in place, `unit_id` stable, property links refreshed
- **not in** the snapshot → retained untouched, so historical units survive; the
  snapshot's membership ends in `source_memberships` instead
- a boundary release re-derives `state`/`region`/`district` for **every** row, retained
  ones included, straight from each unit's own geometry (issue #5)

Quality annotations live as property links, not flags: `collision` plus
`close_to` naming the neighbour (ADR 0005), with
`generator_properties`/`storage_properties` and their link tables per kind
(ADR 0006).

### 2.6 marts — stored wide pivots

Three `MATERIALIZED VIEW`s at **state** grain, refreshed on demand and verified
(ADR 0003). Only active units contribute, so all three agree by construction;
`state IS NULL` collapses into the `outside` bucket.

| View | Pivot | Value |
|---|---|---|
| `marts.installation_counts` | `energy_source` | `COUNT(*)` over active generators ∪ storages |
| `marts.generation_capacity` | `energy_source` | `SUM(installed_capacity)` |
| `marts.storage_capacity` | `source_type` | `SUM(storage_capacity)` |

The stored shape cannot answer date-range or decommissioned queries, and the app
does not read it: the viz header aggregates from `core` with its own active-unit
predicate so the map and the header always resolve the same set (issue #24). The
marts are the analyst/reporting surface, and they are what the drift checks read:
`marts.verify_marts` reconciles each stored pivot against current core, and
`scripts/smoke_etl_container.sh` asserts the three matviews exist at state grain
including the `outside` bucket.

---

## 3. Lineage: one unit, end to end

```mermaid
sequenceDiagram
    autonumber
    participant S3 as S3 object version
    participant ING as ingestion
    participant RAW as raw
    participant STG as stage
    participant CORE as core
    participant MART as marts

    S3->>ING: sources/wind.gpkg @ versionId
    ING->>ING: ingestion_runs (run_id, input_kind=source)
    ING->>RAW: extract → new version table
    ING->>ING: source_memberships per snapshot row (service)
    ING->>STG: transform → unit_id, state/region/district, properties
    ING->>CORE: load → serial unit_id, collisions
    ING->>MART: marts → one refresh per message
    ING->>ING: stage_results per stage
    Note over ING,S3: message deleted only when every record settled
```

| Hop | Key that carries the row | Where the key is written |
|---|---|---|
| file → run | `(bucket, object_key, object_version_id)` | `service.ingestion_runs` — immutable input identity, also the idempotency guard |
| run → raw | `<source>_<YYYYMMDD>_<n>` + row order | `raw`, append-only; the version table name is recorded in the load log |
| raw → stage | `unit_id` = canonical `energy_source` + `reference_id`, or the synthetic hash | `stage.<source>`, mirrored in `source_memberships.unit_key` |
| stage → core | `reference_id` when present, else the recomputed synthetic hash; `unit_id` becomes a serial integer | `core.generators` / `core.storages` |
| core → marts | `state` grain (with the `outside` bucket) | the three materialized views |
| core → app | `unit_id` (not exposed) + `energy_source`, coordinates, capacity | read by `viz_reader` |

**Geography is a second, independent lineage**: `boundaries/level-<n>.gpkg` →
`service.boundaries` (one atomic release transaction per message, issue #5) →
every staging Source and every Core row → collisions → marts. A unit's state is
its geography, so a boundary release re-derives units that no snapshot touched.

```mermaid
flowchart LR
    B1["boundaries/level-0.gpkg<br/>country outline"] --> REL["release<br/>one transaction,<br/>levels published = the levels replaced"]
    B2["boundaries/level-1.gpkg<br/>states + EEZ"] --> REL
    B3["boundaries/level-2.gpkg<br/>regions"] --> REL
    B4["boundaries/level-3.gpkg<br/>districts"] --> REL
    REL --> SVCB[("service.boundaries")]
    SVCB -->|"enrich every staging Source"| STG2[("stage.*")]
    SVCB -->|"re-derive every Core row<br/>incl. retained historical"| CORE2[("core.*")]
    STG2 --> COL["collision detection"]
    CORE2 --> COL
    COL --> MART2[("marts.*")]
```

---

## 4. AWS integration

### 4.1 Event path and bucket layout

```mermaid
flowchart TB
    subgraph bucket["S3 datalake bucket<br/>(the keys are the contract)"]
        K1["sources/bio.gpkg<br/>sources/gas.gpkg<br/>sources/hydro.gpkg<br/>sources/solar.gpkg<br/>sources/wind.gpkg<br/>sources/storage.gpkg"]
        K2["boundaries/level-0.gpkg<br/>… level-3.gpkg"]
    end
    K1 -->|"accepted key"| EV["s3:ObjectCreated"]
    K2 --> EV
    EV --> Q[["SQS queue"]]
    Q --> DLQ[["DLQ"]]
    DLQ --> ALARM["CloudWatch alarm"]
    ALARM --> NOTIFY["SNS → email"]
    Q --> POLL["worker receives one message<br/>= the ingestion unit"]
    POLL --> VIS["visibility extended to 6h,<br/>re-extended every minute"]
    POLL --> RUN["resolve the served version<br/>(head_object)"]
    RUN -->|"older than a succeeded run"| STALE["stale → settled, not retried"]
    RUN --> READ["GetObject(version_id) → temp GPKG,<br/>unlinked after inspect"]
    READ --> LEDGER["service.ingestion_runs"]
    POLL --> DEL["DeleteMessage when every record<br/>is succeeded / terminal / stale"]
    READ -->|"content validation fails"| REJ["terminal → one SNS alert:<br/>upload a new version"]
    READ -->|"infrastructure fails,<br/>5th delivery"| EXH["retryable + exhausted →<br/>visibility 0, redrive to the DLQ"]
    REJ --> NOTIFY
    NOTIFY -.-> LOGS["CloudWatch Logs"]
    POLL -.-> LOGS
```

The event wiring above is Terraform (`terraform/s3.tf`, `terraform/sqs.tf`,
`terraform/cloudwatch.tf`): the S3 notification onto the queue, the redrive
policy with `maxReceiveCount = 5`, and the DLQ alarm are declared in this repo
and applied to the account (ADR 0010). The delivery contract those resources
carry is pinned against `etl/ingestion.py` by `tests/test_terraform_config.py`.

What the pipeline container runs is `python -m etl startup` and then the worker
loop: the startup applies the current Boundary releases, enqueues the Source
object versions that have no run yet, and refuses to start the worker when a
Boundary level is missing (ADR 0009). A rejected object version is announced by
the worker; a message that runs out of deliveries is announced by the DLQ alarm,
so each problem is alerted once. The receive loop is verified end to end, startup
through redrive, by `tests/test_acceptance_workflow.py`
([§6](#6-what-is-left-to-the-account) lists what is left to the account).

The bucket key is the contract: `sources/<source>.gpkg` and
`boundaries/level-<n>.gpkg` are the only accepted keys, and the key alone
decides `input_kind` — never the file's content or name. The `energy_source` that
routes a row is read from the column, not the filename (`AWS.md` §2).

Versioned objects make the input identity immutable, which is what lets a
redelivery be recognised: an object whose `LastModified` is older than a
succeeded run settles as `stale` instead of being re-processed, and a version
already in `loaded_files` is not extracted twice.

The same immutability decides the retry contract. A *rejected object version* —
one that fails its own content validation, in the inspect step or in the
transform that follows it — will fail identically on every delivery, so its run
is `terminal` and it is alerted once. A database, S3 or marts failure is a
property of the moment, so the run stays `retryable` and the delivery is repeated
until SQS's `maxReceiveCount = 5` moves the message to the DLQ. The worker's
attempt number is SQS's `ApproximateReceiveCount`, so a crashed worker's message
is counted, not restarted for free. A message the worker keeps is handed straight
back to the queue when the work is over, so the redrive happens then and not six
hours later.

### 4.2 IAM and observability

The worker's EC2 role needs `s3:GetObject` and `s3:GetObjectVersion` on the fixed accepted
keys, `sqs:ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`, `SendMessage`,
`sns:Publish`, and `logs:CreateLogStream` / `logs:PutLogEvents` (`AWS.md` §5, §3). It is
not granted `s3:ListBucket` or `sqs:GetQueueAttributes`: nothing in the worker lists the
bucket or inspects the queue — the delivery count arrives as a message attribute on the
receive call. A rejected object version publishes an SNS alert; a message that runs out of
deliveries is dead-lettered and a CloudWatch alarm on the DLQ pages the same topic (§4).

### 4.3 Container stack

```mermaid
flowchart LR
    subgraph host["Server / dev machine — Docker Compose"]
        NG["nginx :80<br/>only published surface"]
        VIZ["viz container<br/>Streamlit + PyDeck :8501<br/>image khvostenko/aws-energy-viz"]
        PIPE["pipeline container<br/>python -m etl run-all<br/>image khvostenko/aws-energy-etl"]
        DB[("db container<br/>PostGIS 16 + PostGIS 3.4<br/>volume db_data")]
        VOL[("etl_data volume<br/>raw GPKGs + docker.env<br/>(seeded once)")]
    end
    BROWSER["browser"] --> NG
    NG --> VIZ
    VIZ -->|"VIZ_DATABASE_URL<br/>read-only viz_reader"| DB
    PIPE -->|"DATABASE_URL (owner role)"| DB
    VOL --> PIPE
    VOL --> VIZ
    DB -.->|"docker/viz_reader.sql on a fresh volume"| RD[["viz_reader<br/>SELECT only"]]
```

- The **pipeline** role owns the schemas; the **viz** app connects as
  `viz_reader`, which never receives `INSERT/UPDATE/DELETE` and whose grants
  survive the pipeline's table drops and mart refreshes.
- The detached `etl_data` volume carries the private raw data and
  `DATABASE_URL` / `VIZ_DATABASE_URL`; nothing data-like is baked into an image
  or into the compose file (`scripts/seed_data_volume.sh`).
- `compose.yaml` (server) publishes only nginx; `local_compose.yaml` publishes
  the app directly at `:8501`; `build_compose.yaml` builds both images from this
  repo instead of pulling.

### 4.4 CI/CD

```mermaid
flowchart LR
    MAIN["push to main"] --> PUB["publish-docker.yml"]
    PUB --> HUB[("Docker Hub<br/>khvostenko/aws-energy-etl<br/>khvostenko/aws-energy-viz")]
    HUB --> COMPOSE["docker compose pull"]
    PR["any push / PR"] --> CI["ci.yml — four cheap gates"]
    CI --> CC["compileall"]
    CI --> IB["both image builds"]
    CI --> TV["terraform fmt + validate<br/>+ test_terraform_config.py"]
    CI --> KCF["compose config render<br/>+ test_compose_config.py"]
```

Every gate needs no PostGIS, no raw data and no AWS credentials, so a renamed
CLI command or a changed port fails a pull request instead of a deployment:
byte-compile, both image builds, `terraform fmt -check` / `init -backend=false` /
`validate` plus `tests/test_terraform_config.py` (which pins the accepted keys and
the delivery contract to `etl/ingestion.py`), and `docker compose config` on all
three variants plus `tests/test_compose_config.py` — the container contract that
used to be reachable only through `scripts/smoke_etl_container.sh`, which needs
the private data set (ADR 0011).

The full integration suite needs a PostGIS service, so it runs locally
(`TEST_DATABASE_URL` against a scratch database) rather than in CI. The acceptance
walkthrough belongs to that suite, not to CI: it drives `startup`, `worker` and
`redrive` through the real processor against a real PostGIS with fake AWS
adapters, which is exactly the heavy flow CI has no service for.

---

## 5. Discrepancies

Files win over `TechnicalSpecification.md`; these are the ones that affect this
picture:

- The published files are `V20260203`, not the `V20250101` the spec names, and
  the gas file is `Gas_Producer_V20260203.gpkg`.
- The staging schema is `stage` by default; the spec calls the layer "Staging".
- The spec's Raw tables list `bio, gas, hydro, solar, wind`; storage was added
  as a sixth Source, so raw, stage and core are all six-per-kind.
- `service` holds the boundary reference layer, not `raw.boundaries` (ADR 0007
  superseded that part of ADR 0004).

## 6. What is left to the account

The event wiring, the infrastructure and the operator startup all exist in this
repo (ADR 0009, ADR 0010, ADR 0011); what no code here can do is put them in the
account:

- **The apply, and the two steps the provider cannot take.** Attaching the
  instance profile to the running host, and installing the rendered CloudWatch
  agent config, are one CLI call and one file each (`terraform/README.md`). The
  SNS email subscription stays silent until the operator clicks the
  confirmation.
- **The data itself.** The bucket is empty until an upload happens: six Source
  snapshots and four Boundary levels under the ten accepted keys, with object
  versioning on. Until then startup has nothing to enqueue, no message reaches
  the worker, and every layer above `service` stays empty.
- **The host.** Terraform creates no EC2 instance, no VPC and no RDS — the
  Compose stack and its PostGIS run on a host the account already has.

One thing is absent by design rather than by omission: the admin and API surfaces
in the spec's Serving section do not exist. `AWS.md` §7 lists the rest of the
non-goals.
