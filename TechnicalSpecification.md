# Project. Renewable Energy Installations in Germany

## Project objectives
1. Visualization of geodata showing the locations of renewable energy generation facilities in Germany. 
2. ETL pipeline that integrates geospatial data on renewable energy installations in Germany 
with administrative and maritime boundaries. The data is enriched using spatial joins and stored in PostGIS.
3. Interactive dashboard in Metabase to analyze capacity, energy types, and regional distribution.
Aggregation total installed capacity by state (Bundesland), region (Regierungsbezirk), and district (Landkreis), 
by source type (Bio, Water, Solar, Wind, Gas), by date of commissioning

## Data description
### Energy installations. Source https://zenodo.org/records/20716459  
| Dataset              | Description                                              | Filename                             | Type         |
|----------------------|----------------------------------------------------------|--------------------------------------|--------------|
| Bioenergy            | Locations of power generation units of bioenergy systems | Bioenergy_V20250101.gpkg             | Point        |
| Cogeneration Units   | Locations of cogeneration units (not using for now)      | Cogeneration_Units_V20250101.gpkg    | Point        |
| Energy Storage       | Locations of energy storage units greater than 100 kW    | Energy_Storage_V20250101.gpkg        | Point        |
| Gas Production       | Location of gas production systems                       | Gas_Producer_V20250101.gpkg          | Point        |
| Hydropower           | Locations of hydropower systems                          | Hydropower_V20250101.gpkg            | Point        |
| Solarenergy Polygons | Polygons of solar energy systems (not using for now)     | Solar_Energy_Polygons_V20250101.gpkg | Multipolygon |
| Solar Energy         | Locations of solar energy systems                        | Solar_Energy_V20250101.gpkg          | Point        |
| Wind Energy          | Locations of wind turbines                               | Wind_Energy_V20250101.gpkg           | Point        |

### Germany administrative boundaries (incl. Offshore zones). Source https://gdz.bkg.bund.de/ https://www.quickmaptools.com https://www.marineregions.org
| Dataset        | Description                                         | Filename               | Type         |
|----------------|-----------------------------------------------------|------------------------|--------------|
| State borders  | Germany state outline                               | germany_boundary.gpkg  | Multipolygon |
| States + EEZ   | States boundaries + Germany EEZ (Bundesland)        | germany_states.gpkg    | Multipolygon |
| Regions        | Regions boundaries (Regierungsbezirk)               | germany_regions.gpkg   | Multipolygon |
| Districts      | Districts boundaries (Landkreis + Kreisfreie Stadt) | germany_districts.gpkg | Multipolygon |

## ETL-pipeline description
### 1. Extract
- Input: the raw unit-source GPKG files, each routed by the Energy source value in its content
- Validate before writing: exactly one layer, the required schema and geometry, a non-null homogeneous Energy source, and unique non-null Reference IDs. A snapshot that fails any check is rejected whole and leaves the previous state intact
- Check if the file was already loaded. If attribute force (-f) - load else skip
- Read files to geopandas dataframes.
- Type casting
- Convert secondary properties to dictionary, place it to column 'secondary_attributes'
- Save to PostGIS datalake. Table names should contain source type, date of load, number of load
#### Load administrative and maritime boundaries
- Load .gpkg files with boundaries into the non-versioned `service.boundaries` table (see Data layers — Service)
#### Boundary release (event-driven, issue #5)
- A Boundary object is published at its fixed key `boundaries/level-<n>.gpkg`. Before any write it must have exactly one layer, the columns `name`, `iso`, `level`, rows whose `level` all equal the key's level, non-null names, and valid non-null Polygon/MultiPolygon geometry in EPSG:4326; level 0 holds exactly one country outline
- All Boundary objects of one message form one release, applied before its Source records. One invalid level rejects the whole release
- In one transaction: delete only the published levels' rows, insert the new rows, recompute their area and viz GeoJSON, verify the whole layer (levels 0–3 present, one level-0 row, positive area, valid geometry), and write each object version to `loaded_files`. Any failure rolls back, so no partial release is ever visible. A release whose object versions are all in `loaded_files` is not applied again
- Then rebuild geography once: re-enrich every staging Source and every Core unit of both kinds (including retained historical units) from its own geometry, recompute the state-dependent collisions, and refresh and verify the three marts
- Spatial-join tie-break: a point on a shared border takes the alphabetically first polygon name, in both the transform and the rebuild
### 2. Transform
- Input: list of tables to be transformed
- Add primary keys
- Enrich tables with columns 'state', 'region', 'district' (spatial join with Boundaries)
- Decompose some properties to normalized table **properties** (other properties stay in 'secondary_attribures').
Properties to decompose:
  - biomass_type
  - fuel_type
  - technology
  - reference_source
  - solar_type
  - note
  - location
  - alignment
  - inclination
  - hydro_type
  - inflow_type
  - manufacturer
  - rotor_diameter
  - hub_height
- Quality check. Flag bad records, add property 'bad_record' in which append failed tests description:
  1. installed_capacity <=0 or null
  2. decommissioning_date <= commissioning_date
  3. x_coordinates, y_coordinates and geometry do not match
- Save tables to PostGIS
- A Source snapshot whose transform fails is removed from staging, because a snapshot load reads every staging table of its Core kind: a present staging table always means the Source's last transform was verified, and the failed Source's Core units stay retained until a good snapshot arrives
### 3. Load
#### First load
- Input: list of tables to be loaded
- Create tables **generators**, **storages**
- Insert into **generators** data from Units tables bio, gas, hydro, solar, wind (only good records)
- Insert into **storages** data from Units table storage (only good records)
- Transfer primary keys for dimension tables
- Create indexes
- Quality check. Flag collisions, add property 'collision' with collisions description, 
add property 'close_to' with reference to close unit
  1. close location. Distance between units < 10m (only if geo_accuracy=1)
  2. outside location (state is null)
  2. onshore unit in the sea
  3. storage_capacity <=0 or null (for storages)

#### Incremental load 
Each Unit table carries a complete Source snapshot and is therefore authoritative for the rows it holds, so no freshness gate is applied. `reference_date` is row provenance, not an ordering rule (issue #36, `CONTEXT.md`, ADR 0001).
- Input: list of tables to be loaded
- Append-or-update **generators** table with records from Units tables, matching on record identity (`reference_id`, or the synthetic identity for rows without one) and updating every matched row
- Append-or-update **storages** table with records from Units table storage, on the same match
- Retain Core units that the snapshot omits, so historical units survive
- Transfer primary keys for dimension tables
- Quality check
- Verify every good snapshot row reached Core unchanged: Energy source, Reference ID, `installed_capacity` and `reference_date` for both kinds, plus `storage_type` and `storage_capacity` for **storages**; a bad-quality row must not have created a Core row

### Creating Materialized Views
- Number of units pivot table (state / energy_source)
- Generation capacity pivot table (state / energy_source)
- Storage capacity pivot table (state / source_type)

### Event-driven worker (issue #6)
The event-driven path runs the stages above per uploaded object instead of by hand. One SQS message is the worker's unit of work and is never combined with another message: it is received alone, every S3 record it carries is decoded, and those records are processed in one fixed pass.
- Receive one message at a time (SQS long polling; an empty receive pauses the worker, it does not stop it)
- Order within a message: all Boundary records first, as a single release, then the Source records in the order the queue delivered them. The order matters because Source rows are enriched with the Boundary geography that has to be current first
- Independence: each record is its own Ingestion run, with its own attempts, stage results, and terminal error. One failing record never keeps the other records of the same message from progressing, and each Source record uses exactly the raw table(s) that record produced
- Downstream rebuild once per message: after the last record the geography is rebuilt and the three marts are refreshed and verified once, not once per record
- Acknowledgement: the message is deleted from the queue only when every record is successful, terminally skipped, or stale. A retryable record keeps the message, and redelivery repeats only the records that are not settled yet — a record that already succeeded is not applied again
- The Boundary release rules are described under Extract

### Retries, rejection, DLQ, and alerting (issue #7)
The worker separates the failures that can never succeed from the ones that can, because only one of the two is worth another delivery.

- **Rejected object version (terminal):** a failure that is a property of the immutable bytes, not of the moment they were processed. That is the object's own content validation failing — an unreadable or non-homogeneous Source GPKG, a snapshot the inspect step rejects, a row the transform cannot give an identity to, a Boundary level that does not match its key. Validating the same version again cannot answer differently, so the run becomes `terminal`, the run's error is stored in `terminal_error`, and the stage that rejected it records a failed stage result next to the stages that passed. The version is not retried: the operator fixes the file by uploading a **new version**, which is a new Ingestion run. A Boundary release is the one batch that is all-or-nothing, so when one level of it is rejected every level of that release settles with it: the levels are only valid as a set, and a partly-applied geography is not a state the pipeline has
- **Retryable:** everything else — the database, S3, the queue, the marts refresh, and any other infrastructure error. These are properties of the environment at that moment, and are retried on the next delivery
- **Alerting (direct, once):** a terminal rejection publishes one SNS alert naming the key, the version, the error, and how to recover (upload a new version). The alert is tied to the run, and the run is recorded terminal *before* the alert is sent, so a redelivery of the same message never repeats it. A failed alert is logged and does not hold up the rest of the message
- **No direct alert for exhaustion:** a message whose records are still retryable after the queue's delivery limit is not announced by the worker; it is dead-lettered, and the DLQ alarm publishes that. Infrastructure problems are therefore reported once, by one path
- **Delivery limit:** the queue is configured with `maxReceiveCount = 5` to match `MAX_DELIVERY_ATTEMPTS`. The worker's attempt number comes from SQS's own `ApproximateReceiveCount`, not from a counter of its own, so a redelivery after a worker crash counts as a delivery too. On the fifth delivery the run stays `retryable` (it is never deleted), its `terminal_error` records the cause followed by the exhaustion, and the message is left on the queue for the redrive policy to move. `retryable` rather than `terminal` is deliberate: the run has to be resumable, so an operator who redrives the message from the DLQ after fixing the cause continues the same run instead of finding it settled and ignored
- **Visibility:** while a message is being processed the worker extends its visibility timeout to the six-hour SQS maximum, and re-extends it every minute for as long as the work runs, so a long ingest is not handed to a second worker. When the work is over the worker hands the message back (`VisibilityTimeout: 0`) unless it acknowledged it, because SQS decides on redelivery and on the redrive to the DLQ when a message becomes visible — a kept message must not sit behind the six hours it was just granted. A worker that is killed outright cannot hand anything back, so its message returns when the last timeout lapses: the run is `running` with a recorded attempt, and the redelivery starts the next attempt, which is what makes the work resumable
- **Provisioning:** the bucket, queue + DLQ, topic, instance profile, log groups, metrics and alarms are Terraform (`terraform/`, issue #10). The delivery contract above is deliberately a `local` there rather than a variable, and `tests/test_terraform_config.py` pins it against `VISIBILITY_TIMEOUT_SECONDS` and `MAX_DELIVERY_ATTEMPTS` so the queue cannot be tuned past what the worker survives; see `docs/adr/0010-terraform-deployment-contract.md`

## Serving
### 1. Visualization
- Dashboard with map
- Checkboxes to show units of different type (Bio, Hydro, Wind, Solar, Gas, Storage)
- Multiselector to include different states, regions, districts
- Selectbox to choose timescope 
- Header showing the total installed capacity according to the selected unit types and timescope
<br>
For more details see VisualizationSpec.md

### 2. Admin panel (auth)
- Correcting records (CRUD)
- Resolving collisions

### 3. API (auth)
- Quering records from BD (Exel file)
- Quering aggregated data from BD (JSON)

## Data layers
### 1. Raw
#### Units tables: bio, gas, hydro, solar, wind
| Column               | Data type | Description                                       |
|----------------------|-----------|---------------------------------------------------|
| energy_source        | str       | Type of unit (bio, gas, hydro, solar, wind)       |
| installed_capacity   | float     | Kilowatt (kW)                                     |
| commissioning_date   | date      | Commissioning date of the system                  |
| decommissioning_date | date      | Decommissioning date of the system                |
| x_coordinates        | float     | Longitude WGS-84                                  |
| y_coordinates        | float     | Latitude WGS-84                                   |
| geo_accuracy         | int       | 1/2                                               |
| reference_id         | str       | Reference id of the record in the original source |
| reference_date       | timestamp | Timestamp of the record in the original source    |
| geometry             | point     | WGS-84                                            |
| secondary_attributes | text      | Dictionary of secondary attributes                |

#### Units table: storage
| Column                | Data type | Description                                        |
|-----------------------|-----------|----------------------------------------------------|
| energy_source         | str       | Type of unit (storage)                             |
| installed_capacity    | float     | Kilowatt (kW)                                      |
| commissioning_date    | date      | Commissioning date of the system                   |
| decommissioning_date  | date      | Decommissioning date of the system                 |
| storage_type          | str       | Type of energy storage system                      |
| storage_capacity      | float     | Usable energy storage capacity Kilowatt-hour (kWh) |
| x_coordinates         | float     | Longitude WGS-84                                   |
| y_coordinates         | float     | Latitude WGS-84                                    |
| geo_accuracy          | int       | 1/2                                                |
| reference_id          | str       | Reference id of the record in the original source  |
| reference_date        | timestamp | Timestamp of the record in the original source     |
| geometry              | point     | WGS-84                                             |
| secondary_attributes  | text      | Dictionary of secondary attributes                 |

### 2. Service (operational metadata)
Non-versioned operational metadata, deliberately separate from the versioned raw datalake (ADR 0007).

#### loaded_files - list of datafiles loaded into the Raw layer (load-signature log)
| Column      | Data type |
|-------------|-----------|
| filename    | str       |
| filesize    | int       |
| modified_at | timestamp |
| loaded_at   | timestamp |
| loaded_to   | str       |

#### boundaries - administrative and maritime reference polygons
| Column            | Data type    | Description                                                              |
|-------------------|--------------|--------------------------------------------------------------------------|
| country_iso       | str          | DEU                                                                      |
| name              | str          | name of area                                                             |
| level             | int          | 0 - country, 1 - states + EEZ, 2 - regions, 3 - districts                |
| area              | float        | km2                                                                      |
| geometry          | multipolygon | WGS-84                                                                   |
| geojson           | text         | geometry pre-simplified to `ST_AsGeoJSON(ST_SimplifyPreserveTopology(geometry, 0.001))` at load, served by the viz app (issue #31) |

#### ingestion_runs - one processing lifecycle per accepted S3 object version
Keyed by the immutable input identity `(bucket, object_key, object_version_id)`. Local CLI
runs are tracked by the `loaded_files` load signature instead.

| Column               | Data type    | Description                                                                    |
|----------------------|--------------|--------------------------------------------------------------------------------|
| run_id               | uuid         | pk                                                                            |
| bucket               | str          | S3 bucket of the input                                                        |
| object_key           | str          | S3 key of the input                                                           |
| object_version_id    | str          | immutable S3 version; unique with bucket and key                              |
| input_kind           | str          | `source` or `boundary`, resolved from the accepted key                        |
| object_last_modified | timestamp    | `LastModified` S3 served for the processed version; rejects delayed versions  |
| state                | str          | pending, running, succeeded, retryable, terminal, stale                       |
| current_stage        | str          | stage reached, or the stage that failed                                       |
| attempt_count        | int          | processing attempts so far                                                    |
| terminal_error       | text         | error text for a failed or stale run                                          |
| created_at           | timestamp    | when the run row was created                                                  |
| started_at           | timestamp    | when processing started                                                       |
| finished_at          | timestamp    | when the run reached a settled state                                          |

#### stage_results - per-stage outcome for each Ingestion run
Unique per `(run_id, attempt, target, stage)`.

| Column           | Data type | Description                                                     |
|------------------|-----------|-----------------------------------------------------------------|
| stage_result_id  | bigserial | pk                                                             |
| run_id           | uuid      | the Ingestion run this result belongs to                        |
| attempt          | int       | the processing attempt the result was recorded for              |
| target           | str       | table or object identity the stage acted on                    |
| stage            | str       | processing, extract, transform, load, marts, or bootstrap       |
| outcome          | str       | succeeded or failed                                            |
| row_count        | int       | rows the stage produced, where available                        |
| error            | text      | error text for a failed stage                                   |
| details          | jsonb     | stage-specific detail (inserted/updated/retained, refreshed, …) |
| started_at       | timestamp | when the stage started                                          |
| finished_at      | timestamp | when the stage finished                                         |

#### source_memberships - Source lineage per accepted Source snapshot
One row per unit the snapshot contains, including units marked Bad quality. Membership
is lineage metadata, not an Active flag: a newer snapshot can end a membership without
deleting or deactivating the retained Core unit.

| Column         | Data type | Description                                                         |
|----------------|-----------|---------------------------------------------------------------------|
| run_id         | uuid      | the Ingestion run of the snapshot that carried the row               |
| energy_source  | str       | canonical Energy source the row was routed to                        |
| unit_key       | str       | staging `unit_id` of the row, the key staging and membership join on |
| reference_id   | str       | the row's Reference ID, when it has one                              |
| bad_quality    | bool      | the row's bad-quality flag                                           |
| recorded_at    | timestamp | when the membership was recorded                                     |


### 3. Staging
#### Units tables: bio, gas, hydro, solar, wind
| Column               | Data type | Description                                       |
|----------------------|-----------|---------------------------------------------------|
| unit_id              | str       | pk                                                |
| energy_source        | str       | Type of unit (bio, gas, hydro, solar, wind)       |
| installed_capacity   | float     | Kilowatt (kW)                                     |
| commissioning_date   | date      | Commissioning date of the system                  |
| decommissioning_date | date      | Decommissioning date of the system                |
| geometry             | point     | WGS-84                                            |
| x_coordinates        | float     | Longitude WGS-84                                  |
| y_coordinates        | float     | Latitude WGS-84                                   |
| geo_accuracy         | int       | 1/2                                               |
| reference_date       | timestamp | Timestamp of the record in the original source    |
| reference_id         | str       | Reference id of the record in the original source |
| secondary_attributes | text      | Dictionary of secondary attributes                |
| country_iso          | str       | DEU                                               |
| state                | str       | Bundesland / Sea                                  |
| region               | str       | Regierungsbezirk                                  |
| district             | str       | Landkreis                                         |
| bad_quality          | bool      | Flag bad quality record                           |

#### Units table: storage
| Column               | Data type | Description                                        |
|----------------------|-----------|----------------------------------------------------|
| unit_id              | str       | pk                                                 |
| storage_type         | str       | Type of energy storage system                      |
| storage_capacity     | float     | Usable energy storage capacity Kilowatt-hour (kWh) |
| installed_capacity   | float     | Kilowatt (kW)                                      |
| commissioning_date   | date      | Commissioning date of the system                   |
| decommissioning_date | date      | Decommissioning date of the system                 |
| geometry             | point     | WGS-84                                             |
| x_coordinates        | float     | Longitude WGS-84                                   |
| y_coordinates        | float     | Latitude WGS-84                                    |
| geo_accuracy         | int       | 1/2                                                |
| reference_id         | str       | Reference id of the record in the original source  |
| reference_date       | timestamp | Timestamp of the record in the original source     |
| secondary_attributes | text      | Dictionary of secondary attributes                 |
| country_iso          | str       | DEU                                                |
| state                | str       | Bundesland / Sea                                   |
| region               | str       | Regierungsbezirk                                   |
| district             | str       | Landkreis                                          |
| bad_quality          | bool      | Flag bad quality record                            |

### Dimension tables (normalized, one per-kind set for each staging Unit table)
#### `{source}_properties` (e.g. `bio_properties`, `storage_properties`)
| Column        | Data type |
|---------------|-----------|
| param_id      | int pk    |
| name          | str       |
| value         | str       |
| (name, value) | unique    |
#### `{source}_units_properties` (links the staging unit to its properties)
| Column              | Data type |
|---------------------|-----------|
| unit_id             | int fk    |
| param_id            | int fk    |
| (unit_id, param_id) | pk        |

## 4. Core
### generators
| Column               | Data type | Description                                       |
|----------------------|-----------|---------------------------------------------------|
| unit_id              | int       | pk                                                |
| energy_source        | str       | Type of unit (Bio, Gas, Hydro, Solar, Wind)       |
| installed_capacity   | float     | Kilowatt (kW)                                     |
| commissioning_date   | date      | Commissioning date of the system                  |
| decommissioning_date | date      | Decommissioning date of the system                |
| geometry             | point     | WGS-84                                            |
| longitude            | float     | Longitude WGS-84                                  |
| latitude             | float     | Latitude WGS-84                                   |
| geo_accuracy         | int       | 1/2                                               |
| reference_date       | timestamp | Timestamp of the record in the original source    |
| reference_id         | str       | Reference id of the record in the original source |
| secondary_attributes | text      | Dictionary of secondary attributes                |
| country_iso          | str       | DEU                                               |
| state                | str       | Bundesland / Sea                                  |
| region               | str       | Regierungsbezirk                                  |
| district             | str       | Landkreis                                         |
| collision            | bool      | Flag collisions                                   |

#### storages
| Column               | Data type | Description                                        |
|----------------------|-----------|----------------------------------------------------|
| unit_id              | int       | pk                                                 |
| storage_type         | str       | Type of energy storage system                      |
| storage_capacity     | float     | Usable energy storage capacity Kilowatt-hour (kWh) |
| installed_capacity   | float     | Kilowatt (kW)                                      |
| commissioning_date   | date      | Commissioning date of the system                   |
| decommissioning_date | date      | Decommissioning date of the system                 |
| geometry             | point     | WGS-84                                             |
| longitude            | float     | Longitude WGS-84                                   |
| latitude             | float     | Latitude WGS-84                                    |
| geo_accuracy         | int       | 1/2                                                |
| reference_id         | str       | Reference id of the record in the original source  |
| reference_date       | timestamp | Timestamp of the record in the original source     |
| secondary_attributes | text      | Dictionary of secondary attributes                 |
| country_iso          | str       | DEU                                                |
| state                | str       | Bundesland / Sea                                   |
| region               | str       | Regierungsbezirk                                   |
| district             | str       | Landkreis                                          |
| collision            | bool      | Flag collisions                                    |

### Dimension tables (per unit-kind, ADR 0006)
#### `generator_properties` / `storage_properties`
| Column        | Data type |
|---------------|-----------|
| prop_id       | int pk    |
| name          | str       |
| value         | str       |
| (name, value) | unique    |
#### `generator_units_properties` / `storage_units_properties` (links unit_id → unit table, prop_id → properties)
| Column              | Data type |
|---------------------|-----------|
| unit_id             | int fk    |
| prop_id             | int fk    |
| (unit_id, prop_id)  | pk        |

## 5. Marts
- **Number of installations** pivot table (region / energy source)
- **Generation capacity pivot** table (region / energy source)
- **Storage capacity** pivot table (region / energy source)

## Tools
1. Data Processing: Python (Pandas, GeoPandas)
2. Database: PostgreSQL + PostGIS 
3. Orchestration: Python scripts / Airflow (optional)
4. Visualization: Metabase _(superseded by the Streamlit + PyDeck app, issue #28)_
5. Infrastructure: Docker, GitHub Actions

## Implementation
1. ETL pipeline using CLI interface
2. DB design using schemas
3. Logging with time control
4. For Staging layer using reference_id to create unit_id. If not present create hash-key
5. For Core layer using serial primary key