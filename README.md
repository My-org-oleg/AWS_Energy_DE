# Renewable Energy Installations in Germany

Geospatial registry of renewable energy installations in Germany, with an ETL pipeline
(GeoPandas → PostGIS marts) for analyzing installed capacity by energy source, state,
and commissioning date, and a Streamlit + PyDeck map app that visualises the marts.

## Status

Implemented. The ETL pipeline (extract → staging → core → marts) runs end-to-end via the CLI
(`python -m etl <stage>`, or `run-all` for the whole pass; Click hyphenates the `run_all`
Python function name) and is covered by a full integration
test suite (pytest, 535 tests against a dedicated scratch PostGIS database), and the
Streamlit viz app
(`viz/`, T1–T4, no-auth map with per-source icon layers (IconLayer), area choropleth and header
aggregates) runs in the containerized stack. Design work recorded in:

- **Spec** — `TechnicalSpecification.md` (authoritative source of truth for data models and stages)
- **Domain glossary** — `CONTEXT.md`
- **Architecture decisions** — `docs/adr/`
- **Tickets** — GitHub issues in this repo, labelled `ready-for-agent`, with blocking edges wired
  (`#2` onwards; the spec lives in `#1`)

## Stack

- Python (Pandas, GeoPandas)
- PostgreSQL + PostGIS (`energy_de` database)
- Streamlit + PyDeck (`viz/` — the deployed visualization app, reads the marts through a
  read-only `viz_reader` role)
- Docker (containerized stack — `compose.yaml`: db + pipeline + viz images pulled from
  Docker Hub, data-volume seed, read-only role provisioning; `docs/containerization.md`)
- Terraform (`terraform/` — the AWS infrastructure the event-driven deployment runs on:
  S3 bucket, SQS queue + DLQ, SNS topic, EC2 instance profile, CloudWatch logs/metrics/
  alarms, #10; operator steps in `terraform/README.md`, decisions in
  `docs/adr/0010-terraform-deployment-contract.md`)
- GitHub Actions (`.github/workflows/ci.yml`: byte-compile + pipeline/viz image build +
  `terraform fmt`/`validate` + the Terraform config tests, and `docker compose config` +
  the Compose contract tests, on every push/PR, #14/#10/#36; decisions in
  `docs/adr/0011-layered-verification-contract.md`;
  `.github/workflows/publish-docker.yml`:
  pushes both images to Docker Hub as `khvostenko/aws-energy-etl` /
  `khvostenko/aws-energy-viz` on `main`)

## Data

| What                                                                 | Where                                     |
|----------------------------------------------------------------------|-------------------------------------------|
| Raw unit GPKG files (6 sources incl. solar, wind, storage)           | `data/sources/*.gpkg`                     |
| Germany boundaries (states + EEZ, regions, districts)                | `data/boundaries/germany_*.gpkg`          |
| Source documentation                                                 | `data/sources/data_descriptor_V20260203.xlsx` |

All files are 2026-02-03 versions. The files are authoritative.

## Pipeline

Four PostGIS layers, run per-stage or as one pass via the CLI (`python -m etl <stage>`):

1. **Extract** — read unit sources into versioned `raw.<source>_<date>_<n>` tables guarded by the `service.loaded_files` log and `-f` force flag; reference boundaries into the non-versioned `service.boundaries`; secondary attributes folded into a `secondary_attributes` jsonb column
2. **Transform** — natural staging identity, spatial joins against `service.boundaries`, quality gating (`bad_quality` + property links), attributes decomposed to `properties` / `{source}_units_properties`
3. **Load** — consolidated `generators` and `storages` in `core` (serial keys, record-identity in-place updates, collision flags annotated as property links), per-kind property tables `generator_properties` + `generator_units_properties` and `storage_properties` + `storage_units_properties` (ADR 0006)
4. **Marts** — three Postgres materialized views (installation counts, generation capacity, storage capacity) over active units

Each stage verifies its own output (row counts, key uniqueness, join coverage, idempotency) and
fails loudly on violation. Key decisions: staging identity from `reference_id`, core serial keys with
record-identity updates gated on `reference_date` (ADR 0001), gas production capacity recorded as
`installed_capacity` (ADR 0002), marts as materialized views at state grain (ADR 0003), versioned
raw datalake (ADR 0004), quality annotations as property links (ADR 0005), per-kind property tables
(ADR 0006), operational metadata in the `service` schema (ADR 0007).

See `CONTEXT.md` for the glossary and `docs/adr/` for the rationale behind these choices.

## Configuration

`extract` and `boundaries` take their target from two env vars with repo-root
defaults, so they run with **no arguments** — an explicit `TARGET` always wins:

| Env var | Default | Meaning |
|---------|---------|---------|
| `SOURCES_DATA_DIR` | `<repo>/data/sources` | folder scanned for `*.gpkg` unit-source files, each routed by the Energy source value in its GPKG content (the six sources in `SOURCE_NAMES` order; a file that is not a valid single-layer Energy source snapshot is logged and skipped) |
| `BOUNDARIES_MANIFEST` | `<repo>/data/boundaries/boundaries.txt` | manifest listing the `germany_*.gpkg` boundary files, each file's `level` read from its data |

Both are documented (commented) in `.env.example`. `run-all` uses the same
configured paths, so a bare `python -m etl run-all` needs no arguments at all.

## Deploy

Three supported ways to run the stack; the containerized one is the deploy path, and the
AWS variant below is the same stack provisioned with Terraform.

### Containerized stack (recommended)

Requires Docker, the private raw data set under `data/` (sources + boundaries), and
a running PostGIS database*.

```sh
cp .env.example .env          # optional; overrides documented defaults
scripts/seed_data_volume.sh   # one-time: copies data/ + docker.env into the etl_data volume
docker compose -f local_compose.yaml up --build --wait db viz
```

- `db` provisions the schemas and the read-only `viz_reader` role on a fresh volume
  (`docker/viz_reader.sql`); `pipeline` runs `run-all` on demand
  (`docker compose -f local_compose.yaml run --rm pipeline`); `viz` serves the
  Streamlit app on http://localhost:8501. (The server/deploy variant is
  `compose.yaml` — nginx entry point, no published viz port, no published
  PostGIS port, no data volume: `pipeline` runs `python -m etl startup`, the
  explicit bootstrap-then-worker path reading S3/SQS/SNS;
  `build_compose.yaml` is its build-from-source twin.)
- Verify the marts:
  `docker compose -f local_compose.yaml exec -T db psql -U etl -d energy_de -c "SELECT * FROM marts.installation_counts ORDER BY state LIMIT 8"`
- *Alternative: seed a remote PostGIS and point the seed script's `DB_HOST`/`DB_PORT`
  env at it — see `docs/remote-deploy.md`.

### AWS (event-driven, Terraform)

`compose.yaml` is the server variant: an S3 upload becomes an SQS message, the worker on an
EC2 host ingests it, and the app reads the marts. Terraform provisions the AWS side
(`terraform/README.md` for the rationale, `docs/adr/0010-terraform-deployment-contract.md`
for the decisions). On AWS you skip both the raw-data transfer and the data-volume seed:
the worker reads S3, and the `db` service creates the database itself.

**0. Prereqs.** An EC2 host in the target region — Terraform creates no instance, no VPC
and no RDS — with Docker Engine + Compose v2, and a security group allowing 22 (SSH) and 80
(nginx; the only published port). The published images are amd64-only, so on arm64 use
`build_compose.yaml` to compile natively. Terraform takes your ambient AWS credentials:
nothing is committed and the account id is read from `aws_caller_identity`.

**1. Provision the AWS side.**

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set ec2_instance_id — the only required value
terraform init          # state is git-ignored: back it up, or move it to remote state
terraform plan
terraform apply
```

`ec2_instance_id` is the only required value; `aws_region`, `name_prefix`,
`alert_subscriptions` and `additional_alarm_actions` are optional. Confirm the SNS email
subscription by clicking the link AWS sent — it stays silent until you do. The apply ends
with a `check` warning that the host is not yet running under the instance profile; that is
step 2. The remaining steps run from the repository root, which is where `data/` lives.

**2. Attach the profile and reboot.** The one deployment step Terraform cannot take — the
provider has no resource for it, and a profile only reaches the host's own processes on the
next boot.

```bash
aws ec2 modify-instance-attribute --instance-id i-0123456789abcdef0 \
  --iam-instance-profile "$(terraform -chdir=terraform output -raw instance_profile_name)"
aws ec2 stop-instance  --instance-id i-0123456789abcdef0
aws ec2 start-instance --instance-id i-0123456789abcdef0
```

**3. Install the CloudWatch agent on the host**, with the rendered config so it always
matches the log groups the apply created. `terraform/README.md` has the download and
signature check; the filename below is the agent's default config path, which is the only
one its systemd unit reads:

```bash
terraform -chdir=terraform output -raw cloudwatch_agent_config > /tmp/amazon-cloudwatch-agent.json
scp /tmp/amazon-cloudwatch-agent.json ubuntu@<host>:/tmp/
# on the host (Ubuntu), after installing the signed .deb:
sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
sudo cp /tmp/amazon-cloudwatch-agent.json /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
sudo systemctl enable --now amazon-cloudwatch-agent
```

**4. Upload the ten GPKGs to the fixed keys.** This must happen *before* step 5: a missing
Boundary level is fatal to the bootstrap and the pipeline container crash-loops until it is
fixed (a missing Source is not).

```bash
BUCKET=$(terraform -chdir=terraform output -raw bucket_name)
aws s3 cp data/sources/bio.gpkg     "s3://$BUCKET/sources/bio.gpkg"     # …gas, hydro, solar, storage, wind
aws s3 cp data/boundaries/germany_boundary.gpkg  "s3://$BUCKET/boundaries/level-0.gpkg"
aws s3 cp data/boundaries/germany_states.gpkg    "s3://$BUCKET/boundaries/level-1.gpkg"
aws s3 cp data/boundaries/germany_regions.gpkg   "s3://$BUCKET/boundaries/level-2.gpkg"
aws s3 cp data/boundaries/germany_districts.gpkg "s3://$BUCKET/boundaries/level-3.gpkg"
```

The level of each boundary file is read from its own `level` column, which the worker
checks: 0 = the country outline (1 row), 1 = states (19), 2 = regions (38), 3 = districts
(400) — hence the mapping above. The keys are exact, because `etl.ingestion.ACCEPTED_KEYS`
is enforced per message and anything else is logged as `Ignoring event for unaccepted key`.
Re-uploading a corrected file is how you fix a rejected version: object versions are
immutable and the bucket has no lifecycle rule, so the ledger's trail survives.

**5. Start the stack.**

```bash
git clone https://github.com/My-org-oleg/AWS_Energy_DE.git && cd AWS_Energy_DE

# Substitute the three Terraform outputs before writing this file:
#   bucket_name          -> S3_BUCKET
#   ingestion_queue_url  -> SQS_QUEUE_URL
#   alert_topic_arn      -> SNS_TOPIC_ARN
cat > .env <<'ENV'
POSTGRES_USER=etl
POSTGRES_PASSWORD=etl
POSTGRES_DB=energy_de
DATABASE_URL=postgresql://etl:etl@db:5432/energy_de
AWS_DEFAULT_REGION=eu-central-1
S3_BUCKET=aws-energy-de-<account-id>-data
SQS_QUEUE_URL=https://sqs.eu-central-1.amazonaws.com/<account-id>/aws-energy-de-ingestion
SNS_TOPIC_ARN=arn:aws:sns:eu-central-1:<account-id>:aws-energy-de-alerts
VIZ_DATABASE_URL=postgresql://viz_reader:viz@db:5432/energy_de
ENV

docker compose pull
docker compose up -d --wait db
docker compose up -d --wait pipeline viz nginx
```

`DATABASE_URL` and the `POSTGRES_*` pair are coupled — change both or neither. Every
variable above is required with no default, so `up` fails fast rather than starting a
worker that cannot reach its input. The pipeline mounts no data volume, and a fresh db
volume provisions the read-only `viz_reader` role by itself.

**6. Verify and operate.**

```bash
docker compose logs -f pipeline   # Startup report (metadata, key checks, boundaries applied,
                                  # enqueued) then "Ingestion worker reading <queue url>"
docker compose exec -T db psql -U etl -d energy_de -c \
  "SELECT region, count(*) FROM marts.installation_counts GROUP BY 1 ORDER BY 2 DESC LIMIT 6"

docker compose restart pipeline                            # re-run the bootstrap after a new Boundary release
docker compose run --rm pipeline python -m etl redrive      # re-enqueue failed / DLQ versions (--key sources/bio.gpkg)
```

In CloudWatch, the `WorkerStarted` metric and every ingest line live in the
`aws-energy-de/compose` log group. Two alarms notify the SNS topic: **DLQ not empty** (a
message used up its five deliveries) and **oldest message older than the visibility
window** (the worker is alive but not draining). The app is at `http://<host>` — the app has
no auth by design, so put HTTPS or an authenticating proxy in front of nginx.

### Dev host (CI-style)

```sh
.venv/bin/pip install -r requirements.txt -r requirements-viz.txt   # requirements-viz.txt is self-contained (viz-only)
cp .env.example .env          # set DATABASE_URL (and VIZ_DATABASE_URL) to your PostGIS
python -m etl run-all         # extract → staging → core → marts
.venv/bin/streamlit run viz/app.py
```

### Tests

The suite is hermetic: it needs its own database and nothing else — no raw data, no
pre-seeded dev database, and it never reads `data/`.

```sh
.venv/bin/python -m pytest                                 # 535 tests, ~55s
```

Set `TEST_DATABASE_URL` in `.env` (see `.env.example`) to any throwaway database name —
nothing needs creating up front. The suite creates the database if it is missing (the
role needs `CREATEDB`), installs the PostGIS extension, and drops the contents of the
pipeline schemas every session. It **refuses to run** if `TEST_DATABASE_URL` points at
the same database as `DATABASE_URL`, and with `TEST_DATABASE_URL` unset it overwrites
`DATABASE_URL` with an unreachable placeholder, so no test can fall back to dev.

Source and boundary inputs are the small committed fixtures in `tests/fixtures/`;
regenerate them with `python scripts/make_test_fixtures.py` after editing that script.

Verification is layered (`docs/adr/0011-layered-verification-contract.md`):
`tests/test_acceptance_workflow.py` walks the whole event-driven loop once — `startup`
→ `worker` → `redrive` — through the real processor against a real PostGIS with fake AWS
adapters, and names the guarantee that failed; `tests/test_compose_config.py` renders the
three compose variants and asserts the deployment contract (no stack, no database, no raw
data); `tests/test_terraform_config.py` pins the Terraform delivery contract to
`etl/ingestion.py`. The two contract tests need no database and run in CI too; the
walkthrough is part of the suite above and stays off CI, which has no PostGIS service.

### Reset / clean slate

```sh
scripts/drop_pipeline_data.sh # empties raw/stage/core/marts/service (schemas kept)
python -m etl run-all         # rebuild from the raw data
```

> **Upgrading a database seeded before the states/regions/districts rename (#32):**
> there is no migration code — drop the pipeline data and re-run
> (`scripts/drop_pipeline_data.sh && python -m etl run-all`); staging/core are
> dropped and recreated on every load and the marts views are rebuilt.

Full walkthrough, configuration table, and teardown: `docs/containerization.md`.
