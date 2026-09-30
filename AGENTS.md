# AGENTS.md

## Project status
Implemented. The ETL pipeline (GeoPandas → PostGIS) runs end-to-end from the CLI
(`python -m etl <stage>`, incl. `run-all`; Click hyphenates the `run_all` Python
function name) and is covered by a full integration test suite. Containerized:
a pipeline image + PostGIS + Streamlit-viz compose stack with a one-time data-volume
seed, read-only `viz_reader` role provisioning (dev-host SQL seam in
`docker/viz_reader.sql`), and a smoke seam (`local_compose.yaml`,
`scripts/seed_data_volume.sh`, `scripts/smoke_etl_container.sh`; see
`docs/containerization.md`).
The event-driven deployment is the AWS one: an S3 upload becomes an SQS message, and
`python -m etl startup` (bootstrap, apply Boundary releases, enqueue) followed by
`python -m etl worker` ingests the six Source snapshots and four Boundary levels under the
ten fixed bucket keys, with `python -m etl redrive` for recovery from the ledger (#36,
ADR 0009). The whole loop is walked once end to end by
`tests/test_acceptance_workflow.py` (see Verification).
The AWS deployment's infrastructure is Terraform (`terraform/`): S3 bucket, SQS
queue + DLQ, SNS topic, EC2 instance profile, CloudWatch log groups/metrics/alarms
(#10), with an operator hand-off in `terraform/README.md` and the decisions in
`docs/adr/0010-terraform-deployment-contract.md`.
Treat `TechnicalSpecification.md` as the single authoritative source for data models, table
schemas (raw/staging/service/core/marts), and pipeline design.

## Stack (from spec)
- Python (Pandas, GeoPandas) for ETL — in use
- PostgreSQL + PostGIS (PostGIS required for spatial joins) — in use
- Docker for the containerized stack — in use (compose: db + pipeline + viz, #13/#15/#28)
- Terraform for the AWS infrastructure — in use (`terraform/`, #10; provider `hashicorp/aws`
  ~> 5.0, `required_version >= 1.6.0`). Local verification is a Docker runner:
  `docker run --rm -v "$PWD/terraform:/tf" -w /tf hashicorp/terraform:1.9.5 <fmt|validate>`
  (there is no `terraform` binary on this machine). Credentials are never configured — the
  provider takes the operator's ambient AWS config, and state/`*.tfvars` stay out of git
  (`terraform/.gitignore`)
- Streamlit + PyDeck for the visualization app — in use (the `viz/` package, issues #23+,
  shipped as its own compose `viz` service behind the read-only `viz_reader` role, #28;
  the aborted Dash/Metabase approaches were dropped with the `dash-viz-service` branch
  and #28)

## Location of your data
| What | Where |
|------|-------|
| Raw unit GPKG files (6 sources loaded; Solar polygons & Cogeneration on disk but not loaded) | `data/sources/*.gpkg` |
| Germany boundaries (state, regions+EEZ, districts, municipalities) | `data/boundaries/germany_*.gpkg` |
| Source documentation | `data/sources/data_descriptor_V20260203.xlsx` |

Gotchas:
- **Actual filenames/versions differ from `TechnicalSpecification.md`**: files are `V20260203`
  (spec says `V20250101`) and the gas file is `Gas_Producer_V20260203.gpkg` (spec says
  `Gas_Production`). Trust the files, not the spec.
- The `.venv` is populated; install with `.venv/bin/pip install -r requirements.txt -r requirements-dev.txt`.
  The viz layer (`viz/`, Streamlit + PyDeck) uses the self-contained
  `requirements-viz.txt` (its own pandas/SQLAlchemy/psycopg2 set, no click/geopandas).
  For project-wide dev, install all three: `-r requirements.txt -r requirements-viz.txt -r requirements-dev.txt`.
- `DATABASE_URL` and the raw data are required to run the pipeline; both stay out of git.
- Terraform state and `terraform.tfvars` stay out of git too (`terraform/.gitignore`); the
  committed config carries no account id (it reads `data.aws_caller_identity`) and no
  credential, and `terraform/terraform.tfvars.example` is the only example file.

## Conventions to follow
- Where the spec and files conflict, the files/source datasets win — note the discrepancy rather than silently assuming.
- Follow the spec's ETL stages exactly (extract → staging → core → marts), including
  per-unit-kind property tables (`generator_properties` dimension +
  `generator_units_properties` links, `storage_properties` + `storage_units_properties`
  dimension and links — ADR 0006) and the materialized-view marts.
- Operational metadata (load log + boundary reference layer) lives in the `service` schema
  (ADR 0007), not in raw.

## Verification
pytest is the only test runner (`.venv/bin/python -m pytest`); there are no linters or
typecheckers. The full integration suite is the verification bar, and it is hermetic: it
needs a scratch PostGIS database named by `TEST_DATABASE_URL`, and nothing else — no raw
data, no pre-seeded dev database. It never reads `data/`; the Source and boundary inputs
are the small committed fixtures in `tests/fixtures/` (regenerate with
`python scripts/make_test_fixtures.py`, #12). The session creates the database if it is
missing, installs PostGIS, and drops the contents of the pipeline schemas, so a name that
does not exist yet is enough. The suite **refuses to run** if `TEST_DATABASE_URL` points
at the same database as `DATABASE_URL`, and with `TEST_DATABASE_URL` unset it overwrites
`DATABASE_URL` with an unreachable placeholder so no test can reach dev data. The viz
unit seams (`tests/test_viz_*.py`) take no database but do need `requirements-viz.txt`
installed. Verification is layered (`docs/adr/0011-layered-verification-contract.md`):
`tests/test_acceptance_workflow.py` is part of the suite above and walks the whole
event-driven loop once through the deployment's own entry points — `startup`, then
`worker` per message, then `redrive` — against a real `PipelineProcessor` on a real
PostGIS with fake AWS adapters from the shared harness (`tests/ingestion_harness.py`,
which `tests/test_ingestion.py` imports too), asserting per-phase state; the fakes
hold the queue, so they own delivery counting. `tests/test_terraform_config.py` takes
no database either: it parses
`terraform/*.tf` with `python-hcl2` (pinned `<5.0` in `requirements-dev.txt`, because 5.x
returns a different shape), and pins the accepted-key layout and the delivery contract
against `etl/ingestion.py`. `tests/test_compose_config.py` also takes no database: it
renders all three compose variants with `docker compose config` (interpolation only) and
asserts commands, mounts, ports and env, skipping when `docker compose` is unavailable.
The containerized stack has its own smoke
seam (`scripts/smoke_etl_container.sh`, #13; extended by #28 for db + pipeline + viz).
CI (`.github/workflows/ci.yml`, #14/#10/#36) does *not* run the integration suite — it runs
four cheap gates, all of which need no database, no data and no AWS credentials: byte-compile
via `python -m compileall`, pipeline/viz image builds, `terraform fmt -check` +
`init -backend=false` + `validate` via `hashicorp/setup-terraform` plus
`tests/test_terraform_config.py`, and the compose render plus
`tests/test_compose_config.py`. CI has no PostGIS service — and must never require the raw
data or AWS credentials. The publish workflow
(`.github/workflows/publish-docker.yml`) pushes both images to Docker Hub
(`khvostenko/aws-energy-etl`, `khvostenko/aws-energy-viz`) on `main`, which `compose.yaml`
pulls (`build_compose.yaml`/`local_compose.yaml` still build from source).
The raw data stays private either way.

## Agent skills

### Issue tracker

Issues live as GitHub issues, managed via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` at the repo root, ADRs in `docs/adr/`. See `docs/agents/domain.md`.