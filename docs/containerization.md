# Containerized ETL + viz (issues #13, #15, #28, #8)

The existing CLI pipeline (`python -m etl`) and the Streamlit viz app (`viz/`)
run unchanged inside containers — **no pipeline code changes**. This is pure
packaging: a pipeline image that wraps the CLI, a viz image for the Streamlit
+ PyDeck app, a Docker Compose stack that provisions PostGIS plus the app, a
read-only `viz_reader` database role the app connects as, and a documented
one-time manual seed procedure that puts the private raw data set and
connection settings into a detached volume the containers read (the **local**
workflow — the server deployment does not use it; see "Server startup path").

> **CLI naming:** the pipeline's `run_all` stage is exposed by Click as the
> command `run-all` (Click verbatim-keeps single-word names and hyphenates
> multiword function names). This is true of the host CLI too — the image
> behaves identically to the host run. The container and everything below use
> `run-all`; `etl/__main__.py` names the Python function `run_all`.

## Architecture

```
 host machine (docker)
 ├─ images pulled from Docker Hub:  khvostenko/aws-energy-etl, khvostenko/aws-energy-viz
 ├─ data/sources/*.gpkg, data/boundaries/*.gpkg   (private, git-ignored)
 ├─ scripts/seed_data_volume.sh  ── once per machine ──►  volume: etl_data
 │                                                       (data + docker.env)
 ├─ compose.yaml:  db          (imresamu/postgis)        ┐
 ├─ compose.yaml:  pipeline    (image; startup → worker) ┤ network
 ├─ compose.yaml:  viz         (image)                ───┤
 └─ compose.yaml:  nginx       reverse-proxy 80 ──► viz ─┘
```

The stack ships as **three compose variants**: `compose.yaml` is the server/deploy
variant — the pipeline and viz containers are **pulled from Docker Hub**
(`khvostenko/aws-energy-etl`, `khvostenko/aws-energy-viz`, both `latest` by default; see
"CI & publishing" below); the viz container publishes no host port, `nginx`
(port `${NGINX_PORT:-80}`) is the only externally reachable surface, proxying to
`viz:8501` with WebSocket upgrade headers for Streamlit's live runtime
(`docker/nginx.conf`), and **PostGIS is not published to the host** — the
database stays internal to the compose network. The pipeline container runs
the server startup path (`python -m etl startup`, issue #8) and mounts **no
data volume**: the worker reads its input from S3/SQS/SNS, with settings
(`DATABASE_URL`, `S3_BUCKET`, `SQS_QUEUE_URL`, `SNS_TOPIC_ARN`,
`AWS_DEFAULT_REGION`) supplied by the
operator's environment. `local_compose.yaml` is the local-dev twin — identical
stack, but the pipeline runs the `run-all` CLI workflow against the mounted
`etl_data` seed volume, the app is published directly on host port
`${VIZ_PORT:-8501}` (no nginx), and PostGIS is published on
`${POSTGRES_PORT:-5433}` for host tooling. `build_compose.yaml` is the
build-from-source twin of the server variant: the same deployment, but both
images are compiled from this repo (`Dockerfile` / `Dockerfile.viz`) instead of
pulled — used for testing local image changes without publishing.

| What | Where |
|------|-------|
| Pipeline image | pulled from Docker Hub (`khvostenko/aws-energy-etl:latest`, `${PIPELINE_IMAGE}` override), built by CI and published on `main`; exposes `python -m etl` unchanged |
| PostGIS database | `db` service; PostGIS extension enabled by the image on first init; the read-only `viz_reader` role provisioned from `docker/viz_reader.sql` on the same init; **not published to the host** in the server variants |
| Visualization app | `viz` service (Streamlit + PyDeck, no auth), pulled from Docker Hub (`khvostenko/aws-energy-viz:latest`, `${VIZ_IMAGE}` override), reads `core`/`service`/`marts` as `viz_reader`; publishes `http://localhost:8501` in `local_compose.yaml` only |
| External entry point | `nginx` service (`nginx:stable-alpine`), publishes `${NGINX_PORT:-80}` → `viz:8501` with WebSocket support; only in `compose.yaml` / `build_compose.yaml` (server variants) |
| Raw data + connection settings | detached named volume `etl_data`, seeded once per machine — **local variant only** |
| Volume mount | `etl_data` → `/app/data` in `local_compose.yaml` (so `run-all`'s `data/sources/...`, `data/boundaries/...` resolve and both containers source `docker.env`); the server variants mount no data volume |
| Server startup path | `python -m etl startup` (issue #8): bootstrap (preconditions, fixed S3 keys, current Boundary releases, current Source versions enqueued), then the worker; settings from the environment |
| Recovery | `python -m etl redrive`: re-enqueues failed or DLQ object versions (`--key` narrows to one key) |
| Smoke seam | `scripts/smoke_etl_container.sh` pins `COMPOSE_FILE=local_compose.yaml` — it probes the app over its published 8501 port, so it exercises the local variant — and additionally asserts the server variant's deployment contract via `docker compose -f compose.yaml config` |
| Build-from-source seams | `build_compose.yaml` (server variant) and `local_compose.yaml` (local variant) compile the images from this repo (`Dockerfile`, `Dockerfile.viz`) |

The images never contain data or real connection settings. `etl/config.py` reads
`DATABASE_URL` / `viz/data.py` reads `VIZ_DATABASE_URL` from the environment;
the shared container entrypoint (`docker/entrypoint.py`) loads
`/app/data/docker.env` from the seeded volume before handing control to the
command (an operator-supplied URL wins over the volume). The only credentials
that live in git are the **documented dev defaults** for a self-contained stack
(`etl`/`etl`, and the read-only `viz_reader`/`viz` pair in
`docker/viz_reader.sql`) — the same precedent as the compose `POSTGRES_*`
defaults; anything real replaces them at seed time and stays in the volume.

> **Distroless runtime (issue #34):** both images are slimmed by a multi-stage
> build on the Chainguard distroless Python base (`cgr.dev/chainguard/python`,
> see `Dockerfile`/`Dockerfile.viz`); the builder installs the requirements into
> a venv, the runtime copies only the venv + code. The runtime runs **as a
> non-root user** and ships **no shell, no pip, no `grep`/`curl`** — so the old
> `docker compose exec <service> sh` debugging and `exec -T ... grep`-style
> smoke steps do not exist; use `docker compose exec <service> python -c` or
> `docker compose run --rm ...` with a single-shot alpine helper instead. A
> Python replacement for the shell entrypoint (`docker/entrypoint.py`) loads
> `docker.env`, then `execvp`s the real command so it stays PID 1 and receives
> SIGINT on `docker stop`. The native manylinux wheels (shapely/pyogrio bundle
> GDAL/GEOS/PROJ) load on the wolfi/glibc base — the smoke exercises this
> end-to-end.

> **Read-only scope:** in the container the app always connects as `viz_reader`
> (the seed always writes `VIZ_DATABASE_URL`). The `DATABASE_URL` fallback in
> `viz/data.py` exists for the dev host, where the operator usually has no
> `VIZ_DATABASE_URL` set and the app reads with the dev `DATABASE_URL` role —
> so on a dev host the read-only guarantee only holds when `VIZ_DATABASE_URL`
> is set (the dev-host SQL seam makes that easy).

> **Platform note:** the `db` image is a multi-arch build
> (`imresamu/postgis`, arm64 + amd64), so the database runs natively on both
> Apple Silicon and x86 servers — the official `postgis/postgis` images are
> amd64-only and would otherwise run under emulation on ARM. The **published**
> pipeline/viz images are amd64-only (the CI runners that publish them are
> x86); the build-from-source variants (`build_compose.yaml`, `local_compose.yaml`)
> compile native `arm64` Python images on the machine they run on.

## One-time seed (per machine)

Prerequisites: docker; the private raw data set present under `data/sources/`
and `data/boundaries/` (including the `boundaries.txt` manifest).

```bash
scripts/seed_data_volume.sh          # defaults to volume `etl_data`
# or with ENV:  ETL_DATA_VOLUME=etl_data scripts/seed_data_volume.sh
```

What it does (idempotent, safe to re-run):

1. creates the named volume `etl_data` if absent,
2. copies `data/sources/` and `data/boundaries/` into it,
3. writes `/data/docker.env` with `DATABASE_URL` (the pipeline role) and
   `VIZ_DATABASE_URL` (the read-only `viz_reader` role the app uses).

A fresh machine is then self-contained — the raw data and connection settings
ride in the volume, not in git or the images.

## Run the pipeline

On a **fresh database volume** the `db` service self-provisions the
`viz_reader` role from `docker/viz_reader.sql` during first init, so the stack
comes up fully provisioned. (An existing volume from before this change gets
the role once by running the seam by hand — see below.)

```bash
docker compose pull                       # pull the published images (default: compose.yaml)
docker compose up -d --wait db             # db healthy (role provisioned on fresh db)
docker compose run --rm pipeline           # the full pass (run-all)
```

The deploy file **pulls** the pipeline and viz images from Docker Hub — nothing
is built on the machine. To build both images from this repo instead (e.g. to
test unpushed changes), run the same stack through the build-from-source twin:

```bash
docker compose -f build_compose.yaml build pipeline viz
docker compose -f build_compose.yaml up -d --wait db
docker compose -f build_compose.yaml run --rm pipeline
```

The full pass runs extract → transform → load → marts against the compose
`db` service, verifying every stage; the marts stage reconciles the stored
pivots to core and **fails loudly (non-zero exit) on drift**.

The image exposes the same CLI as the host, so any single stage works too (with
`db` up):

```bash
docker compose up -d --wait db                      # ensure db is healthy
docker compose run --rm pipeline python -m etl marts
docker compose run --rm pipeline python -m etl transform wind
```

`DATABASE_URL` comes from the seeded volume. `run-all` is the default command,
so a bare `docker compose run --rm pipeline` runs the pass too.

## Server startup path (issue #8)

The server variants (`compose.yaml`, `build_compose.yaml`) run the pipeline
container as `python -m etl startup` — the explicit operator startup path,
which bootstraps the deployment **before** the worker starts:

1. **Database preconditions + fixed S3 key checks** — the service metadata
   (Ingestion runs, load ledger, memberships, stage results) is created, and
   every fixed key (`boundaries/level-{0..3}.gpkg`, `sources/{six sources}.gpkg`)
   is checked with a HEAD. A missing Boundary is **fatal**; a missing Source
   is non-fatal.
2. **Current Boundary releases** — every available release is validated and
   the new ones applied as one batch (already-ledgered versions are skipped),
   the downstream geography rebuilt, and the marts refreshed once — so a
   boundary-only change leaves no stale pivots even with no Source available.
3. **Current Source object versions enqueued** — each available Source's
   current version is sent to the SQS queue as one S3-record message, unless
   the service already holds a run for that exact version (succeeded, stale,
   terminal, DLQ, or in-flight — none are re-enqueued). **No Ingestion run is
   created by the bootstrap**: a run belongs to the worker's processing of the
   message.
4. **The worker starts** — only if no fatal Boundary condition was found.
   Otherwise the command exits non-zero before the worker starts, so the
   container's restart policy crash-loops until the operator fixes the
   Boundary condition.

The bootstrap creates no Ingestion runs and mounts no data volume: the worker
reads S3 through the event queue. Settings come from the environment
(`DATABASE_URL`, `S3_BUCKET`, `SQS_QUEUE_URL`, `SNS_TOPIC_ARN`,
`AWS_DEFAULT_REGION` for the
pipeline; `VIZ_DATABASE_URL` for the app) — the compose files declare them
required (`${VAR:?}`), so `docker compose up` fails fast when one is missing.

### Redrive (recovery)

`python -m etl redrive` is the explicit recovery operation: every object
version the worker left unsettled — a retryable failure, or a message the
queue spent its deliveries on and moved to the DLQ — is sent back to the
queue, whose redelivery resumes the version's existing run. Settled versions
(succeeded, stale, terminal) are left alone. `--key sources/<name>.gpkg`
narrows the redrive to one object key:

```bash
docker compose run --rm pipeline python -m etl redrive
docker compose run --rm pipeline python -m etl redrive --key sources/solar.gpkg
```

## Visualization app (viz)

`viz` is a long-running service that comes up with `docker compose up` (either
variant), with **no auth** (per the visualization spec). It reads the live
`core`, `service`, and `marts` tables/app via the **read-only `viz_reader`
role**, so the app can never mutate the database even if the app itself were
compromised. The app is reached differently per variant:

- **local** (`local_compose.yaml`): directly at <http://localhost:8501>.
- **server** (`compose.yaml`): through the `nginx` proxy at
  `http://<host>` (port 80 by default); the viz container publishes no host
  port. Streamlit's live runtime is a WebSocket, so `docker/nginx.conf`
  forwards `Upgrade`/`Connection` headers and keeps long-lived connections
  open — and `nginx` health-checks the whole entry path (`/_stcore/health`
  through the proxy) before it is considered healthy.

### Read-only role (viz_reader)

`docker/viz_reader.sql` is a single idempotent SQL seam that provisions the
role, used by both sides:

- **compose** — mounted into the `db` container at
  `/docker-entrypoint-initdb.d/99-viz-reader.sql:ro`, so a fresh database
  volume creates the role on the first `up` (issue #28 acceptance). The
  single-file mount is deliberate: a directory mount would hide the postgis
  image's own extension-bootstrap scripts.
- **dev host** — the same file is the dev-host seam:
  `psql -U etl -d energy_de -f docker/viz_reader.sql`
  (run as the role that owns the pipeline schemas).

It creates `viz_reader LOGIN` (password `viz`) iff missing, then grants
**default** privileges (SELECT on tables/sequences, USAGE on schemas, for the
running pipeline role) together with equivalent grants on already-existing
`core`/`service`/`marts` objects. The default privileges are schema-less on
purpose: they cover the `marts` schema and its materialized views, which only
appear after the first pipeline pass, without this file pre-creating (and thus
owning) any schema. Re-running is a no-op and preserves an existing role's
password.

The app connects with `VIZ_DATABASE_URL` (read from the seeded `docker.env`)
and falls back to `DATABASE_URL` on the dev host (`viz/data.py`).

> **Coupled pairs:** like the pipeline's `POSTGRES_*`/`DATABASE_URL`, the
> `viz_reader` password is hard-coded in `docker/viz_reader.sql` and written by
> the seed script (`scripts/seed_data_volume.sh`, `VIZ_PASSWORD` default
> `viz`). Change both together. Existing database volumes (created before this
> issue) provision the role once by hand:
> ```bash
> docker compose exec -T db psql -U etl -d energy_de -f \
>   /docker-entrypoint-initdb.d/99-viz-reader.sql
> ```

## Verify the marts

```bash
docker compose exec -T db psql -U etl -d energy_de -c "SELECT * FROM marts.installation_counts ORDER BY state LIMIT 8"
```

The three materialized views exist in schema `marts` at state grain
(`installation_counts`, `generation_capacity`, `storage_capacity`); units with
no state fall under the `outside` bucket rather than `NULL`.

## Automated smoke check

`scripts/smoke_etl_container.sh` proves the acceptance criteria on a genuinely
fresh stack: teardown → seed → healthy PostGIS → containerized `run-all` →
mart assertions (three views exist, non-empty, no NULL states, `outside`
bucket present) → idempotent `viz_reader` re-provisioning + read checks over
the app's TCP path → viz up and answering `/_stcore/health` on port 8501. The
`metabase` service's absence is asserted too, and the **server variant's
deployment contract** is asserted via `docker compose -f compose.yaml config`:
no `etl_data` seed-volume reference, only nginx's port 80 published (PostGIS
internal), the `startup` command, and environment-supplied settings. The
smoke runs its stack against the **local** variant (it pins
`COMPOSE_FILE=local_compose.yaml` to probe the app over its published port);
the server variant's nginx entry is verified by its own
`/_stcore/health`-through-the-proxy healthcheck in compose, and the startup
order (bootstrap before the worker, fatal-Boundary refusal) is verified against
real PostGIS with fake AWS adapters by `tests/test_acceptance_workflow.py` (the
whole loop: `startup` → `worker` → `redrive`) and, for the unit-level rules,
`tests/test_ingestion.py`.
Re-run it any time the packaging changes:

```bash
scripts/smoke_etl_container.sh
```

The parts of the deployment contract that need no stack, no database and no raw
data are asserted separately, so drift there is a pull-request failure rather
than a surprise on the machine that owns the data:
`tests/test_compose_config.py` renders all three variants with `docker compose
config` and asserts the command, the mounts, the ports and the env, and
`tests/test_terraform_config.py` pins the accepted keys and the delivery contract
to `etl/ingestion.py`. Both run in CI as well (ADR 0011,
`docs/adr/0011-layered-verification-contract.md`); this script stays the seam
that exercises the real images and a real database.

## CI & publishing

Two GitHub Actions workflows keep the images honest:

- `.github/workflows/ci.yml` (issues #14, #10, #36) — four cheap gates on every push to
  `main` and pull request, none of which needs a database, the raw data or AWS
  credentials:
  - **Compile check** — byte-compiles `etl`/`viz`/`tests`/`scripts`/`docker`.
  - **Image build** — builds **both** the pipeline and viz images.
  - **Terraform** — `init -backend=false`, `fmt -check`, `validate`, then
    `tests/test_terraform_config.py`, which pins the accepted keys and the delivery
    contract to `etl/ingestion.py`.
  - **Compose** — `docker compose config` on `compose.yaml`, `build_compose.yaml` and
    `local_compose.yaml` (interpolation only: no stack, no image, no database), then
    `tests/test_compose_config.py`, which asserts the command, mounts, ports and env
    and skips when `docker compose` is unavailable.

  The integration suite is deliberately **not** in CI: it needs a PostGIS service,
  which CI has none of. It runs locally (ADR 0011,
  `docs/adr/0011-layered-verification-contract.md`).
- `.github/workflows/publish-docker.yml` — on every push to `main`, builds and
  pushes the images to Docker Hub as `khvostenko/aws-energy-etl` and
  `khvostenko/aws-energy-viz` (both tagged `latest`, the tags `compose.yaml` pulls).
  Needs the `DOCKER_PASSWORD` repo secret. The images carry no raw data or
  connection settings by design — the seed provides those at deploy time. The
  build-from-source variants are not affected: `build_compose.yaml` and
  `local_compose.yaml` compile from this repo regardless of what is published.

## Configuration

| Env var | Default | Meaning |
|---------|---------|---------|
| `ETL_DATA_VOLUME` | `etl_data` | detached volume holding data + `docker.env` (seed + `local_compose.yaml` only) |
| `POSTGRES_USER` | `etl` | db superuser (compose `db`; the role the pipeline runs as and viz_reader's grants are keyed to) |
| `POSTGRES_PASSWORD` | `etl` | db password (compose `db`) |
| `POSTGRES_DB` | `energy_de` | default database, PostGIS enabled there |
| `POSTGRES_PORT` | `5433` | host port for the db (`local_compose.yaml` only; the server variants publish no db port) |
| `VIZ_PORT` | `8501` | host port for the viz app (`local_compose.yaml` only) |
| `NGINX_PORT` | `80` | host port for the nginx reverse proxy (`compose.yaml`, server variant) |
| `PIPELINE_IMAGE` | `khvostenko/aws-energy-etl:latest` | pipeline image `compose.yaml` pulls (override to pin a tag/registry) |
| `VIZ_IMAGE` | `khvostenko/aws-energy-viz:latest` | viz image `compose.yaml` pulls (override to pin a tag/registry) |
| `DATABASE_URL` | — (required) | the pipeline role's PostGIS URL; server variants read it from the environment (no seeded volume) |
| `S3_BUCKET` | — (required) | versioned data bucket holding the fixed `boundaries/` and `sources/` keys |
| `SQS_QUEUE_URL` | — (required) | queue carrying the S3 ObjectCreated events |
| `SNS_TOPIC_ARN` | — (required) | topic for the worker, blocked-startup and DLQ alerts |
| `AWS_DEFAULT_REGION` | — (required) | region the worker container builds its AWS clients in; without it boto3 would look for one on the instance metadata service |
| `VIZ_DATABASE_URL` | — (required) | the read-only `viz_reader` URL the app connects as; must match `docker/viz_reader.sql` |
| `DB_USER`/`DB_PASSWORD`/`DB_HOST`/`DB_PORT`/`DB_NAME` | `etl`/`etl`/`db`/`5432`/`energy_de` | what the seed script writes into `docker.env` `DATABASE_URL` (for the pipeline) |
| `VIZ_USER`/`VIZ_PASSWORD` | `viz_reader`/`viz` | what the seed script writes into `docker.env` `VIZ_DATABASE_URL` (for the viz app); must match `docker/viz_reader.sql` |

The compose `POSTGRES_*` credentials and the `DATABASE_URL`/`VIZ_DATABASE_URL`
the seed writes are coupled pairs: if you change `POSTGRES_PASSWORD` or
`VIZ_PASSWORD`, change the seed env (and, for the viz password,
`docker/viz_reader.sql`) accordingly and re-run `scripts/seed_data_volume.sh`.

## Teardown / start over

```bash
docker compose down --remove-orphans   # stop and remove containers + network
docker compose down -v                 # also delete the db volume (fresh stack)
```

The data volume is deliberately **external to compose** (`external: true`), so it
*detaches* — `down -v` removes the db volume but leaves `etl_data` intact
(that's what makes the seed "once per machine"). To reset the data volume too:

```bash
docker volume rm "${ETL_DATA_VOLUME:-etl_data}"   # drop the detached data volume
scripts/seed_data_volume.sh                       # re-seed
```

Re-seeding in place also works — `seed_data_volume.sh` overwrites the volume
contents, so `seed → up` is a complete runnable cycle on top of an existing
seed. Re-running `docker volume create` is idempotent.

## Tickets

- #13 containerization
- #14 CI smoke gates (compile check + pipeline image build) — `.github/workflows/ci.yml` now
  lives in CI: byte-compile (`python -m compileall`) + `docker build` of the pipeline and viz
  images on push to `main` and pull requests, later extended by #10 with the Terraform
  `fmt`/`validate` gate plus `tests/test_terraform_config.py`, and by #36 with the Compose
  render plus `tests/test_compose_config.py` — still no raw data, no database and no
  integration suite; a publish
  workflow (`.github/workflows/publish-docker.yml`) pushes both images to Docker Hub on `main`
- #15 Metabase was joined to this compose stack and from #28 **removed** in
  favour of the Streamlit viz app; the marts are the still-authoritative
  analytical layer, surfaced by the app instead of Metabase dashboards.
- #16 full-stack verification seam (one command) — this smoke now covers
  db + pipeline + viz.
- #28 containerize the viz service (read-only `viz_reader`, health probe,
  Metabase removal)