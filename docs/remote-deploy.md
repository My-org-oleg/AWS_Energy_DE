# Remote deployment (issue #13)

Step-by-step for deploying the containerized stack (pipeline image + PostGIS +
Streamlit viz app + data-volume seed, `docs/containerization.md`) on a remote
server. The server only needs Docker — nothing private (raw data, connection
settings) is cloned or committed; it is transferred and seeded per machine.

> **On AWS** the server stack does not read the transferred data at all: it
> reads S3 through the SQS queue, and steps 3 and 5 are replaced by
> `terraform/README.md` (the apply, plus attaching the instance profile and
> installing the CloudWatch agent config).

## 0. Prereqs

- Linux server (x86_64/amd64 or arm64 — the `db` image is multi-arch). The
  **published pipeline/viz images are amd64-only** (the CI runners that build
  them are x86), so on an arm64 server they run under emulation until the
  publish workflow goes multi-arch; `build_compose.yaml` compiles native images
  on the server if that matters.
- Docker Engine + Compose v2
- A machine that holds the private raw data set (`data/sources`,
  `data/boundaries`)

## 1. Install Docker

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"
# log out and back in (or: newgrp docker) so the group takes effect
docker --version && docker compose version
```

## 2. Get the code

```bash
git clone https://github.com/My-org-oleg/AWS_Energy_DE.git
cd AWS_Energy_DE
```

No `.env` is needed on the server — the connection settings live in the seeded
volume (`etl_data:/app/data/docker.env`).

## 3. Transfer the private raw data

The data is git-ignored, so it must be copied from the machine that holds it:

```bash
scp -r data/sources data/boundaries user@server:/path/to/Energy_DE/data/
```

On the server, confirm the data came along:

```bash
ls data/sources data/boundaries/boundaries.txt
```

## 4. Set credentials

The defaults are `etl`/`etl`. The compose `POSTGRES_*` variables and the seed
script's `DB_*` variables are a coupled pair — they must match. Set both before
building/seeding; the viz role password defaults to `viz` in both
`docker/viz_reader.sql` (the DB-side provisioning) and the seed. Set both
before seeding if you deviate:

```bash
export POSTGRES_USER=etl POSTGRES_PASSWORD='<you-know>'
export DB_USER=etl DB_PASSWORD='<you-know>'
```

## 5. Pull images + seed the data volume (once per machine)

The pipeline and viz images are published to Docker Hub by CI
(`khvostenko/aws-energy-etl`, `khvostenko/aws-energy-viz` — see
`docs/containerization.md` → "CI & publishing"), so the server only **pulls**:

```bash
docker compose pull                # pulls the published images (no build on the server)
./scripts/seed_data_volume.sh      # creates etl_data volume: raw data + docker.env
```

> **Building instead of pulling:** to compile both images from this repo on the
> server (e.g. an unpushed change, or native arm64), use the build-from-source
> twin and build+seed together: `docker compose -f build_compose.yaml build
> pipeline viz && ./scripts/seed_data_volume.sh`.

## 6. Start the stack (explicit bootstrap, then the worker)

The server variant runs the pipeline container as `python -m etl startup`: on
`up` it bootstraps the deployment — database preconditions, the fixed S3 key
checks, the current Boundary releases, the current Source object versions
enqueued — and only then starts the ingestion worker. A fatal Boundary
condition (a missing or invalid release) exits non-zero **before** the worker
starts, so the container crash-loops until you fix it.

```bash
docker compose up -d --wait db      # waits until the DB really accepts TCP
docker compose up -d --wait pipeline viz nginx
```

The pipeline container mounts no data volume — the worker reads S3 through the
event queue — and takes its settings from the environment (`DATABASE_URL`,
`S3_BUCKET`, `SQS_QUEUE_URL`, `SNS_TOPIC_ARN`, `AWS_DEFAULT_REGION`;
`VIZ_DATABASE_URL` for the app).
The compose files declare them required, so `up` fails fast when one is
missing. PostGIS is not published to the host; `nginx` (port 80 by default,
`NGINX_PORT`) is the only externally reachable surface and proxies to
`viz:8501` (WebSocket upgrade headers in `docker/nginx.conf`, so Streamlit's
live runtime works through it).

To re-run the explicit bootstrap by hand (e.g. after a new Boundary release is
uploaded), restart the pipeline service:

```bash
docker compose restart pipeline
```

To retry object versions the worker left unsettled (failed, or moved to the
DLQ), use the explicit redrive operation:

```bash
docker compose run --rm pipeline python -m etl redrive
docker compose run --rm pipeline python -m etl redrive --key sources/solar.gpkg
```

> **Distroless runtime (issue #34):** the pipeline and viz images are slimmed
> via multi-stage builds on the Chainguard distroless Python base — the runtime
> runs as a **non-root user with no shell/pip**. There is no
> `docker compose exec <service> sh` debugging and no in-container `grep`/`curl`;
> inspect with `docker compose exec <service> python -c "..."` or
> `docker compose run --rm pipeline` for a clean CLI shell-in. The native
> manylinux wheels load on the wolfi/glibc base (the smoke verifies this
> end-to-end); if a future dependency drops prebuilt wheels for the base, the
> documented fallback is a `python:3.12-slim` runtime stage.

The app (Streamlit + PyDeck, reads the marts/core/service tables as the
read-only role) is then at `http://<host>` (port 80) with **no auth**.

A fresh db volume provisions the read-only `viz_reader` role automatically from
`docker/viz_reader.sql` (`/docker-entrypoint-initdb.d`). For a database volume
that predates issue #28, run the seam once by hand:

```bash
docker compose exec -T db psql -U etl -d energy_de -f \
  /docker-entrypoint-initdb.d/99-viz-reader.sql
```

> **Local dev:** the repo also ships `local_compose.yaml`, the twin variant for
> dev machines: the pipeline runs the `run-all` CLI workflow against the mounted
> seed volume (`docker compose run --rm pipeline` for a one-shot full pass), the
> app is published directly at `http://localhost:8501` (`VIZ_PORT`), and PostGIS
> is published on `POSTGRES_PORT` (5433) for host tooling.

## 7. Verify the marts (incl. the `outside` bucket)

```bash
docker compose exec -T db psql -U etl -d energy_de -c \
  "SELECT region, count(*) FROM marts.installation_counts GROUP BY 1 ORDER BY 2 DESC LIMIT 6"
docker compose exec -T db psql -U etl -d energy_de -c \
  "SELECT count(*) FROM marts.installation_counts WHERE region='outside'"
```

## 8. Firewall / exposure

The server variant publishes **only** the `nginx` proxy (`80`, `$NGINX_PORT`) —
PostGIS stays internal to the compose network (no host port at all), and the
viz app publishes no host port either (only `nginx` reaches it, over the
compose network). So the only rule the stack needs is for port 80. Use HTTPS in
front of `nginx` for a public deployment, or leave port 80 open to the world
for a private setup (the app has no auth by design, per the visualization
spec):

```bash
sudo ufw default deny incoming
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp          # nginx → Streamlit viz app (no auth — keep it restricted)
# no rule for 5432 = PostGIS is not published to the host in the server variant
# no rule for 8501 = the viz app publishes no host port in the server variant
sudo ufw enable
```

## 9. Starting over / backups

- Fresh rebuild: `docker compose down -v` (drops the db volume; the server
  stack mounts no data volume, so nothing else to remove — the local-dev
  seed volume `etl_data` is external and survives).
- Backups: the loaded DB lives in the `energy-de-etl_db_data` volume. The
  simplest durable backup is:

```bash
docker compose exec -T db pg_dump -U etl energy_de > backup.sql
```