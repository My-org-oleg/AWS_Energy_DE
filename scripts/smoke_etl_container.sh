#!/usr/bin/env bash
# Smoke test for the containerized ETL + viz stack (issue #13/#28 acceptance).
#
# Proves, on a genuinely fresh stack: the pipeline and viz images build, compose
# brings up a PostGIS database, the pipeline, and the viz app, the containerized
# `run_all` completes every stage (failing loudly on any drift), the three
# marts are produced at state grain including the `outside` bucket, the
# read-only `viz_reader` role is provisioned and can read core/service/marts,
# and the viz service passes Streamlit's `/_stcore/health` probe on port 8501.
#
# This is the seam the containerization work is verified against: it covers
# issue #16's full-stack verification intent (one command proves the stack).
# The pipeline's own integration suite still owns pipeline semantics.
#
# The stack runs in two compose variants: `compose.yaml` is the server/deploy
# variant (nginx reverse-proxies the viz app; no host port is published for
# Streamlit), and `local_compose.yaml` is the local-dev variant (viz directly
# published on host port 8501). This smoke exercises the local variant: it
# probes the app over its published port, so it pins COMPOSE_FILE to
# local_compose.yaml rather than the deploy file.
#
# Usage:  scripts/smoke_etl_container.sh
#
# Requires docker (with the aarch64/arm64 platforms as needed) and the private
# raw data set under data/ (used only to seed the volume).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Local-dev compose variant (viz published on 8501). The deploy variant
# (compose.yaml) goes through nginx instead, which this smoke does not cover.
COMPOSE_FILE="${COMPOSE_FILE:-local_compose.yaml}"
# docker compose only sees an *exported* COMPOSE_FILE; without this it would
# fall back to the (now nonexistent) default compose.yaml.
export COMPOSE_FILE

VOLUME_NAME="${ETL_DATA_VOLUME:-etl_data}"
DB_USER="${DB_USER:-etl}"
DB_NAME="${DB_NAME:-energy_de}"
VIZ_PORT="${VIZ_PORT:-8501}"
VIZ_USER="${VIZ_USER:-viz_reader}"
VIZ_PASSWORD="${VIZ_PASSWORD:-viz}"

FAILURES=0

step() { printf '\n=== %s ===\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; FAILURES=$((FAILURES + 1)); }

query() {
    docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" -tAc "$1"
}

# The same credentials the seeded VIZ_DATABASE_URL carries: TCP (scram, needs
# the password) as the read-only role, not the local socket (trust). This is
# the app's exact connect path, so a pass here proves the pair end to end.
query_viz() {
    docker compose exec -T -e PGPASSWORD="$VIZ_PASSWORD" db psql \
        -h 127.0.0.1 -U "$VIZ_USER" -d "$DB_NAME" -tAc "$1"
}

# Assert `psql` returns an integer count and that `count OP expected`.
# `runner` defaults to `query`; a check that must run as the read-only role
# (its own DB account / connection path) passes `query_viz`.
expect_count() {
    local desc="$1" op="$2" expected="$3" sql="$4" runner="${5:-query}"
    local actual
    actual="$($runner "$sql")" || { fail "$desc (query failed)"; return; }
    case "$actual" in
        '' | *[!0-9-]*)
            fail "$desc (not an integer count: '$actual')"
            return
            ;;
    esac
    if [ "$((actual))" "$op" "$((expected))" ]; then
        printf 'PASS: %s (%s %s %s)\n' "$desc" "$actual" "$op" "$expected"
    else
        printf 'FAIL: %s (got %s, expected %s %s)\n' "$desc" "$actual" "$op" "$expected" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

step "Building the pipeline and viz images"
docker compose build pipeline viz

step "Fresh start: teardown existing stack and volumes (db + seeded data)"
docker compose down -v --remove-orphans >/dev/null
# The data volume is external to compose, so `down -v` alone does not drop it;
# remove it explicitly for a genuinely fresh seed.
docker volume rm -f "$VOLUME_NAME" >/dev/null 2>&1 || true

step "Seeding the detached data volume (one-time per machine, idempotent)"
"$REPO_ROOT/scripts/seed_data_volume.sh" "$VOLUME_NAME"

step "Bringing up PostGIS and waiting for it to be healthy"
docker compose up -d --wait db

step "Running the containerized pipeline (python -m etl run-all)"
# `run` attaches to the already-up db service and returns the container's exit
# code, so a failing stage (or mart drift) fails the smoke run loudly.
docker compose run --rm -T pipeline

step "Verifying the three marts at state grain (incl. outside bucket)"
expect_count "materialized mart views exist" -eq 3 \
    "SELECT count(*) FROM pg_matviews WHERE schemaname='marts' AND matviewname IN ('installation_counts','generation_capacity','storage_capacity')"

expect_count "installation_counts has rows" -gt 0 \
    "SELECT count(*) FROM marts.installation_counts"
expect_count "generation_capacity has rows" -gt 0 \
    "SELECT count(*) FROM marts.generation_capacity"
expect_count "storage_capacity has rows" -gt 0 \
    "SELECT count(*) FROM marts.storage_capacity"

expect_count "installation_counts state grain has no NULL states" -eq 0 \
    "SELECT count(*) FROM marts.installation_counts WHERE state IS NULL"
expect_count "generation_capacity state grain has no NULL states" -eq 0 \
    "SELECT count(*) FROM marts.generation_capacity WHERE state IS NULL"
expect_count "storage_capacity state grain has no NULL states" -eq 0 \
    "SELECT count(*) FROM marts.storage_capacity WHERE state IS NULL"

expect_count "'outside' bucket appears across the marts" -gt 0 \
    "SELECT count(*) FROM (
        SELECT 'installation_counts' AS mart FROM marts.installation_counts WHERE state='outside'
        UNION ALL
        SELECT 'generation_capacity' AS mart FROM marts.generation_capacity WHERE state='outside'
        UNION ALL
        SELECT 'storage_capacity' AS mart FROM marts.storage_capacity WHERE state='outside'
    ) o"

step "Mart sample rows"
docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT * FROM marts.installation_counts ORDER BY state LIMIT 8"
docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT * FROM marts.storage_capacity ORDER BY state LIMIT 5"

step "Provisioning: viz_reader role (idempotent SQL seam)"
# The role was created from /docker-entrypoint-initdb.d when the fresh db
# volume initialized; re-running the same file proves the seam is idempotent.
if docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" \
    -f /docker-entrypoint-initdb.d/99-viz-reader.sql >/dev/null 2>&1; then
    printf 'PASS: viz_reader provisioning re-run (idempotent)\n'
else
    fail "viz_reader provisioning re-run"
fi
expect_count "viz_reader role exists" -eq 1 \
    "SELECT count(*) FROM pg_roles WHERE rolname='viz_reader'"

step "viz_reader can read core/service/marts over the app's TCP path"
expect_count "viz_reader reads core.generators" -gt 0 \
    "SELECT count(*) FROM core.generators" query_viz
expect_count "viz_reader reads core.storages" -gt 0 \
    "SELECT count(*) FROM core.storages" query_viz
expect_count "viz_reader reads service.boundaries" -gt 0 \
    "SELECT count(*) FROM service.boundaries" query_viz
expect_count "viz_reader reads the marts materialized views" -gt 0 \
    "SELECT count(*) FROM marts.installation_counts" query_viz

step "metabase service dropped from the compose stack"
if docker compose config --services | grep -qx metabase; then
    fail "metabase still present in compose"
else
    printf 'PASS: compose has no metabase service\n'
fi

# The command a rendered service runs, normalized to one space-separated line.
# Compose renders `command:` as a YAML list, so the items after the key are
# joined; a scalar form (`command: python -m etl run-all`) is taken as-is.
service_command() {
    awk '
        /^ *command:/ {
            sub(/^ *command: */, "")
            if (length($0) > 0) { gsub(/"/, "", $0); print $0; exit }
            collecting = 1
            next
        }
        collecting && /^ *- / {
            sub(/^ *- */, "")
            gsub(/"/, "", $0)
            printf "%s%s", (printed ? " " : ""), $0
            printed = 1
            next
        }
        collecting { exit }
        END { if (printed) printf "\n" }
    ' <<<"$1"
}

step "Bootstrap-before-worker startup order (issue #8)"
# The event-driven server path must bootstrap before it works: `startup`
# applies the Boundary releases and enqueues the current Source versions, and
# only then does the worker read the queue (ADR 0009). The compose level
# asserts the *order* (the pipeline entrypoint is the startup path, and it
# starts only against a healthy db); the behavioural order — bootstrap before
# the worker, fatal Boundary refusal — is verified by
# tests/test_acceptance_workflow.py against real PostGIS with fake AWS
# adapters, end to end.
server_pipeline_block="$(DATABASE_URL=postgresql://etl:etl@db:5432/energy_de \
    S3_BUCKET=dummy SQS_QUEUE_URL=https://dummy SNS_TOPIC_ARN=arn:dummy \
    AWS_DEFAULT_REGION=eu-central-1 \
    VIZ_DATABASE_URL=postgresql://viz_reader:viz@db:5432/energy_de \
    docker compose -f compose.yaml config \
    | sed -n '/^  pipeline:/,/^  [a-z]/p')"
server_entrypoint="$(service_command "$server_pipeline_block")"
if [ "$server_entrypoint" = "python -m etl startup" ]; then
    printf 'PASS: server pipeline entrypoint is the startup path (bootstrap, then worker)\n'
else
    fail "server pipeline command is not the startup path (got '$server_entrypoint')"
fi

step "Server variant deployment contract (compose.yaml + build_compose.yaml, issues #8/#34)"
# The server stacks must not mount the private source-data seed volume, must
# not publish PostGIS, and must run the explicit startup-before-worker path.
# `config` only interpolates and renders — no stack, data, or AWS needed —
# so the required settings get dummy values here. Both variants carry the same
# contract: one is pulled, the other built from this repository, and a
# deployment that differs from the tested one is a deployment nobody ran.
for variant in compose.yaml build_compose.yaml; do
    server_config="$(DATABASE_URL=postgresql://etl:etl@db:5432/energy_de \
        S3_BUCKET=dummy SQS_QUEUE_URL=https://dummy SNS_TOPIC_ARN=arn:dummy \
        AWS_DEFAULT_REGION=eu-central-1 \
        VIZ_DATABASE_URL=postgresql://viz_reader:viz@db:5432/energy_de \
        docker compose -f "$variant" config)" \
        || fail "$variant does not render"
    if echo "$server_config" | grep -q "etl_data"; then
        fail "$variant still references the etl_data seed volume"
    else
        printf 'PASS: %s mounts no source-data seed volume\n' "$variant"
    fi
    # nginx's 80 is the one allowed publication; anything else (PostGIS on
    # 5432, the viz app on 8501) would be a leaked internal surface.
    published_ports="$(echo "$server_config" | grep 'published:' | tr -d ' \"' | cut -d: -f2 | sort -u)"
    if [ "$published_ports" = "80" ]; then
        printf 'PASS: %s publishes only nginx (PostGIS stays internal)\n' "$variant"
    else
        fail "$variant publishes unexpected ports: $published_ports"
    fi
    if echo "$server_config" | grep -q "DATABASE_URL"; then
        printf 'PASS: %s takes its settings from the environment\n' "$variant"
    else
        fail "$variant does not pass DATABASE_URL from the environment"
    fi
    # Startup order at the compose level: the pipeline (bootstrap + worker)
    # may not start before PostGIS is healthy.
    pipeline_block="$(echo "$server_config" | sed -n '/^  pipeline:/,/^  [a-z]/p')"
    if echo "$pipeline_block" | grep -q "service_healthy"; then
        printf 'PASS: %s pipeline starts only after db is healthy\n' "$variant"
    else
        fail "$variant pipeline does not wait for a healthy db"
    fi
done

step "Local variant contract preserved (local_compose.yaml, issues #13/#33)"
# The server path must not have quietly taken the developer workflow with it:
# the local variant still runs the CLI pass against the seeded volume and
# still publishes the db and viz ports for host tooling.
local_config="$(docker compose -f local_compose.yaml config)" \
    || fail "local_compose.yaml does not render"
if echo "$local_config" | grep -q "etl_data"; then
    printf 'PASS: local variant still mounts the seeded etl_data volume\n'
else
    fail "local variant lost the seeded etl_data seed volume"
fi
local_entrypoint="$(service_command "$(echo "$local_config" | sed -n '/^  pipeline:/,/^  [a-z]/p')")"
if [ "$local_entrypoint" = "python -m etl run-all" ]; then
    printf 'PASS: local variant still runs the containerized run-all pass\n'
else
    fail "local variant no longer runs the run-all pass (got '$local_entrypoint')"
fi
local_ports="$(echo "$local_config" | grep 'published:' | tr -d ' \"' | cut -d: -f2 | sort -u | tr '\n' ' ')"
if [ "$local_ports" = "5433 8501 " ]; then
    printf 'PASS: local variant publishes the db and viz ports (5433, 8501)\n'
else
    fail "local variant publishes unexpected ports: $local_ports"
fi

step "Bringing up the viz service (Streamlit, port ${VIZ_PORT})"
docker compose up -d --wait viz

step "Viz health probe (Streamlit /_stcore/health)"
if curl -fsS "http://127.0.0.1:${VIZ_PORT}/_stcore/health" | grep -qx ok; then
    printf 'PASS: /_stcore/health returned ok (port %s)\n' "$VIZ_PORT"
else
    fail "viz health probe on port $VIZ_PORT"
fi

step "Viz app shell reachable (no auth)"
if curl -fsS -o /dev/null "http://127.0.0.1:${VIZ_PORT}/"; then
    printf 'PASS: viz app served at http://localhost:%s\n' "$VIZ_PORT"
else
    fail "viz app not served at http://localhost:$VIZ_PORT"
fi

step "Seeded volume carries VIZ_DATABASE_URL the app reads"
# The viz container is distroless (no shell/grep — issue #34), so check the
# seeded docker.env from inside it with the python interpreter instead.
if docker compose exec -T viz python -c \
    "import sys; sys.exit(0) if any(l.startswith(r'export VIZ_DATABASE_URL=postgresql://$VIZ_USER:') for l in open('/app/data/docker.env')) else sys.exit(1)"; then
    printf 'PASS: VIZ_DATABASE_URL present in docker.env (viz container mount)\n'
else
    fail "VIZ_DATABASE_URL missing from /app/data/docker.env"
fi

if [ "$FAILURES" -gt 0 ]; then
    printf '\nSMOKE FAILED: %d assertion(s) failed.\n' "$FAILURES" >&2
    exit 1
fi

printf '\nSMOKE PASSED: containerized run_all completed, the three marts\n'
printf 'are populated at state grain (incl. the outside bucket), the\n'
printf 'read-only viz_reader role reads core/service/marts, and the viz\n'
printf 'service answers its health probe on port %s.\n' "$VIZ_PORT"