"""The Compose stacks are the deployment contract (issues #8, #34).

Terraform's configuration is pinned by `tests/test_terraform_config.py` and the
worker's behaviour by the integration suite, but the *container* contract —
which command each variant runs, what it mounts, and what it publishes to the
host — is asserted nowhere cheap. Today it is only checked by
`scripts/smoke_etl_container.sh`, which needs the private raw data set and a
full image build, so a drift between a compose file and the CLI it runs would
only surface on a machine that can afford that. These tests render all three
variants with `docker compose config` (interpolation only: no stack is created,
no image is pulled, no database is touched) and assert the deployment contract
of each:

- the **server variants** (`compose.yaml`, `build_compose.yaml`) run the
  explicit `startup` path — bootstrap before worker — take their settings from
  the environment, mount no private source-data seed volume, keep PostGIS
  internal, and publish only nginx,
- the **build-from-source twin** (`build_compose.yaml`) is the same deployment
  compiled here rather than pulled, so a change to one that is not made to the
  other is a visible failure rather than a deployment that quietly differs from
  the tested one,
- the **local variant** (`local_compose.yaml`) still carries the local
  workflow: the seeded `etl_data` volume mounted at `/app/data`, the `run-all`
  CLI pass, the published db and viz ports, and no AWS settings at all.

Every command asserted here is a command the CLI really registers
(`etl.__main__.cli`), so a renamed or removed command fails this module rather
than a deployment that would crash-loop on `up`.

Cheap by construction: no database, no raw data, no AWS credentials, no network.
The module skips when `docker compose` is unavailable — it renders through
Compose rather than parsing the YAML itself so that the assertions are about
what the operator's `docker compose` would actually run, defaults and
interpolation included.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from etl.__main__ import cli

REPO_ROOT = Path(__file__).resolve().parent.parent

SERVER_VARIANTS = ("compose.yaml", "build_compose.yaml")
ALL_VARIANTS = (*SERVER_VARIANTS, "local_compose.yaml")

# Dummy settings so the server variants' required variables (`${VAR:?}`)
# interpolate: `config` only renders, so nothing here is ever connected to.
DUMMY_ENVIRONMENT = {
    "DATABASE_URL": "postgresql://etl:etl@db:5432/energy_de",
    "VIZ_DATABASE_URL": "postgresql://viz_reader:viz@db:5432/energy_de",
    "S3_BUCKET": "compose-contract-bucket",
    "SQS_QUEUE_URL": "https://sqs.eu-central-1.amazonaws.com/1/ingestion",
    "SNS_TOPIC_ARN": "arn:aws:sns:eu-central-1:1:alerts",
    "AWS_DEFAULT_REGION": "eu-central-1",
}


def _compose_available() -> bool:
    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(
        ["docker", "compose", "version"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return probe.returncode == 0


pytestmark = pytest.mark.skipif(
    not _compose_available(),
    reason="docker compose is not available to render the stacks",
)


def render(variant: str) -> dict:
    """One compose variant, interpolated, as Compose itself resolves it."""
    environment = {
        **os.environ,
        **DUMMY_ENVIRONMENT,
        # Compose warns about an unset COMPOSE_FILE when the variant is named
        # explicitly; naming it here as well would shadow the -f flag.
        "COMPOSE_FILE": "",
    }
    rendered = subprocess.run(
        ["docker", "compose", "-f", variant, "config", "--format", "json"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env={key: value for key, value in environment.items() if value},
    )
    assert rendered.returncode == 0, (
        f"{variant} does not render: {rendered.stderr.strip()}"
    )
    return json.loads(rendered.stdout)


@pytest.fixture(scope="module")
def stacks() -> dict[str, dict]:
    """Every committed compose variant, rendered once."""
    return {variant: render(variant) for variant in ALL_VARIANTS}


@pytest.fixture(scope="module", params=SERVER_VARIANTS)
def server(request, stacks) -> dict:
    """One of the two operator-deployment variants at a time."""
    return stacks[request.param]


@pytest.fixture(scope="module")
def published(stacks) -> dict:
    """`compose.yaml` — the deployment the operator pulls images for."""
    return stacks["compose.yaml"]


@pytest.fixture(scope="module")
def built(stacks) -> dict:
    """`build_compose.yaml` — the same deployment, compiled here."""
    return stacks["build_compose.yaml"]


@pytest.fixture(scope="module")
def local(stacks) -> dict:
    """`local_compose.yaml` — the developer workflow."""
    return stacks["local_compose.yaml"]


def _command(stack: dict, service: str) -> list[str]:
    """The command a service runs, as a list (``[]` when the image default is used)."""
    return list(stack["services"][service].get("command") or [])


def _ports(stack: dict) -> set[int]:
    """Every host port the stack publishes, whatever the service."""
    published = set()
    for service in stack["services"].values():
        for port in service.get("ports") or ():
            published.add(int(port["published"]))
    return published


def _mount_sources(stack: dict) -> set[str]:
    """Every host path and volume name the stack mounts, whatever the service."""
    sources = set()
    for service in stack["services"].values():
        for mount in service.get("volumes") or ():
            source = mount.get("source")
            if source:
                sources.add(source)
    return sources


def _pipeline_environment(stack: dict) -> dict[str, str]:
    return stack["services"]["pipeline"].get("environment") or {}


class TestServerVariants:
    """`compose.yaml` and `build_compose.yaml`: the operator's deployment."""

    def test_the_pipeline_runs_the_explicit_startup_path(self, server):
        # Bootstrap before worker: `startup` is the only command that applies
        # the Boundary releases and enqueues the current Source versions before
        # the worker reads the queue (ADR 0009).
        assert _command(server, "pipeline") == ["python", "-m", "etl", "startup"]
        assert "startup" in cli.commands

    def test_the_pipeline_starts_only_after_the_database_is_healthy(self, server):
        assert server["services"]["pipeline"]["depends_on"] == {
            "db": {"condition": "service_healthy", "required": True}
        }

    def test_the_settings_come_from_the_environment(self, server):
        environment = _pipeline_environment(server)
        for variable in (
            "DATABASE_URL",
            "S3_BUCKET",
            "SQS_QUEUE_URL",
            "SNS_TOPIC_ARN",
            "AWS_DEFAULT_REGION",
        ):
            assert environment[variable] == DUMMY_ENVIRONMENT[variable], variable
        assert server["services"]["viz"]["environment"]["VIZ_DATABASE_URL"] == (
            DUMMY_ENVIRONMENT["VIZ_DATABASE_URL"]
        )

    def test_the_private_source_data_seed_volume_is_not_mounted(self, server):
        # The worker reads S3 through the queue; the private raw data has no
        # business on the server stack.
        assert "etl_data" not in _mount_sources(server)
        assert "etl_data" not in (server.get("volumes") or {})

    def test_only_nginx_is_published(self, server):
        assert _ports(server) == {80}
        assert server["services"]["nginx"]["ports"][0]["target"] == 80
        assert "ports" not in server["services"]["db"]
        assert "ports" not in server["services"]["viz"]
        assert "ports" not in server["services"]["pipeline"]

    def test_nginx_is_the_only_external_surface_and_proxies_the_app(self, server):
        assert server["services"]["nginx"]["depends_on"] == {
            "viz": {"condition": "service_healthy", "required": True}
        }
        mounts = {
            mount["target"]: mount["read_only"]
            for mount in server["services"]["nginx"]["volumes"]
        }
        assert mounts == {"/etc/nginx/conf.d/default.conf": True}

    def test_every_service_restarts_and_the_database_keeps_its_data(self, server):
        for name in ("db", "pipeline", "viz", "nginx"):
            assert server["services"][name]["restart"] == "always", name
        assert [mount["target"] for mount in server["services"]["db"]["volumes"]] == [
            "/var/lib/postgresql/data",
            "/docker-entrypoint-initdb.d/99-viz-reader.sql",
        ]
        assert server["services"]["db"]["healthcheck"]["test"][-1].startswith(
            "pg_isready -h 127.0.0.1"
        )


class TestBuildVariantMatchesThePublishedOne:
    """`build_compose.yaml` is the same deployment, compiled instead of pulled."""

    def test_the_images_are_built_from_this_repository(self, published, built):
        assert "build" not in published["services"]["pipeline"]
        assert built["services"]["pipeline"]["build"]["dockerfile"] == "Dockerfile"
        assert built["services"]["viz"]["build"]["dockerfile"] == "Dockerfile.viz"

    def test_everything_except_the_image_source_is_identical(self, published, built):
        def deployment(stack: dict) -> dict:
            """The deployment contract, with how an image is obtained removed."""
            without_builds = {
                name: {
                    key: value
                    for key, value in service.items()
                    if key not in {"build", "image"}
                }
                for name, service in stack["services"].items()
            }
            return {"services": without_builds, "volumes": stack.get("volumes") or {}}

        assert deployment(built) == deployment(published)

    def test_the_local_variant_is_the_one_that_builds_for_a_local_run(self, stacks):
        # The build-from-source *local* workflow is `local_compose.yaml`; the
        # server variants never seed a data volume, so they never build the
        # run-all CLI variant at all.
        local = stacks["local_compose.yaml"]
        assert local["services"]["pipeline"]["build"]["dockerfile"] == "Dockerfile"


class TestLocalVariant:
    """`local_compose.yaml`: the developer workflow the server path replaced."""

    def test_the_pipeline_runs_the_cli_pass_against_the_seeded_volume(self, local):
        assert _command(local, "pipeline") == ["python", "-m", "etl", "run-all"]
        assert "run-all" in cli.commands

    def test_the_seeded_data_volume_is_mounted_where_the_entrypoint_expects_it(self, local):
        for service in ("pipeline", "viz"):
            mounts = {
                mount["source"]: mount["target"]
                for mount in local["services"][service]["volumes"]
            }
            assert mounts["etl_data"] == "/app/data", service
            assert local["services"][service]["environment"]["DATA_ENV_FILE"] == (
                "/app/data/docker.env"
            )
        assert local["volumes"]["etl_data"] == {"name": "etl_data", "external": True}

    def test_the_database_and_the_app_are_published_for_host_tooling(self, local):
        assert _ports(local) == {5433, 8501}
        assert "nginx" not in local["services"]
        assert local["services"]["db"]["ports"][0]["target"] == 5432
        assert local["services"]["viz"]["ports"][0]["target"] == 8501

    def test_it_carries_no_aws_settings(self, local):
        # The local workflow reads files from the volume, so an S3/SQS/SNS
        # setting on it would be a leftover from the server path.
        for variable in ("S3_BUCKET", "SQS_QUEUE_URL", "SNS_TOPIC_ARN", "AWS_DEFAULT_REGION"):
            assert variable not in _pipeline_environment(local), variable
        assert _command(local, "pipeline") != ["python", "-m", "etl", "startup"]

    def test_the_read_only_role_is_provisioned_on_a_fresh_database(self, local):
        targets = [
            mount["target"] for mount in local["services"]["db"]["volumes"]
        ]
        assert "/docker-entrypoint-initdb.d/99-viz-reader.sql" in targets
        assert (REPO_ROOT / "docker" / "viz_reader.sql").is_file()

    def test_no_variant_carries_the_retired_metabase_service(self, stacks):
        for variant, stack in stacks.items():
            assert "metabase" not in stack["services"], variant


def test_ci_renders_every_compose_variant():
    """The Compose contract is a gate, not a local convenience.

    Rendering is interpolation over committed files: no stack, no data, no AWS
    credentials, so it belongs in the cheap CI path next to the Terraform
    checks. Without this, the server stack's deployment contract would only be
    proven by the smoke script — which needs the private raw data set.
    """
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    assert "tests/test_compose_config.py" in workflow
    assert "docker compose" in workflow
    for variant in ALL_VARIANTS:
        assert variant in workflow, f"CI does not render {variant}"


def test_the_smoke_script_asserts_the_same_contract_against_a_real_stack():
    """The cheap render and the real stack prove the same contract.

    `scripts/smoke_etl_container.sh` renders the server variant with the same
    command these tests use, so a contract that drifts from the smoke's own
    assertions is a gap rather than two independent truths.
    """
    smoke = (REPO_ROOT / "scripts" / "smoke_etl_container.sh").read_text(
        encoding="utf-8"
    )
    assert "docker compose -f compose.yaml config" in smoke
    for variant in SERVER_VARIANTS:
        assert variant in smoke, f"the smoke does not assert the {variant} contract"
    assert "local_compose.yaml" in smoke
