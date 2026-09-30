"""The Terraform configuration is the deployment contract (issue #10).

The deployed stack reads S3 through an SQS queue, alerts on an SNS topic, and
runs on the existing EC2 host under an instance profile. None of that is
exercised by the integration suite — the suite proves the *pipeline*, these
tests prove the *infrastructure around it*, and the only way to assert on a
deployment configuration without an AWS account is to read the configuration.
So they parse `terraform/*.tf` and assert the things a live apply would
otherwise only prove by accident:

- the fixed accepted key layout in Terraform is the same set the worker
  accepts (`etl.ingestion.ACCEPTED_KEYS`), so the bucket cannot promise a
  layout the worker rejects,
- the delivery contract (six-hour visibility, five attempts, fourteen-day DLQ
  retention, thirty-day log retention) matches the constants in
  `etl/ingestion.py`,
- the instance profile grants exactly the S3, SQS, SNS and CloudWatch actions
  the worker and the CloudWatch agent call, and nothing wider,
- the queue policy lets S3 deliver to the queue and nothing else, the DLQ
  accepts a redrive from the ingestion queue and nothing else, and the
  subscription endpoints come from variables rather than from a committed
  address,
- nothing secret and no account id is committed, and CI validates the
  configuration.

`hcl2` is a dev dependency (`requirements-dev.txt`): parsing the configuration
is the cheapest seam that catches a typo, a dropped permission, or a queue
whose timeout no longer matches the worker. It is pinned below 5.0 because 5.x
returns a different shape for the same file.
"""

import json
import re
from pathlib import Path

import hcl2
import pytest

from etl.ingestion import (
    ACCEPTED_KEYS,
    MAX_DELIVERY_ATTEMPTS,
    VISIBILITY_TIMEOUT_SECONDS,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TERRAFORM_DIR = REPO_ROOT / "terraform"
TF_FILES = sorted(TERRAFORM_DIR.glob("*.tf"))
AGENT_TEMPLATE = TERRAFORM_DIR / "templates" / "cloudwatch-agent.json.tftpl"
TFVARS_EXAMPLE = TERRAFORM_DIR / "terraform.tfvars.example"
TERRAFORM_README = TERRAFORM_DIR / "README.md"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
ETL_MAIN = REPO_ROOT / "etl" / "__main__.py"

SIX_HOURS = 6 * 60 * 60
FOURTEEN_DAYS = 14 * 24 * 60 * 60

# The line each worker metric counts, written the way the worker emits it. In a
# CloudWatch pattern a term matches anywhere in the log event, and a `?` in
# front of it requires at least one character before it — so whether a pattern
# carries a `?` is a fact about the line it counts, not a matter of taste.
WORKER_LOG_LINES = {
    # The worker prints its queue URL on the line it starts reading.
    "WorkerStarted": "Ingestion worker reading https://sqs.eu-central-1.amazonaws.com/1/q",
    # Both report lines sit inside the CLI's two-space indented report block.
    "StartupBlocked": "  Worker start     : blocked",
    "BoundaryApplied": "  Boundary release : applied (levels 1, 2, 3)",
    # The logger's own format renders the level and the module name.
    "WorkerErrors": "2026-09-29 10:11:12,345 ERROR etl.ingestion: Rejected sources/bio.gpkg",
}


@pytest.fixture(scope="module")
def config() -> dict:
    """Every `terraform/*.tf` parsed into one document.

    One parse for the whole module: the tests read the same declarations from
    several angles (a resource and the local its attribute refers to), and
    re-parsing per test would only make the suite slower. A syntax error fails
    here, which is the point — the files are the artefact under test.
    """
    assert TF_FILES, f"no Terraform files under {TERRAFORM_DIR}"
    document: dict = {}
    for path in TF_FILES:
        with path.open(encoding="utf-8") as handle:
            parsed = hcl2.load(handle)
        # `resource`, `variable`, `locals` and `data` are block *lists*, so a
        # plain `update` would keep only the last file declaring each kind.
        for kind, blocks in parsed.items():
            if isinstance(blocks, list):
                document.setdefault(kind, []).extend(blocks)
            else:
                document[kind] = blocks
    return _unwrap(document)


_INTERPOLATION = re.compile(r"\$\{([^{}]*)\}")


def _unwrap(value):
    """hcl2 keeps Terraform's `${...}` syntax, which makes every assertion about
    a reference read as `'${local.log_groups}'`. Drop it where a value is a
    single reference; a mixed string like `"${arn}/*"` keeps its syntax, because
    there the interpolation is part of the value.
    """
    if isinstance(value, str):
        match = _INTERPOLATION.fullmatch(value)
        return match.group(1) if match else value
    if isinstance(value, list):
        return [_unwrap(item) for item in value]
    if isinstance(value, dict):
        return {key: _unwrap(item) for key, item in value.items()}
    return value


def _bodies(document: dict, kind: str, type_name: str) -> list[dict]:
    """The `{label: body}` of every `<kind> "<type_name>"` block, in file order."""
    return [
        block[type_name]
        for block in document.get(kind, [])
        if type_name in block
    ]


def _all(document: dict, kind: str, type_name: str) -> list[dict]:
    """The body of every block of that type, whichever label it carries."""
    return [body for block in _bodies(document, kind, type_name) for body in block.values()]


def _checks(document: dict) -> dict[str, list[dict]]:
    """The `check` blocks as `{name: [assert, ...]}`."""
    return {
        name: _blocks(body, "assert")
        for block in document.get("check", [])
        for name, body in block.items()
    }


def _resource(document: dict, type_name: str, name: str) -> dict:
    """One resource's body, addressed by type and label."""
    return _named(_bodies(document, "resource", type_name), type_name, name)


def _data(document: dict, type_name: str, name: str) -> dict:
    """One data source's body, addressed by type and label."""
    return _named(_bodies(document, "data", type_name), type_name, name)


def _named(found: list[dict], type_name: str, name: str) -> dict:
    """The body of the one block carrying that label."""
    labelled = [block[name] for block in found if name in block]
    assert len(labelled) == 1, (
        f"expected exactly one {type_name}.{name}, found {len(labelled)}"
    )
    return labelled[0]


def _locals(document: dict) -> dict:
    """All `locals` blocks merged into one mapping."""
    return {
        name: body
        for block in document.get("locals", [])
        for name, body in block.items()
    }


def _variables(document: dict) -> dict:
    """All `variable` blocks as `{name: body}`."""
    return {
        name: body
        for block in document.get("variable", [])
        for name, body in block.items()
    }


def _outputs(document: dict) -> dict:
    """All `output` blocks as `{name: body}`."""
    return {
        name: body
        for block in document.get("output", [])
        for name, body in block.items()
    }


def _blocks(body: dict, name: str) -> list[dict]:
    """Nested blocks of one kind, whether HCL grouped them into a list or not."""
    value = body.get(name, [])
    return value if isinstance(value, list) else [value]


def _block(body: dict, name: str) -> dict:
    """The single nested block of that kind."""
    found = _blocks(body, name)
    assert len(found) == 1, f"expected one {name} block, found {len(found)}"
    return found[0]


def _statements(policy: dict) -> list[dict]:
    """The statements of a policy document."""
    return _blocks(policy, "statement")


def _conditions(statement: dict) -> dict[str, dict]:
    """A statement's conditions, keyed by the condition key they test."""
    return {condition["variable"]: condition for condition in _blocks(statement, "condition")}


def _actions(statement: dict) -> set[str]:
    """The actions of a statement, whether written as a list or as one string."""
    return set(_blocks(statement, "actions") or statement["actions"])


def _worker_policy(document: dict) -> dict[str, dict]:
    """The instance profile's policy statements, keyed by their `sid`.

    Several tests read the same policy from different angles, and the `sid` is
    the only stable address for a statement in a document that is free to
    reorder them.
    """
    policy = _data(document, "aws_iam_policy_document", "worker")
    return {statement["sid"]: statement for statement in _statements(policy)}


def _granted_actions(statements: dict[str, dict]) -> set[str]:
    """Every action any of these statements grants, flattened."""
    return {
        action
        for statement in statements.values()
        for action in _actions(statement)
    }


def _cloudwatch_matches(pattern: str, line: str) -> bool:
    """Whether a CloudWatch filter pattern matches a log line.

    CloudWatch looks for each term of a pattern anywhere in the event, in any
    order, and a `?` in front of a term requires at least one character before
    it. That is the whole of the semantics these patterns rely on.
    """
    for term in pattern.split():
        anchored = term.startswith("?")
        index = line.find(term.lstrip("?"))
        if index < 0 or (anchored and index == 0):
            return False
    return True


def _log_format() -> str:
    """The format the CLI configures `logging` with, read from its own source."""
    match = re.search(r'format="([^"]+)"', ETL_MAIN.read_text(encoding="utf-8"))
    assert match, "etl/__main__.py no longer configures a logging format"
    return match.group(1)


def _rendered_agent_config() -> dict:
    """The CloudWatch agent template as JSON, with interpolations stubbed out.

    Every interpolation in the template sits inside a string literal, so
    replacing `${...}` with a placeholder leaves the document valid JSON. That
    keeps the structural assertions (which files are collected, into which
    groups, with which options) possible without a Terraform evaluation, which
    this suite deliberately does not have.
    """
    text = AGENT_TEMPLATE.read_text(encoding="utf-8")
    return json.loads(re.sub(r"\$\{[^}]*\}", "placeholder", text))


def test_every_deployment_resource_is_declared(config):
    """The deployment is one bucket, one queue pair, one topic, one profile.

    Counted rather than merely present: an extra queue or a second topic would
    be invisible to a reader of the code and ambiguous for the worker, which is
    handed exactly one queue URL and one topic ARN.
    """
    assert len(_bodies(config, "resource", "aws_s3_bucket")) == 1
    assert len(_bodies(config, "resource", "aws_sqs_queue")) == 2
    assert len(_bodies(config, "resource", "aws_sns_topic")) == 1
    assert len(_bodies(config, "resource", "aws_iam_instance_profile")) == 1
    assert len(_bodies(config, "resource", "aws_s3_bucket_notification")) == 1


def test_data_bucket_is_private_versioned_and_never_expires(config):
    """One private, versioned bucket that keeps every object version.

    Versioning is what makes an upload identifiable by an immutable object
    version, so it is a correctness requirement, not a nicety. No lifecycle
    configuration at all: object versions are the audit trail the Ingestion run
    ledger points at, so a bucket that expires them would quietly delete
    history the pipeline still claims to reference.
    """
    bucket = _resource(config, "aws_s3_bucket", "data")
    assert bucket["force_destroy"] is False

    versioning = _resource(config, "aws_s3_bucket_versioning", "data")
    assert _block(versioning, "versioning_configuration")["status"] == "Enabled"

    public_access = _resource(config, "aws_s3_bucket_public_access_block", "data")
    for flag in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert public_access[flag] is True, f"{flag} must be true"

    ownership = _resource(config, "aws_s3_bucket_ownership_controls", "data")
    assert _block(ownership, "rule")["object_ownership"] == "BucketOwnerEnforced"

    encryption = _resource(
        config, "aws_s3_bucket_server_side_encryption_configuration", "data"
    )
    default = _block(_block(encryption, "rule"), "apply_server_side_encryption_by_default")
    assert default["sse_algorithm"] == "AES256"

    lifecycle = [
        type_name
        for type_name in {
            name
            for block in config.get("resource", [])
            for name in block
        }
        if "lifecycle" in type_name
    ]
    assert not lifecycle, f"the bucket must not expire objects: {lifecycle}"


def test_bucket_policy_denies_plain_http(config):
    """The bucket refuses unencrypted transport.

    The bucket is private, but a private bucket is still reachable over plain
    HTTP, which would put the published source data on the wire in the clear.
    """
    policy = _data(config, "aws_iam_policy_document", "bucket")
    statements = _statements(policy)
    assert len(statements) == 1
    statement = statements[0]

    assert statement["effect"] == "Deny"
    assert set(statement["actions"]) == {"s3:*"}
    assert _conditions(statement)["aws:SecureTransport"]["values"] == ["false"]
    assert statement["resources"] == [
        "aws_s3_bucket.data.arn",
        "${aws_s3_bucket.data.arn}/*",
    ]


def test_accepted_key_layout_matches_the_worker(config):
    """The bucket's fixed layout is the worker's accepted-key set, exactly.

    `etl.ingestion.ACCEPTED_KEYS` is the contract the worker enforces — a
    record under any other key is ignored. Terraform declares the same list so
    the bucket can name these objects in a policy; if the two drift, the
    deployment either locks the worker out of a key it must read or advertises
    a key the worker will never process.
    """
    accepted = _locals(config)["accepted_keys"]
    assert sorted(accepted) == sorted(ACCEPTED_KEYS)
    assert len(accepted) == len(ACCEPTED_KEYS), "the layout must not repeat a key"


def test_ingestion_queue_delivery_contract_matches_the_worker(config):
    """Six hours of visibility, five deliveries, fourteen days in the DLQ.

    These are the numbers `etl/ingestion.py` is written against
    (`VISIBILITY_TIMEOUT_SECONDS`, `MAX_DELIVERY_ATTEMPTS`), and the queue is
    what enforces them. A visibility timeout shorter than the six hours the
    worker extends to would let a second delivery start on top of a running
    ingest; a `maxReceiveCount` above the worker's own count would let the
    worker keep working a message the queue has given up on.
    """
    locals_ = _locals(config)
    assert locals_["visibility_timeout_seconds"] == SIX_HOURS
    assert locals_["max_receive_count"] == MAX_DELIVERY_ATTEMPTS
    assert locals_["dlq_message_retention_seconds"] == FOURTEEN_DAYS
    assert SIX_HOURS == VISIBILITY_TIMEOUT_SECONDS

    queue = _resource(config, "aws_sqs_queue", "ingestion")
    assert queue["visibility_timeout_seconds"] == "local.visibility_timeout_seconds"
    assert "fifo_queue" not in queue, "the worker consumes one standard queue"

    redrive = queue["redrive_policy"]
    assert "aws_sqs_queue.dlq.arn" in redrive
    assert "local.max_receive_count" in redrive

    dlq = _resource(config, "aws_sqs_queue", "dlq")
    assert dlq["message_retention_seconds"] == "local.dlq_message_retention_seconds"


def test_object_created_events_are_delivered_to_the_ingestion_queue(config):
    """Uploads become queue messages, and only GPKG uploads.

    The worker has no poller: an upload it is not told about is never ingested.
    S3 can filter one prefix/suffix pair per notification configuration, so the
    `.gpkg` suffix is the widest filter available here; the exact-key rule is
    enforced where it is exact, by the worker and by the instance profile's
    object grant.
    """
    notification = _resource(config, "aws_s3_bucket_notification", "ingestion")
    assert notification["bucket"] == "aws_s3_bucket.data.id"

    queue = _block(notification, "queue")
    assert queue["queue_arn"] == "aws_sqs_queue.ingestion.arn"
    assert any(event.startswith("s3:ObjectCreated") for event in queue["events"])
    assert queue["filter_suffix"] == ".gpkg"


def test_queue_policy_admits_only_s3_delivery_from_this_bucket(config):
    """S3 may send to the queue; nothing else may, and not from another bucket.

    The delivery permission lives on the queue (S3's side of the integration),
    and it is conditioned on both the bucket ARN and the account — without the
    account condition a bucket elsewhere could use this queue as a relay.
    """
    statements = _statements(_data(config, "aws_iam_policy_document", "ingestion_queue"))
    assert len(statements) == 1
    statement = statements[0]

    assert statement["effect"] == "Allow"
    assert _actions(statement) == {"sqs:SendMessage"}
    assert statement["resources"] == ["aws_sqs_queue.ingestion.arn"]
    assert _block(statement, "principals") == {
        "type": "Service",
        "identifiers": ["s3.amazonaws.com"],
    }

    conditions = _conditions(statement)
    assert conditions["aws:SourceArn"]["values"] == ["aws_s3_bucket.data.arn"]
    assert conditions["aws:SourceAccount"]["values"] == [
        "data.aws_caller_identity.current.account_id"
    ]


def test_dlq_redrive_is_allowed_only_from_the_ingestion_queue(config):
    """The dead-letter queue accepts a redrive from the ingestion queue only.

    SQS moves a message to the DLQ under the source queue's redrive policy,
    which needs the DLQ's own policy to allow it. Scoping that to this queue's
    ARN means the DLQ cannot be used as a general dead-letter sink.
    """
    statements = _statements(_data(config, "aws_iam_policy_document", "dlq"))
    assert len(statements) == 1
    statement = statements[0]

    assert statement["resources"] == ["aws_sqs_queue.dlq.arn"]
    conditions = _conditions(statement)
    assert conditions["aws:SourceArn"]["values"] == ["aws_sqs_queue.ingestion.arn"]
    assert "aws:SourceAccount" in conditions


def test_alert_topic_has_one_parameterised_subscription_shape(config):
    """One topic, and subscription endpoints supplied by the operator.

    An address committed to the repository is an address nobody can change
    without a code change, and an email address in a public repository is a
    harvested address. The subscriptions are a variable with no default, so an
    apply without endpoints provisions the topic and nothing else.
    """
    topic = _resource(config, "aws_sns_topic", "alerts")
    assert topic["name"] == "local.topic_name"

    subscriptions = _resource(config, "aws_sns_topic_subscription", "alerts")
    assert subscriptions["for_each"] == "var.alert_subscriptions"
    assert subscriptions["topic_arn"] == "aws_sns_topic.alerts.arn"
    assert subscriptions["endpoint"] == "each.value.endpoint"
    assert subscriptions["protocol"] == "each.value.protocol"

    declared = _variables(config)["alert_subscriptions"]
    assert declared["default"] == {}, "an apply must not invent a subscriber"
    assert "map(object" in declared["type"]
    for field in ("protocol", "endpoint"):
        assert field in declared["type"]


def test_instance_profile_grants_only_what_the_worker_calls(config):
    """Least privilege, checked statement by statement.

    The worker calls `head_object`/`get_object` on the accepted keys, receives,
    deletes, re-times and sends on the one queue, and publishes on the one topic;
    the CloudWatch agent adds log writes. Anything wider — `s3:*`, a
    bucket-wide object grant, a wildcard log group, a metric grant nothing
    publishes — would be a privilege the deployment does not need and a
    reviewer cannot check by eye.
    """
    expected = {
        "ReadAcceptedObjects": {
            "s3:GetObject",
            "s3:GetObjectVersion",
        },
        "OperateIngestionQueue": {
            "sqs:ChangeMessageVisibility",
            "sqs:DeleteMessage",
            "sqs:ReceiveMessage",
            "sqs:SendMessage",
        },
        "PublishAlerts": {"sns:Publish"},
        "WriteOperationalLogs": {
            "logs:CreateLogStream",
            "logs:DescribeLogStreams",
            "logs:PutLogEvents",
        },
    }

    statements = _worker_policy(config)
    assert set(statements) == set(expected), (
        "every grant needs a named reason, and nothing may be granted without "
        f"one; got {sorted(statements)}, want {sorted(expected)}"
    )
    for sid, actions in expected.items():
        assert _actions(statements[sid]) == actions, f"{sid} grants the wrong actions"

    for sid, statement in statements.items():
        for action in _actions(statement):
            assert not action.endswith(":*"), f"{sid} grants the wildcard {action}"

    # No metric publication: the worker metrics are log-derived and the queue
    # metrics are native, so nothing on the host calls `PutMetricData`. The
    # wildcard resource such a grant needs is the widest one in the policy.
    assert not any(
        action.startswith("cloudwatch:") for action in _granted_actions(statements)
    )


def test_object_grant_is_confined_to_the_accepted_keys(config):
    """The worker can read the fixed keys and nothing else in the bucket.

    A bucket-wide object grant would make every future upload readable by the
    host, which is not a permission the workflow needs: the worker only ever
    reads a version of an accepted key.
    """
    resources = _worker_policy(config)["ReadAcceptedObjects"]["resources"]
    assert "local.accepted_keys" in resources
    assert "aws_s3_bucket.data.arn}" in resources


def test_nothing_is_granted_beyond_what_the_worker_calls(config):
    """The policy grants no bucket-level or whole-service permission.

    A grant on the bucket itself is a grant on the listing of every key in it,
    and a grant the worker never calls is one nothing in this repository can be
    reviewed against: the worker addresses its accepted keys directly and reads
    the delivery count off the message it receives, so it needs neither a
    listing nor a queue-attribute read.
    """
    statements = _worker_policy(config)

    for sid, statement in statements.items():
        assert "aws_s3_bucket.data.arn" != statement["resources"], (
            f"{sid} is granted the bucket itself, which is a listing grant"
        )

    granted = _granted_actions(statements)
    for action in ("s3:ListBucket", "s3:ListBucketVersions", "sqs:GetQueueAttributes"):
        assert action not in granted, f"{action} is granted but never called"


def test_queue_grant_covers_the_ingestion_queue_only(config):
    """No SQS access to the DLQ: the worker never reads or drains it.

    Draining the DLQ is an operator action (`python -m etl redrive` re-enqueues
    from the database, not from the DLQ), so the instance needs no DLQ
    permission at all.
    """
    statements = _worker_policy(config)

    assert statements["OperateIngestionQueue"]["resources"] == [
        "aws_sqs_queue.ingestion.arn"
    ]
    assert statements["PublishAlerts"]["resources"] == ["aws_sns_topic.alerts.arn"]


def test_log_grant_is_confined_to_the_groups_terraform_creates(config):
    """Log writes reach the created groups and nothing else.

    The agent creates streams inside groups that already exist, so retention
    stays Terraform's to set. The `:*` is a stream suffix, not a wildcard
    prefix: it names the groups, it does not widen them.
    """
    resources = _worker_policy(config)["WriteOperationalLogs"]["resources"]
    assert resources == [
        "${aws_cloudwatch_log_group.compose.arn}:*",
        "${aws_cloudwatch_log_group.agent.arn}:*",
    ]


def test_host_is_checked_for_the_worker_profile(config):
    """The host is the instance the operator named, and the profile is a check.

    The AWS provider has no resource for attaching a profile to an instance, so
    the attachment is a documented CLI step and Terraform's part is to notice
    when it has not happened. A `check` block warns rather than failing, because
    the first apply cannot see its own profile attached. `ec2:RunInstances` is
    deliberately absent — nothing here launches an instance.
    """
    role = _resource(config, "aws_iam_role", "worker")
    assert role["assume_role_policy"] == "data.aws_iam_policy_document.worker_assume_role.json"
    trust = _data(config, "aws_iam_policy_document", "worker_assume_role")
    principal = _block(_statements(trust)[0], "principals")
    assert principal["type"] == "Service"
    assert principal["identifiers"] == ["ec2.amazonaws.com"]

    profile = _resource(config, "aws_iam_instance_profile", "worker")
    assert profile["role"] == "aws_iam_role.worker.name"
    assert _data(config, "aws_instance", "server")["instance_id"] == "var.ec2_instance_id"

    checked = _checks(config)
    assert set(checked) == {"host_runs_the_worker_profile"}
    assertions = checked["host_runs_the_worker_profile"]
    assert len(assertions) == 1
    assert assertions[0]["condition"] == (
        "data.aws_instance.server.iam_instance_profile"
        " == aws_iam_instance_profile.worker.name"
    )
    assert "terraform/README.md" in assertions[0]["error_message"]

    assert not any(
        action.startswith("ec2:") for action in _granted_actions(_worker_policy(config))
    )


def test_operational_log_groups_retain_thirty_days(config):
    """Both log groups expire after thirty days, set here and not by default.

    Thirty days is the window an investigation gets once the EC2 console is
    gone. Retention is Terraform's to set because CloudWatch's own default is
    `never expire`, which is the mistake a log group makes silently: it costs
    nothing until someone needs a log from last March.
    """
    variables = _variables(config)
    assert variables["log_retention_days"]["default"] == 30
    assert "validation" in variables["log_retention_days"]

    for label in ("compose", "agent"):
        group = _resource(config, "aws_cloudwatch_log_group", label)
        assert group["name"] == f'local.log_groups["{label}"]'
        assert group["retention_in_days"] == "var.log_retention_days"

    names = list(_locals(config)["log_groups"].values())
    assert len(set(names)) == len(names), "log group names collide"
    assert all("var.log_group_prefix" in name for name in names)


def test_worker_metrics_come_from_the_compose_log_group(config):
    """Log-derived worker metrics, and no alarm on them.

    The worker writes its progress to stdout, which the agent ships to the
    Compose log group, so counting the lines it already prints is the cheapest
    honest activity signal — it needs no code change in the container. Each
    pattern is narrow enough to only match the ETL logger or the CLI's own
    output, because that group also carries the database, the app and the proxy.

    These filters publish metrics only: the two alert paths (a rejected object
    version published by the worker, and the DLQ alarm) already report problems,
    and a third would report the same one twice.
    """
    locals_ = _locals(config)
    patterns = locals_["worker_metric_patterns"]
    assert patterns, "the worker must expose at least one metric"

    groups = _resource(config, "aws_cloudwatch_log_metric_filter", "worker")
    assert groups["for_each"] == "local.worker_metric_patterns"
    assert groups["log_group_name"] == "aws_cloudwatch_log_group.compose.name"

    transformation = _block(groups, "metric_transformation")
    assert transformation["namespace"] == "var.metrics_namespace"
    assert transformation["name"] == "each.key"
    assert _variables(config)["metrics_namespace"]["default"]


def test_every_worker_metric_pattern_matches_a_line_the_worker_prints(config):
    """Each pattern can actually match the line it was written for.

    The Docker json-file driver makes every stdout line its own log event, so
    "at the start of the event" is "at the start of the line". A `?` on a line
    that starts its own text is therefore a filter that can never fire — a
    metric that publishes nothing while looking configured, which is the one
    failure a parsed-configuration test is otherwise blind to.
    """
    patterns = _locals(config)["worker_metric_patterns"]
    assert set(patterns) == set(WORKER_LOG_LINES), (
        "every pattern needs the line it counts, so a new metric cannot ship "
        f"unverified; unexpected {sorted(set(patterns) ^ set(WORKER_LOG_LINES))}"
    )
    for metric, line in WORKER_LOG_LINES.items():
        assert _cloudwatch_matches(patterns[metric], line), (
            f"{metric} cannot match {line!r} with pattern {patterns[metric]!r}"
        )

    # The example lines are the worker's own: the fixed text each one carries is
    # written by the CLI, so a renamed banner fails here instead of quietly
    # emptying a metric in production.
    source = " ".join(ETL_MAIN.read_text(encoding="utf-8").split())
    assert "Ingestion worker reading" in source
    assert "Worker start" in source and "'blocked'" in source
    assert "Boundary release : applied" in source
    assert "%(levelname)s %(name)s:" in _log_format(), (
        "the error pattern counts the lines this log format renders"
    )


def test_no_alarm_announces_exhaustion_twice(config):
    """No metric-filter alarm duplicates the worker's or the DLQ's alerts.

    Each operational problem is reported once, by exactly one path: a rejected
    object version by the worker, retry exhaustion by the DLQ alarm, a stalled
    queue by the age alarm. An alarm on a log-derived metric would be a second
    voice for something already announced, so both alarms sit on the queue's
    own `AWS/SQS` metrics.
    """
    alarms = _all(config, "resource", "aws_cloudwatch_metric_alarm")
    assert {alarm["namespace"] for alarm in alarms} == {"AWS/SQS"}
    assert len(alarms) == 2


def test_cloudwatch_agent_config_collects_every_log_group(config):
    """Every log group Terraform creates is collected by the agent config.

    A group with no collector is a group nothing is ever written to, and the
    operator only finds out when an investigation comes up empty. The container
    entry sets `publish_multi_logs`, because a container's log file cannot be
    told apart from its path: the stack shares one group with one stream per
    container, which is a real limit of shipping Docker logs this way and the
    reason no per-service group is claimed.
    """
    groups = _locals(config)["log_groups"]
    template = AGENT_TEMPLATE.read_text(encoding="utf-8")
    main = (TERRAFORM_DIR / "main.tf").read_text(encoding="utf-8")

    entries = _rendered_agent_config()["logs"]["logs_collected"]["files"]["collect_list"]
    assert len(entries) == len(groups)
    for entry in entries:
        assert entry["log_group_name"] == "placeholder"
        assert entry["log_stream_name"]
        assert entry["file_path"]

    containers = [entry for entry in entries if entry.get("publish_multi_logs")]
    assert len(containers) == 1, "the container log file is one glob, one group"
    assert "${docker_container_log_glob}" in template
    assert _variables(config)["docker_container_log_glob"]["default"].endswith(
        "*/*-json.log"
    )

    # Every value the template interpolates has to be handed to it by the
    # output, from the right source, or `templatefile` fails at apply time.
    sources = (
        ("docker_container_log_glob", "var.docker_container_log_glob"),
        ("worker_log_group", "local.worker_log_group"),
        ("agent_log_group", "local.agent_log_group"),
    )
    rendered_by = _outputs(config)["cloudwatch_agent_config"]["value"]
    assert set(re.findall(r"\$\{(\w+)\}", template)) == {name for name, _ in sources}
    for _, source in sources:
        assert source in rendered_by, f"the agent config output ignores {source}"

    agent_own = [
        entry for entry in entries if "amazon-cloudwatch-agent.log" in entry["file_path"]
    ]
    assert len(agent_own) == 1, "the agent's own log is collected, so silence is explainable"

    # Each group is reachable from the template through a local, so adding a
    # group without a collection section fails here.
    referenced = set(re.findall(r'local\.log_groups\["([^"]+)"\]', main))
    assert referenced == set(groups), f"uncollected log groups: {set(groups) - referenced}"

    assert "cloudwatch-agent.json.tftpl" in _outputs(config)["cloudwatch_agent_config"]["value"]

def test_outputs_cover_the_settings_the_deployment_needs(config):
    """The outputs are the hand-off to the Compose stack.

    `compose.yaml` requires `S3_BUCKET`, `SQS_QUEUE_URL` and `SNS_TOPIC_ARN`, so
    those three have to be printed under names the operator can act on; the log
    groups and the DLQ are what an operator looks up while investigating, and
    the agent config is what the two post-apply steps need.
    """
    outputs = _outputs(config)
    for name in (
        "aws_region",
        "bucket_name",
        "ingestion_queue_url",
        "alert_topic_arn",
        "dead_letter_queue_url",
        "log_group_names",
        "cloudwatch_agent_config",
    ):
        assert name in outputs, f"missing output {name}"
        assert outputs[name]["description"], f"output {name} is undocumented"

    assert outputs["bucket_name"]["value"] == "aws_s3_bucket.data.id"
    assert outputs["ingestion_queue_url"]["value"] == "aws_sqs_queue.ingestion.url"
    assert outputs["alert_topic_arn"]["value"] == "aws_sns_topic.alerts.arn"
    assert outputs["aws_region"]["value"] == "var.aws_region"

    for name, variable in (
        ("bucket_name", "S3_BUCKET"),
        ("ingestion_queue_url", "SQS_QUEUE_URL"),
        ("alert_topic_arn", "SNS_TOPIC_ARN"),
        ("aws_region", "AWS_DEFAULT_REGION"),
    ):
        assert variable in outputs[name]["description"], (
            f"output {name} must name the Compose variable {variable}"
        )

    # A value Terraform prints but Compose never reads is a hand-off the
    # operator has to invent, and one the stack then fails to start without.
    compose = (REPO_ROOT / "compose.yaml").read_text(encoding="utf-8")
    for variable in ("S3_BUCKET", "SQS_QUEUE_URL", "SNS_TOPIC_ARN", "AWS_DEFAULT_REGION"):
        assert f"{variable}: ${{{variable}" in compose, (
            f"compose.yaml does not require {variable}"
        )


def test_nothing_secret_or_account_specific_is_committed():
    """The committed configuration carries no credentials and no account id.

    State and variable files are the two places real values live, and both are
    ignored. What is committed has to be safe to read: the account is resolved
    from the caller's identity, and no key, token or password is spelled out.
    """
    committed = {
        path: path.read_text(encoding="utf-8")
        for path in [*TF_FILES, TFVARS_EXAMPLE, AGENT_TEMPLATE, TERRAFORM_README]
    }
    secret_patterns = {
        "access key id": r"AKIA[0-9A-Z]{8,}",
        "secret access key": r"aws_secret_access_key",
        "password": r"(?i)\bpassword\s*=",
        "bearer token": r"(?i)\bbearer\s+[A-Za-z0-9._-]{12,}",
    }
    for label, pattern in secret_patterns.items():
        for path, text in committed.items():
            assert not re.search(pattern, text), f"{path} looks like it holds a {label}"

    for path, text in committed.items():
        account_ids = re.findall(r"\b\d{12}\b", text)
        assert not account_ids, (
            f"{path} hard-codes an account id; resolve it with "
            "data.aws_caller_identity"
        )

    ignored = (TERRAFORM_DIR / ".gitignore").read_text(encoding="utf-8")
    for pattern in ("*.tfstate", "*.tfvars", ".terraform/"):
        assert pattern in ignored, f"terraform/.gitignore must ignore {pattern}"


def test_ci_validates_the_configuration():
    """CI runs `terraform fmt`, `validate` and the configuration tests.

    `validate` needs the provider schemas and no credentials, so it is a cheap
    gate: a renamed argument or a broken reference fails the pull request
    rather than the apply. The configuration tests need no database, no raw data
    and no credentials either, and they are what pins the Terraform delivery
    contract to the worker's constants — the integration suite cannot cover any
    of this.
    """
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    assert "hashicorp/setup-terraform" in workflow
    assert "init -backend=false" in workflow, "the provider must be fetched without a backend"
    assert "fmt -check" in workflow
    assert "validate" in workflow
    assert "tests/test_terraform_config.py" in workflow
    assert "requirements-dev.txt" in workflow


def test_deployment_documentation_covers_the_handoff():
    """The apply steps and the operator hand-off are written down.

    Terraform is only reproducible if the next person can find the variables,
    the outputs, and the steps that follow an apply — installing the agent
    config and pointing Compose at the outputs.
    """
    readme = TERRAFORM_README.read_text(encoding="utf-8")
    for topic in (
        "terraform apply",
        "terraform output",
        "cloudwatch_agent_config",
        "SQS_QUEUE_URL",
        "SNS_TOPIC_ARN",
        "redrive",
    ):
        assert topic in readme, f"terraform/README.md does not cover {topic}"
