import logging
from pathlib import Path

import click
import time

from etl.config import SOURCE_NAMES, boundaries_manifest, get_engine, sources_data_dir
from etl.extract import extract_boundaries, extract_source
from etl.ingestion import (
    Boto3S3Adapter,
    Boto3SNSAdapter,
    Boto3SQSAdapter,
    BootstrapConfig,
    PipelineProcessor,
    bootstrap as bootstrap_ingestion,
    redrive as redrive_versions,
    run_startup,
    run_worker,
)
from etl.load import load_generators, load_storages
from etl.marts import build_marts
from etl.source_data import SourceDataset, SourceValidationError, inspect_source_gpkg
from etl.transform import transform_sources


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging.")
def cli(verbose: bool):
    """Energy DE ETL pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=level)


def _client(service: str):
    import boto3

    return boto3.client(service)


def _s3_adapter() -> Boto3S3Adapter:
    return Boto3S3Adapter(_client("s3"))


def _sqs_adapter(queue_url: str) -> Boto3SQSAdapter:
    return Boto3SQSAdapter(_client("sqs"), queue_url)


def _sns_adapter(topic_arn: str) -> Boto3SNSAdapter:
    return Boto3SNSAdapter(_client("sns"), topic_arn)


def _echo_check_report(title: str, result) -> None:
    """The shared bootstrap report head: metadata, worker start, key checks."""
    click.echo(f"\n{title}:")
    click.echo(f"  Metadata ready   : {'yes' if result.metadata_ready else 'no'}")
    click.echo(
        f"  Worker start     : {'allowed' if result.worker_start_allowed else 'blocked'}"
    )
    for check in result.checks:
        requirement = "required" if check.required else "optional"
        click.echo(f"  {check.key} ({requirement}): {check.message}")


@cli.command()
@click.option("--bucket", envvar="S3_BUCKET", required=True, help="Versioned S3 data bucket.")
def bootstrap(bucket: str):
    result = bootstrap_ingestion(
        BootstrapConfig(bucket=bucket),
        engine=get_engine(),
        s3=_s3_adapter(),
    )
    _echo_check_report("Bootstrap report", result)
    if not result.worker_start_allowed:
        raise SystemExit(1)


@cli.command()
@click.option("--queue-url", envvar="SQS_QUEUE_URL", required=True, help="Queue carrying the S3 ObjectCreated events.")
@click.option("--topic-arn", envvar="SNS_TOPIC_ARN", required=True, help="Topic for the worker, blocked-startup and DLQ alerts.")
@click.option("--max-messages", type=int, default=None, help="Stop after this many messages (default: run until stopped).")
def worker(queue_url: str, topic_arn: str, max_messages: int | None):
    """Run the event-driven ingestion worker, one SQS message at a time.

    Each message is an ordered work unit: its Boundary records are applied as
    one release before its Source records, every record is processed
    independently, and the message is acknowledged only when all of them are
    successful or terminally skipped — anything retryable stays for SQS
    redelivery. The three marts are refreshed once per message. Run
    `python -m etl bootstrap` first: the worker expects the Boundary levels to
    be in place.
    """
    click.echo(f"\nIngestion worker reading {queue_url}")
    engine = get_engine()
    s3 = _s3_adapter()
    try:
        processed = run_worker(
            engine=engine,
            s3=s3,
            sqs=_sqs_adapter(queue_url),
            sns=_sns_adapter(topic_arn),
            processor=PipelineProcessor(engine, s3=s3),
            max_messages=max_messages,
        )
    except KeyboardInterrupt:
        click.echo("Worker stopped.")
        return
    click.echo(f"Worker processed {processed} message(s).")


@cli.command()
@click.option("--bucket", envvar="S3_BUCKET", required=True, help="Versioned S3 data bucket.")
@click.option("--queue-url", envvar="SQS_QUEUE_URL", required=True, help="Queue carrying the S3 ObjectCreated events.")
@click.option("--topic-arn", envvar="SNS_TOPIC_ARN", required=True, help="Topic for rejected-file, blocked-startup and DLQ alerts.")
def startup(bucket: str, queue_url: str, topic_arn: str):
    """Bootstrap the server deployment, then run the ingestion worker.

    The explicit startup path (issue #8): database preconditions and the fixed
    S3 key checks, the current Boundary releases applied with their downstream
    rebuild, and the current Source object versions enqueued — then the worker.
    A fatal Boundary condition (a missing or invalid release) refuses the
    worker start, so the container crash-loops until the operator fixes it.
    Because a crash loop is not a notification, the refusal is announced on the
    topic as well — once per distinct condition, not once per restart.
    """
    engine = get_engine()
    s3 = _s3_adapter()
    sns = _sns_adapter(topic_arn)
    processor = PipelineProcessor(engine, s3=s3)
    result = run_startup(
        BootstrapConfig(bucket=bucket),
        engine=engine,
        s3=s3,
        sqs=_sqs_adapter(queue_url),
        sns=sns,
        processor=processor,
    )

    _echo_check_report("Startup report", result)
    if result.release_results:
        applied = sorted(
            {
                str(detail)
                for stage in result.release_results
                for name, detail in stage.details.items()
                if name == "level"
            }
        )
        click.echo(f"  Boundary release : applied (levels {', '.join(applied)})")
    else:
        click.echo("  Boundary release : already current")
    click.echo(f"  Enqueued         : {len(result.enqueued)} source object version(s)")
    for object_id in result.enqueued:
        click.echo(f"    {object_id.key} ({object_id.version_id})")
    click.echo(f"  Already known    : {len(result.known)} source object version(s)")
    if not result.worker_start_allowed:
        # Only worth printing when it is news: on a healthy start there is
        # nothing to have alerted about. On a blocked one, "no" is the answer an
        # operator needs, because it means nobody was told except by the log.
        click.echo(f"  Alert published  : {'yes' if result.alerted else 'no'}")
        raise SystemExit(1)

    click.echo(f"\nIngestion worker reading {queue_url}")
    run_worker(
        engine=engine,
        s3=s3,
        sqs=_sqs_adapter(queue_url),
        sns=sns,
        processor=processor,
    )


@cli.command()
@click.option("--queue-url", envvar="SQS_QUEUE_URL", required=True, help="Queue the S3 ObjectCreated events are redriven to.")
@click.option("--key", default=None, help="Redrive only this object key (default: every failed or DLQ object version).")
def redrive(queue_url: str, key: str | None):
    """Re-enqueue failed or DLQ object versions for another attempt.

    The explicit recovery path (issue #8): every object version the worker left
    unsettled — a retryable failure, or a message the queue moved to the DLQ —
    is sent back to the queue, which resumes its run on the next delivery.
    Settled versions (succeeded, stale, terminal) are left alone.
    """
    redriven = redrive_versions(get_engine(), sqs=_sqs_adapter(queue_url), key=key)

    click.echo(f"\nRedriven {len(redriven)} object version(s):")
    for object_id in redriven:
        click.echo(f"  {object_id.key} ({object_id.version_id})")


@cli.command()
@click.argument("target", required=False)
@click.option("-f", "--force", is_flag=True, help="Reload boundaries whose load signature is already logged.")
def boundaries(target: str | None, force: bool):
    """Load boundary reference files listed in a manifest into service.boundaries.

    TARGET is the manifest file path (defaults to the configured boundaries
    manifest). Each file name on its own line is resolved against the
    manifest's directory; the first file replaces the table and the rest
    append, the level being read from each file's data. Files already logged
    in loaded_files are skipped unless --force is given. Area is then computed
    in km² via PostGIS.
    """
    manifest = Path(target) if target else boundaries_manifest()
    click.echo(f"\nLoading boundaries from {manifest}")
    start = time.perf_counter()

    if not manifest.is_file():
        raise click.BadParameter("Manifest not found!")

    report = extract_boundaries(manifest, force=force)

    click.echo("\nBoundaries report:")
    click.echo(report.summary())
    click.echo(f"\nLoading boundaries complete. Total time: {(time.perf_counter() - start):.2f} seconds.")

    if not report.passed:
        raise SystemExit(1)


@cli.command()
@click.argument("target", required=False)
@click.option("-f", "--force", is_flag=True, help="Reload files whose load signature is already logged.")
def extract(target: str | None, force: bool):
    """Extract unit sources found in a folder into versioned raw tables.

    TARGET is the sources folder (defaults to the configured sources folder).
    Each *.gpkg whose single layer is a valid unit-source snapshot is extracted
    in SOURCE_NAMES order, so discovery follows the file content rather than its
    name. Look-alikes such as Solar_Energy_Polygons and Cogeneration_Units fail
    snapshot validation and are logged and skipped. Files already logged in
    loaded_files are skipped unless --force is given.
    """
    log = logging.getLogger(__name__)

    click.echo("\nStart Extraction...")
    start = time.perf_counter()

    data_dir = Path(target) if target else sources_data_dir()
    if not data_dir.is_dir():
        raise click.BadParameter(f"Sources folder not found: {data_dir}")

    log.info("Extracting energy sources from %s", data_dir.name)
    by_source: dict[str, tuple[Path, SourceDataset]] = {}
    skipped: list[str] = []
    for f in sorted(data_dir.glob("*.gpkg")):
        try:
            dataset = inspect_source_gpkg(f)
        except SourceValidationError as error:
            log.info("Skipping %s: %s", f.name, error)
            skipped.append(f.name)
            continue
        if dataset.source in by_source:
            log.warning(
                "Multiple files map to source %s: keeping %s, ignoring %s",
                dataset.source, by_source[dataset.source][0].name, f.name,
            )
            continue
        by_source[dataset.source] = (f, dataset)
    if skipped:
        log.info("Skipping unrecognised file(s): %s", ", ".join(skipped))

    chosen = [by_source[s] for s in SOURCE_NAMES if s in by_source]
    log.info("Extracting %d source file(s) from %s", len(chosen), data_dir)

    reports = [
        extract_source(path, force=force, dataset=dataset)
        for path, dataset in chosen
    ]

    for r in reports:
        click.echo(f"\nExtraction report ({r.source or r.origin}):")
        click.echo(r.summary())
    click.echo(f"\nExtraction complete. Total time: {(time.perf_counter() - start):.2f} seconds.")

    if any(not r.passed for r in reports):
        raise SystemExit(1)


@cli.command()
@click.argument(
    "sources",
    nargs=-1,
    type=click.Choice(SOURCE_NAMES + ("all",)),
)
def transform(sources):
    """Transform SOURCES into their staging tables (default: all).

    SOURCES is one or more of the six unit sources, or "all" to transform
    every source.  With no arguments, all sources are transformed.  Builds
    the staging row (unit_id — natural from reference_id or synthetic where
    absent, canonical energy_source, geo_accuracy, country_iso, geometry),
    spatially joins boundaries to assign state/region/district, runs
    the quality gate, and decomposes the whitelisted secondary attributes
    into normalized properties (the rest staying in the reduced
    secondary_attributes json). A state-null row is not bad quality
    (spec v2.2). Storage staging additionally carries its storage shape
    (storage_type, storage_capacity).
    """

    if sources:
        click.echo(f"\nStart transforming for {' '.join(sources)}.")
    else:
        click.echo("\nRunning transform stage for all sources...")
    start = time.perf_counter()

    report = transform_sources(*sources) if sources else transform_sources()

    click.echo(f"\nTransform report ({report.source}):")
    click.echo(report.summary())
    click.echo(f"\nTransform complete. Total time: {(time.perf_counter() - start):.2f} seconds.")

    if not report.passed:
        raise SystemExit(1)


@cli.command()
def load():
    """Load consolidated generators and storages into core.

    Reads the good staging rows from all six sources and upserts generators
    into core.generators and storages into core.storages, each with a serial
    surrogate key, collision checks, and property-link annotation.  The
    staging tables are authoritative for the rows they carry, so the load is
    repeatable and never deletes a core unit.
    """
    click.echo("\nRunning load stage for generators and storages...")
    start = time.perf_counter()

    reports = [
        load_generators(),
        load_storages(),
    ]

    for report in reports:
        click.echo(f"\nLoad report ({report.target}):")
        click.echo(report.summary())
    click.echo(f"\nLoad complete. Total time: {(time.perf_counter() - start):.2f} seconds.")

    if any(not report.passed for report in reports):
        raise SystemExit(1)


@cli.command()
def marts():
    """Create, refresh, and verify the marts materialized views.

    Ensures the marts schema and the three stored pivots exist
    (installation_counts, generation_capacity, storage_capacity), refreshes
    them from core at state grain (active units only, state-null units
    under the "outside" bucket), and verifies the stored pivots reconcile to
    the live core aggregates — failing loudly and exiting non-zero on drift.
    """
    click.echo("\nStart creating marts materialized views...")
    report = build_marts()

    click.echo(f"\nMarts report:")
    click.echo(report.summary())

    if not report.passed:
        raise SystemExit(1)


@cli.command()
@click.option("-f", "--force", is_flag=True, help="Reload files whose load signature is already logged.")
def run_all(force: bool):
    """Run all ETL stages"""
    click.echo("\n==== Run all ETL stages ====")
    start = time.perf_counter()
    ctx = click.get_current_context()

    ctx.invoke(boundaries, force=force)

    ctx.invoke(extract, force=force)

    ctx.invoke(transform)

    ctx.invoke(load)

    ctx.invoke(marts)

    click.echo(f"\n==== All stages complete. Total time: {(time.perf_counter() - start):.2f} seconds.")


if __name__ == "__main__":
    cli()