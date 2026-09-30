"""Integration tests for the core.storages load (issue #7).

Run against the live dev PostGIS (`DATABASE_URL`).  core.storages is loaded
once per module via a shared fixture — read-only tests query that shared
state; the idempotency and incremental-update tests manage their own core
lifecycle because they need fresh loads.
"""

import os

import pytest
from sqlalchemy import create_engine, text

from etl.config import (
    BAD_QUALITY_PROPERTY,
    COLLISION_PROPERTY,
    CORE_SCHEMA,
    DECOMPOSED_PROPERTIES,
    SEA_REGIONS,
    STAGING_SCHEMA,
    STORAGE_CAPACITY_COLLISION_REASON,
)
from etl.load import load_storages

ENGINE = create_engine(os.environ["DATABASE_URL"])

SOURCE = "storage"


def _scalar(sql: str) -> int:
    with ENGINE.connect() as conn:
        return int(conn.execute(text(sql)).scalar())


def _drop_core():
    """Drop core tables so each test/module starts clean."""
    with ENGINE.begin() as conn:
        for tbl in (
            "storage_units_properties",
            "storage_properties",
            "storages",
        ):
            conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.{tbl} CASCADE"))


@pytest.fixture(scope="module")
def _loaded_core():
    """Load storage into core.storages once per module."""
    _drop_core()
    report = load_storages()
    assert report.passed, report.errors
    yield report
    _drop_core()


@pytest.fixture(scope="module", autouse=True)
def _staging_ready(_staged_sources):
    """Staging is transformed once per session; nothing to do here."""


# ------------------------------------------------------------------ #
#  Table shape                                                         #
# ------------------------------------------------------------------ #


class TestTableShape:
    def test_storages_exists(self, _loaded_core):
        n = _scalar(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{CORE_SCHEMA}' AND table_name = 'storages'"
        )
        assert n == 1

    def test_storages_columns(self, _loaded_core):
        with ENGINE.connect() as conn:
            cols = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        f"WHERE table_schema = '{CORE_SCHEMA}' AND table_name = 'storages'"
                    )
                )
            }
        expected = {
            "unit_id",
            "collision",
            "storage_type",
            "storage_capacity",
            "secondary_attributes",
            "geometry",
            "longitude",
            "latitude",
            "energy_source",
            "installed_capacity",
            "commissioning_date",
            "decommissioning_date",
            "geo_accuracy",
            "reference_id",
            "reference_date",
            "country_iso",
            "state",
            "region",
            "district",
        }
        assert expected.issubset(cols), f"Missing columns: {expected - cols}"

    def test_storages_unit_id_is_serial(self, _loaded_core):
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    f"WHERE table_schema = '{CORE_SCHEMA}' AND table_name = 'storages' "
                    "AND column_name = 'unit_id'"
                )
            ).fetchone()
        assert row is not None
        assert row[0] == "integer"

    def test_storages_storage_capacity_is_double(self, _loaded_core):
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    f"WHERE table_schema = '{CORE_SCHEMA}' AND table_name = 'storages' "
                    "AND column_name = 'storage_capacity'"
                )
            ).fetchone()
        assert row is not None
        assert row[0] == "double precision"

    def test_storages_collision_default_false(self, _loaded_core):
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT column_default FROM information_schema.columns "
                    f"WHERE table_schema = '{CORE_SCHEMA}' "
                    "AND table_name = 'storages' AND column_name = 'collision'"
                )
            ).fetchone()
        assert row is not None
        assert row[0] == "false"

    def test_storages_geometry_point_4326(self, _loaded_core):
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT srid, type FROM geometry_columns "
                    f"WHERE f_table_schema = '{CORE_SCHEMA}' AND f_table_name = 'storages'"
                )
            ).fetchone()
        assert row is not None
        assert row[0] == 4326
        assert row[1] == "POINT"

    def test_storages_has_longitude_latitude(self, _loaded_core):
        with ENGINE.connect() as conn:
            for col in ("longitude", "latitude"):
                row = conn.execute(
                    text(
                        "SELECT data_type FROM information_schema.columns "
                        f"WHERE table_schema = '{CORE_SCHEMA}' AND table_name = 'storages' "
                        f"AND column_name = '{col}'"
                    )
                ).fetchone()
                assert row is not None, f"Missing column {col}"


# ------------------------------------------------------------------ #
#  First load — row counts and uniqueness                              #
# ------------------------------------------------------------------ #


class TestFirstLoad:
    def test_first_load_row_count(self, _loaded_core):
        """Only bad_quality=false staging rows land in core."""
        good_staging = _scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{SOURCE} WHERE NOT bad_quality"
        )
        core_count = _scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages")
        assert core_count == good_staging
        assert _loaded_core.rows_inserted == good_staging

    def test_no_duplicate_reference_id_pairs(self, _loaded_core):
        dups = _scalar(
            f"SELECT COUNT(*) FROM ("
            f"SELECT energy_source, reference_id FROM {CORE_SCHEMA}.storages "
            f"WHERE reference_id IS NOT NULL "
            f"GROUP BY energy_source, reference_id HAVING COUNT(*) > 1) d"
        )
        assert dups == 0

    def test_energy_sources_covered(self, _loaded_core):
        with ENGINE.connect() as conn:
            sources = sorted(
                row[0]
                for row in conn.execute(
                    text(
                        f"SELECT DISTINCT energy_source FROM {CORE_SCHEMA}.storages "
                        "ORDER BY 1"
                    )
                ).fetchall()
            )
        assert sources == [SOURCE]


# ------------------------------------------------------------------ #
#  Idempotency — running twice gives the same result                   #
# ------------------------------------------------------------------ #


class TestIdempotency:
    def test_load_twice_same_counts(self):
        _drop_core()
        r1 = load_storages()
        count_after_first = _scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages")
        assert count_after_first == r1.rows_inserted

        r2 = load_storages()
        count_after_second = _scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages")
        assert count_after_second == count_after_first
        assert r2.rows_inserted == 0
        assert r2.rows_updated == count_after_first
        assert r2.rows_retained == 0
        assert r1.idempotent and r2.idempotent


# ------------------------------------------------------------------ #
#  Snapshot authority — present rows always update, absent rows stay      #
# ------------------------------------------------------------------ #


class TestSnapshotAuthority:
    def test_fresher_row_updates_in_place(self):
        load_storages()
        # Pick an existing unit with a reference_id
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT unit_id, energy_source, reference_id, reference_date "
                    f"FROM {CORE_SCHEMA}.storages "
                    f"WHERE reference_id IS NOT NULL LIMIT 1"
                )
            ).fetchone()
        assert row is not None
        core_unit_id, energy_source, reference_id, old_ref_date = row

        # Update staging to have a fresher reference_date for this unit
        import datetime

        new_ref_date = datetime.datetime(2999, 1, 1, 0, 0, 0)
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {STAGING_SCHEMA}.{energy_source} "
                    f"SET reference_date = :new_date "
                    f"WHERE reference_id = :ref_id"
                ),
                {"new_date": new_ref_date, "ref_id": reference_id},
            )

        load_storages()

        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT unit_id, reference_date "
                    f"FROM {CORE_SCHEMA}.storages "
                    f"WHERE reference_id = :ref_id"
                ),
                {"ref_id": reference_id},
            ).fetchone()
        assert row is not None
        assert row[0] == core_unit_id  # same unit_id
        assert row[1] >= new_ref_date  # reference_date refreshed

        # Restore staging
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {STAGING_SCHEMA}.{energy_source} "
                    f"SET reference_date = :old_date "
                    f"WHERE reference_id = :ref_id"
                ),
                {"old_date": old_ref_date, "ref_id": reference_id},
            )

    def test_present_row_updates_even_with_older_date(self):
        load_storages()
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT unit_id, energy_source, reference_id, reference_date "
                    f"FROM {CORE_SCHEMA}.storages "
                    f"WHERE reference_id IS NOT NULL LIMIT 1"
                )
            ).fetchone()
        assert row is not None
        core_unit_id, energy_source, reference_id, old_ref_date = row

        # Set staging to a much older reference_date
        import datetime

        stale_date = datetime.datetime(1900, 1, 1, 0, 0, 0)
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {STAGING_SCHEMA}.{energy_source} "
                    f"SET reference_date = :stale_date "
                    f"WHERE reference_id = :ref_id"
                ),
                {"stale_date": stale_date, "ref_id": reference_id},
            )

        load_storages()

        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT unit_id, reference_date "
                    f"FROM {CORE_SCHEMA}.storages "
                    f"WHERE reference_id = :ref_id"
                ),
                {"ref_id": reference_id},
            ).fetchone()
        assert row is not None
        assert row[0] == core_unit_id
        # A complete Source snapshot is authoritative for the rows it carries,
        # so an older Reference Date does not shield the Core row from update.
        assert row[1] == stale_date

        # Restore staging
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {STAGING_SCHEMA}.{energy_source} "
                    f"SET reference_date = :old_date "
                    f"WHERE reference_id = :ref_id"
                ),
                {"old_date": old_ref_date, "ref_id": reference_id},
            )


# ------------------------------------------------------------------ #
#  Collision detection                                                  #
# ------------------------------------------------------------------ #


class TestCollisions:
    def test_state_null_flagged(self, _loaded_core):
        state_null_collisions = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages "
            f"WHERE state IS NULL AND collision"
        )
        staging_state_null = _scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{SOURCE} "
            f"WHERE NOT bad_quality AND state IS NULL"
        )
        assert state_null_collisions == staging_state_null

    def test_storage_capacity_collision_flagged(self, _loaded_core):
        bad_capacity = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages "
            f"WHERE (storage_capacity IS NULL OR storage_capacity <= 0) AND collision"
        )
        staging_bad_capacity = _scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{SOURCE} "
            f"WHERE NOT bad_quality "
            f"AND (storage_capacity IS NULL OR storage_capacity <= 0)"
        )
        assert bad_capacity == staging_bad_capacity

    def test_storage_capacity_rule_actually_fires(self, _loaded_core):
        """Absolute count, unlike the reconciliation above.

        `test_storage_capacity_collision_flagged` compares core against staging,
        so it passes just as happily when no storage trips the rule at all. The
        fixture deliberately carries one unit with storage_capacity 0 and a
        positive installed capacity, so pin that it is still there.
        """
        # The property dimension is UNIQUE on (name, value), so this counts the
        # distinct reason string, not the units carrying it.
        assert _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_properties "
            f"WHERE name = '{COLLISION_PROPERTY}' "
            f"AND value LIKE '%{STORAGE_CAPACITY_COLLISION_REASON}%'"
        ) == 1

    def test_onshore_in_sea_flagged(self, _loaded_core):
        sea_states = ", ".join(f"'{region}'" for region in SEA_REGIONS)
        onshore_sea = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages "
            f"WHERE state IN ({sea_states}) AND collision"
        )
        expected = _scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{SOURCE} "
            f"WHERE NOT bad_quality AND state IN ({sea_states})"
        )
        assert onshore_sea == expected

    def test_collision_property_links_exist(self, _loaded_core):
        collision_links = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages g "
            f"JOIN {CORE_SCHEMA}.storage_units_properties gp ON gp.unit_id = g.unit_id "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = gp.prop_id "
            f"WHERE g.collision AND p.name = 'collision'"
        )
        collision_rows = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages WHERE collision"
        )
        assert collision_links == collision_rows

    def test_close_to_neighbours_in_secondary_attributes(self, _loaded_core):
        # close_to neighbours live in secondary_attributes, not property links.
        close_to_links = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages g "
            f"JOIN {CORE_SCHEMA}.storage_units_properties gp ON gp.unit_id = g.unit_id "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = gp.prop_id "
            f"WHERE p.name = 'close_to'"
        )
        assert close_to_links == 0

        # Collision values carry the phrase, never a unit id.
        unit_id_reasons = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_properties "
            f"WHERE name = 'collision' AND value LIKE '%close_to %'"
        )
        assert unit_id_reasons == 0

        close_units = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages "
            f"WHERE secondary_attributes IS NOT NULL "
            f"AND secondary_attributes::jsonb ? 'close_to'"
        )
        close_phrase = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_units_properties gp "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = gp.prop_id "
            f"WHERE p.name = 'collision' AND p.value LIKE '%close location%'"
        )
        assert close_phrase == close_units

        # Every close_to mention is reciprocated by the neighbour unit.
        asymmetric = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages a, "
            f"jsonb_array_elements("
            f"a.secondary_attributes::jsonb -> 'close_to') nb "
            f"WHERE a.secondary_attributes IS NOT NULL "
            f"AND a.secondary_attributes::jsonb ? 'close_to' "
            f"AND NOT EXISTS ("
            f"SELECT 1 FROM {CORE_SCHEMA}.storages b "
            f"WHERE b.unit_id = (nb #>> '{{}}')::int "
            f"AND b.secondary_attributes IS NOT NULL "
            f"AND b.secondary_attributes::jsonb ? 'close_to' "
            f"AND (b.secondary_attributes::jsonb -> 'close_to') "
            f"@> to_jsonb(a.unit_id)"
            f")"
        )
        assert asymmetric == 0


# ------------------------------------------------------------------ #
#  Verification — counts reconcile                                      #
# ------------------------------------------------------------------ #


class TestVerification:
    def test_storage_count_reconciles(self, _loaded_core):
        good_staging = _scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{SOURCE} WHERE NOT bad_quality"
        )
        core_count = _scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages")
        assert core_count == good_staging

    def test_capacity_reconciles(self, _loaded_core):
        staging_cap = _scalar(
            f"SELECT COALESCE(SUM(storage_capacity),0) "
            f"FROM {STAGING_SCHEMA}.{SOURCE} WHERE NOT bad_quality"
        )
        core_cap = _scalar(
            f"SELECT SUM(storage_capacity) FROM {CORE_SCHEMA}.storages"
        )
        assert abs(core_cap - staging_cap) < 0.01

    def test_installed_capacity_reconciles(self, _loaded_core):
        staging_cap = _scalar(
            f"SELECT COALESCE(SUM(installed_capacity),0) "
            f"FROM {STAGING_SCHEMA}.{SOURCE} WHERE NOT bad_quality"
        )
        core_cap = _scalar(
            f"SELECT SUM(installed_capacity) FROM {CORE_SCHEMA}.storages"
        )
        assert abs(core_cap - staging_cap) < 0.01


# ------------------------------------------------------------------ #
#  Normalized property transfer (ADR 0006 / issue #8 pattern)          #
# ------------------------------------------------------------------ #


class TestPropertyTransfer:
    def _whitelist_sql(self) -> str:
        return ", ".join(f"'{n}'" for n in DECOMPOSED_PROPERTIES)

    def test_core_properties_cover_good_staging(self, _loaded_core):
        """Every whitelist (name, value) on a good staging row is in core."""
        staged = set()
        with ENGINE.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT DISTINCT p.name, p.value "
                    f"FROM {STAGING_SCHEMA}.{SOURCE}_units_properties up "
                    f"JOIN {STAGING_SCHEMA}.{SOURCE}_properties p ON p.param_id = up.param_id "
                    f"JOIN {STAGING_SCHEMA}.{SOURCE} u ON u.unit_id = up.unit_id "
                    f"WHERE NOT u.bad_quality"
                )
            ).fetchall()
            staged.update((n, v) for n, v in rows)
        staged = {(n, v) for n, v in staged if n != BAD_QUALITY_PROPERTY}

        with ENGINE.connect() as conn:
            core_rows = set(
                conn.execute(
                    text(
                        f"SELECT name, value FROM {CORE_SCHEMA}.storage_properties "
                        f"WHERE name IN ({self._whitelist_sql()})"
                    )
                ).fetchall()
            )
        assert staged - core_rows == set()

    def test_core_links_match_good_staging_links(self, _loaded_core):
        """Whitelist links in core equal the whitelist links of good staging rows."""
        with ENGINE.connect() as conn:
            staging_links = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{SOURCE}_units_properties up "
                    f"JOIN {STAGING_SCHEMA}.{SOURCE}_properties p ON p.param_id = up.param_id "
                    f"JOIN {STAGING_SCHEMA}.{SOURCE} u ON u.unit_id = up.unit_id "
                    f"WHERE NOT u.bad_quality AND p.name IN ({self._whitelist_sql()})"
                )
            ).scalar()
            core_links = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_units_properties up "
                    f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = up.prop_id "
                    f"WHERE p.name IN ({self._whitelist_sql()})"
                )
            ).scalar()
        assert int(core_links) == int(staging_links)

    def test_bad_quality_absent_from_core(self, _loaded_core):
        n = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_properties "
            f"WHERE name = '{BAD_QUALITY_PROPERTY}' "
            f"LIMIT 1"
        )
        assert n == 0

    def test_no_orphaned_links(self, _loaded_core):
        orphans = _scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_units_properties up "
            f"LEFT JOIN {CORE_SCHEMA}.storages g ON g.unit_id = up.unit_id "
            f"WHERE g.unit_id IS NULL"
        )
        assert orphans == 0

    def test_updated_unit_links_refresh(self):
        """An in-place update rewrites the unit's whitelist links (ADR 0001)."""
        load_storages()
        # Pick a core unit with a reference_id and a refreshed staging row
        with ENGINE.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT g.unit_id, g.energy_source, g.reference_id "
                    f"FROM {CORE_SCHEMA}.storages g "
                    f"JOIN {CORE_SCHEMA}.storage_units_properties up ON up.unit_id = g.unit_id "
                    f"WHERE g.reference_id IS NOT NULL "
                    f"LIMIT 1"
                )
            ).fetchone()
        assert row is not None
        core_unit_id, energy_source, reference_id = row

        import datetime

        new_ref_date = datetime.datetime(2999, 1, 1, 0, 0, 0)
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {STAGING_SCHEMA}.{energy_source} "
                    f"SET reference_date = :d WHERE reference_id = :r"
                ),
                {"d": new_ref_date, "r": reference_id},
            )
        load_storages()

        # The unit must now carry every whitelist link its staging row has.
        staged_pairs = set()
        core_pairs = set()
        whitelist = self._whitelist_sql()
        with ENGINE.connect() as conn:
            staged = conn.execute(
                text(
                    f"SELECT p.name, p.value "
                    f"FROM {STAGING_SCHEMA}.{energy_source}_units_properties up "
                    f"JOIN {STAGING_SCHEMA}.{energy_source}_properties p ON p.param_id = up.param_id "
                    f"JOIN {STAGING_SCHEMA}.{energy_source} u ON u.unit_id = up.unit_id "
                    f"WHERE u.reference_id = :r AND p.name IN ({whitelist})"
                ),
                {"r": reference_id},
            ).fetchall()
            staged_pairs = {(n, v) for n, v in staged}
            core = conn.execute(
                text(
                    f"SELECT p.name, p.value "
                    f"FROM {CORE_SCHEMA}.storage_units_properties up "
                    f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = up.prop_id "
                    f"WHERE up.unit_id = :uid AND p.name IN ({whitelist})"
                ),
                {"uid": core_unit_id},
            ).fetchall()
            core_pairs = {(n, v) for n, v in core}
            old_date = conn.execute(
                text(
                    f"SELECT reference_date FROM {STAGING_SCHEMA}.{energy_source} "
                    f"WHERE reference_id = :r"
                ),
                {"r": reference_id},
            ).scalar()
        assert staged_pairs == core_pairs

        # Restore staging and re-load to reset core state.
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    f"UPDATE {STAGING_SCHEMA}.{energy_source} "
                    f"SET reference_date = :d WHERE reference_id = :r"
                ),
                {"d": old_date, "r": reference_id},
            )
        load_storages()