from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas
import pyogrio

from etl.config import SOURCE_NAMES, SYNTHETIC_ID_PREFIX


class SourceValidationError(ValueError):
    pass


ENERGY_SOURCE_LABELS = {
    "bio": ("Bioenergy",),
    "gas": ("Gas",),
    "hydro": ("Hydropower",),
    "solar": ("Solar Energy", "Solar energy"),
    "wind": ("Wind Energy",),
    "storage": ("Energy Storage",),
}

_SOURCE_BY_LABEL = {
    label.casefold(): source
    for source, labels in ENERGY_SOURCE_LABELS.items()
    for label in labels
}

_COMMON_COLUMNS = {
    "energy_source",
    "commissioning_date",
    "decommissioning_date",
    "x_coordinates",
    "y_coordinates",
    "geo_accuracy",
    "reference_id",
    "reference_date",
    "geometry",
}

_CAPACITY_COLUMNS = {
    source: {"gas_production_capacity"} if source == "gas" else {"installed_capacity"}
    for source in SOURCE_NAMES
}

SOURCE_REQUIRED_COLUMNS = {
    source: _COMMON_COLUMNS | _CAPACITY_COLUMNS[source]
    | ({"storage_type", "storage_capacity"} if source == "storage" else set())
    for source in SOURCE_NAMES
}


@dataclass(frozen=True)
class SourceDataset:
    source: str
    data: gpd.GeoDataFrame
    unit_ids: pandas.Series


def inspect_source_gpkg(path: Path) -> SourceDataset:
    layers = pyogrio.list_layers(path)
    if len(layers) != 1:
        raise SourceValidationError(
            f"Source GPKG must contain exactly one layer, found {len(layers)}"
        )

    layer_name = str(layers[0][0])
    data = gpd.read_file(path, layer=layer_name)
    source = _canonical_source(data)
    _validate_schema(data, source)
    _validate_geometry(data)
    data = data.copy()
    data["energy_source"] = source
    data["reference_id"] = _normalized_reference_ids(data["reference_id"])
    _validate_reference_ids(data["reference_id"])
    unit_ids = _unit_ids(data, source)
    return SourceDataset(source=source, data=data, unit_ids=unit_ids)


def _canonical_source(data: gpd.GeoDataFrame) -> str:
    if "energy_source" not in data.columns:
        raise SourceValidationError("Source GPKG missing required column: energy_source")

    labels = data["energy_source"]
    if labels.isna().any():
        raise SourceValidationError("Source GPKG requires non-null homogeneous Energy source")
    normalized = labels.astype("string").str.strip()
    if normalized.eq("").any():
        raise SourceValidationError("Source GPKG requires non-null homogeneous Energy source")

    folded = normalized.str.casefold()
    unknown = sorted(set(folded[~folded.isin(_SOURCE_BY_LABEL)]))
    if unknown:
        raise SourceValidationError(f"unknown Energy source label(s): {unknown}")

    canonical = folded.map(_SOURCE_BY_LABEL)
    if canonical.nunique(dropna=False) != 1:
        raise SourceValidationError("Source GPKG requires homogeneous Energy source")
    return str(canonical.iloc[0])


def _validate_schema(data: gpd.GeoDataFrame, source: str) -> None:
    missing = sorted(SOURCE_REQUIRED_COLUMNS[source] - set(data.columns))
    if missing:
        raise SourceValidationError(
            f"Source GPKG missing required columns: {', '.join(missing)}"
        )


def _validate_geometry(data: gpd.GeoDataFrame) -> None:
    if data.geometry.isna().any() or not data.geometry.geom_type.eq("Point").all():
        raise SourceValidationError("Source GPKG rows must use non-null Point geometry")
    if data.crs is None or data.crs.to_epsg() != 4326:
        raise SourceValidationError("Source GPKG Point geometry must use EPSG:4326")


def _normalized_reference_ids(values: pandas.Series) -> pandas.Series:
    return values.astype("string").str.strip().replace("", pandas.NA)


def _validate_reference_ids(reference_ids: pandas.Series) -> None:
    # Null Reference IDs are not identities, so only the non-null ones can
    # collide; the mask is taken on that subset and must index that subset.
    present = reference_ids[reference_ids.notna()]
    duplicated = present.duplicated(keep=False)
    if duplicated.any():
        duplicates = sorted(present[duplicated].astype(str).unique())
        raise SourceValidationError(
            f"Duplicate non-null Reference IDs in Source snapshot: {duplicates}"
        )


def _unit_ids(data: gpd.GeoDataFrame, source: str) -> pandas.Series:
    if (~data["reference_id"].isna()).all():
        return source + "_" + data["reference_id"]
    if "location" not in data.columns:
        raise SourceValidationError(
            "Null Reference IDs require a location column for synthetic identity"
        )
    return synthetic_unit_ids(
        source=source,
        reference_ids=data["reference_id"],
        locations=data["location"],
        x=data["x_coordinates"],
        y=data["y_coordinates"],
        geo_accuracy=data["geo_accuracy"],
    )


def synthetic_unit_ids(
    *,
    source: str,
    reference_ids: pandas.Series,
    locations: pandas.Series,
    x: pandas.Series,
    y: pandas.Series,
    geo_accuracy: pandas.Series,
) -> pandas.Series:
    """Derive the staging Unit ID for every row of a Source snapshot.

    A non-null Reference ID gives ``<source>_<reference_id>``. A null one falls
    back to the synthetic identity, which depends only on attributes that
    describe *where* the unit is, so a unit keeps its identity when its
    capacity or dates change (ADR 0001). Two rows that resolve to the same
    identity make the snapshot ambiguous and fail it.
    """
    unit_ids = pandas.Series(index=reference_ids.index, dtype="string")
    has_reference = reference_ids.notna()
    unit_ids.loc[has_reference] = source + "_" + reference_ids[has_reference]
    if (~has_reference).any():
        unit_ids.loc[~has_reference] = [
            synthetic_identity(
                source=source,
                location=location,
                x=x_coordinates,
                y=y_coordinates,
                geo_accuracy=accuracy,
            )
            for location, x_coordinates, y_coordinates, accuracy in zip(
                locations[~has_reference],
                x[~has_reference],
                y[~has_reference],
                geo_accuracy[~has_reference],
            )
        ]

    duplicated = unit_ids.duplicated(keep=False)
    for mask, message in (
        (~has_reference, "Synthetic identity collision in Source snapshot"),
        (has_reference, "Duplicate unit identity in Source snapshot"),
    ):
        selected = duplicated & mask
        if bool(selected.any()):
            values = sorted(unit_ids[selected].astype(str).unique())
            raise SourceValidationError(f"{message}: {values}")
    return unit_ids


def synthetic_identity(*, source: str, location, x, y, geo_accuracy) -> str:
    payload = "|".join(
        _stable_value(value)
        for value in (source, location, round(float(x), 6), round(float(y), 6), geo_accuracy)
    )
    digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return SYNTHETIC_ID_PREFIX + digest


def _stable_value(value) -> str:
    if value is None or value is pandas.NA or value is pandas.NaT:
        return "<null>"
    if isinstance(value, (bool, str)):
        return value.strip() if isinstance(value, str) else str(value)
    if isinstance(value, (int, float)):
        if pandas.isna(value):
            return "<null>"
        return f"{float(value):.6f}"
    if hasattr(value, "item") and not isinstance(value, (bytes, bytearray)):
        return _stable_value(value.item())
    return str(value).strip()
