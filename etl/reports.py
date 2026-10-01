from __future__ import annotations

from dataclasses import dataclass, field


class ReportBase:
    """Shared shape for stage reports: error list, pass flag, and error lines."""

    errors: list[str]

    @property
    def passed(self) -> bool:
        """True if the report completed without errors (a skip is also a pass)."""
        return not self.errors

    def _error_lines(self) -> list[str]:
        return [f"  ERROR: {e}" for e in self.errors]


@dataclass
class BoundariesReport(ReportBase):
    """Result of the boundary reference-data load."""

    loaded: bool = False
    skipped: bool = False
    rows_by_level: dict[int, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"  Loaded           : {'yes' if self.loaded else 'no (already present or error)'}",
            "  Rows by level    : " + (", ".join(f"{k}={v}" for k, v in sorted(self.rows_by_level.items())) or "-"),
            f"  Status           : {'SKIPPED (already loaded)' if self.skipped else ('PASS' if self.passed else 'FAIL')}",
        ]
        lines.extend(self._error_lines())
        return "\n".join(lines)


@dataclass
class ExtractionReport(ReportBase):
    """Result of one unit source extract: counts, version and verification outcome."""

    source: str = ""
    origin: str = ""
    source_row_count: int = 0
    rows_loaded: int = 0
    attributes_empty: int = 0
    loaded_to: str | None = None
    skipped: bool = False
    errors: list[str] = field(default_factory=list)
    total_time: float = 0.0

    def summary(self) -> str:
        lines = [
            f"  Origin           : {self.origin or '-'}",
            f"  Source file rows : {self.source_row_count}",
            f"  Rows loaded      : {self.rows_loaded}",
            f"  Empty attributes  : {self.attributes_empty}",
            f"  Loaded to        : {self.loaded_to or '-'}",
            f"  Status           : {'SKIPPED (already loaded)' if self.skipped else ('PASS' if self.passed else 'FAIL')}",
            f"  Total time       : {self.total_time:.3f}s",
        ]
        lines.extend(self._error_lines())
        return "\n".join(lines)


@dataclass
class TransformReport(ReportBase):
    """Result of one source's transform into its staging tables."""

    source: str = ""
    raw_table: str | None = None
    rows_read: int = 0
    rows_written: int = 0
    synthetic_ids: int = 0
    join_unmapped: dict[str, int] = field(default_factory=dict)
    bad_quality: int = 0
    quality_reasons: dict[str, int] = field(default_factory=dict)
    properties_count: int = 0
    links_count: int = 0
    errors: list[str] = field(default_factory=list)
    total_time: float = 0.0

    def summary(self) -> str:
        lines = [
            f"  Raw table        : {self.raw_table or '-'}",
            f"  Rows read        : {self.rows_read}",
            f"  Rows written     : {self.rows_written}",
            f"  Synthetic ids    : {self.synthetic_ids}",
            "  Join unmapped    : "
            + (", ".join(f"{k}={v}" for k, v in self.join_unmapped.items()) or "-"),
            f"  Bad quality      : {self.bad_quality}",
            "  Quality reasons  : "
            + (", ".join(f"{k}={v}" for k, v in self.quality_reasons.items()) or "-"),
            f"  Properties       : {self.properties_count}",
            f"  Links            : {self.links_count}",
            f"  Status           : {'PASS' if self.passed else 'FAIL'}",
            f"  Total time       : {self.total_time:.3f}s",
        ]
        lines.extend(self._error_lines())
        return "\n".join(lines)


@dataclass
class LoadReport(ReportBase):
    """Result of loading consolidated units into core."""

    target: str = ""
    rows_read: int = 0
    bad_rows_dropped: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_retained: int = 0
    collisions: int = 0
    collision_links: int = 0
    links_count: int = 0
    properties_count: int = 0
    idempotent: bool = True
    errors: list[str] = field(default_factory=list)
    total_time: float = 0.0

    def summary(self) -> str:
        lines = [
            f"  Core table       : {self.target or '-'}",
            f"  Good rows read   : {self.rows_read}",
            f"  Bad rows dropped : {self.bad_rows_dropped}",
            f"  Rows inserted    : {self.rows_inserted}",
            f"  Rows updated     : {self.rows_updated}",
            f"  Rows retained    : {self.rows_retained}",
            f"  Collisions       : {self.collisions}",
            f"  Collision links  : {self.collision_links}",
            f"  Properties       : {self.properties_count}",
            f"  Links            : {self.links_count}",
            f"  Idempotency      : {'PASS' if self.idempotent else 'FAIL'}",
            f"  Status           : {'PASS' if self.passed else 'FAIL'}",
            f"  Total time       : {self.total_time:.3f}s",
        ]
        lines.extend(self._error_lines())
        return "\n".join(lines)


@dataclass
class MartsReport(ReportBase):
    """Result of building (creating/refreshing/verifying) the marts views."""

    created: list[str] = field(default_factory=list)
    recreated: list[str] = field(default_factory=list)
    refreshed: list[str] = field(default_factory=list)
    refresh_times: dict[str, float] = field(default_factory=dict)
    verified: bool = False
    errors: list[str] = field(default_factory=list)
    total_time: float = 0.0

    def summary(self) -> str:
        lines = [
            "  Created          : " + (", ".join(self.created) or "none"),
            "  Recreated        : " + (", ".join(self.recreated) or "none"),
            "  Refreshed        : " + (", ".join(self.refreshed) or "none"),
            "  Refresh times    : "
            + (
                ", ".join(f"{k}: {t:.3f}s" for k, t in self.refresh_times.items())
                or "-"
            ),
            f"  Verified         : {'yes' if self.verified else 'no'}",
            f"  Status           : {'PASS' if self.passed else 'FAIL'}",
            f"  Total time       : {self.total_time:.3f}s",
        ]
        lines.extend(self._error_lines())
        return "\n".join(lines)