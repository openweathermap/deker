"""Tests of the public types: the integrity finding and report."""

from pathlib import Path

import pytest

from deker.types import IntegrityCheck, IntegrityFinding, IntegrityFindingKind, IntegrityReport


class TestIntegrityReport:
    """The report: findings, the summary properties and the text."""

    @pytest.fixture
    def findings(self) -> list[IntegrityFinding]:
        return [
            IntegrityFinding(
                check=IntegrityCheck.collections,
                kind=IntegrityFindingKind.collection_unreadable,
                message="Directory half has no collection file",
                path=Path("/storage/half/half.json"),
                repairable=True,
            ),
            IntegrityFinding(
                check=IntegrityCheck.links,
                kind=IntegrityFindingKind.symlink_missing,
                message="Symlink of Array a1 not found",
                collection="weather",
                array_id="a1",
                path=Path("/storage/weather/array_symlinks/1/a1.hdf5"),
                repairable=True,
                repaired=True,
            ),
            IntegrityFinding(
                check=IntegrityCheck.links,
                kind=IntegrityFindingKind.duplicate_arrays,
                message="Directory holds several arrays",
                collection="weather",
                path=Path("/storage/weather/array_symlinks/2"),
            ),
            IntegrityFinding(
                check=IntegrityCheck.data,
                kind=IntegrityFindingKind.data_unreadable,
                message="Array a3 data is corrupted",
                collection="climate",
                array_id="a3",
                repairable=False,
            ),
        ]

    def test_empty_report_is_ok(self):
        report = IntegrityReport()
        assert report.ok
        assert report.findings == []
        assert report.unrepaired == []
        assert str(report) == ""

    def test_report_with_findings_is_not_ok(self, findings: list[IntegrityFinding]):
        report = IntegrityReport(findings)
        assert not report.ok
        assert report.findings == findings

    def test_by_check(self, findings: list[IntegrityFinding]):
        report = IntegrityReport(findings)
        assert report.by_check(IntegrityCheck.links) == findings[1:3]
        assert report.by_check(IntegrityCheck.counters) == []

    def test_unrepaired_leaves_out_the_repaired_findings_only(self, findings: list[IntegrityFinding]):
        report = IntegrityReport(findings)
        assert report.unrepaired == [findings[0], findings[2], findings[3]]

    def test_text_groups_by_collection_and_check(self, findings: list[IntegrityFinding]):
        report = IntegrityReport(findings)
        assert str(report) == (
            "Storage, collections:\n"
            "\t- Directory half has no collection file\n"
            "Collection weather, links:\n"
            "\t- Symlink of Array a1 not found (repaired)\n"
            "\t- Directory holds several arrays\n"
            "Collection climate, data:\n"
            "\t- Array a3 data is corrupted\n"
        )

    def test_text_tells_a_failed_repair(self):
        finding = IntegrityFinding(
            check=IntegrityCheck.markers,
            kind=IntegrityFindingKind.marker_left,
            message="Marker x is left",
            collection="weather",
            repairable=True,
            repair_error="Permission denied",
        )
        assert (
            str(IntegrityReport([finding]))
            == "Collection weather, markers:\n\t- Marker x is left (repair failed: Permission denied)\n"
        )
        assert IntegrityReport([finding]).unrepaired == [finding]

    def test_finding_defaults(self):
        finding = IntegrityFinding(check=IntegrityCheck.data, kind=IntegrityFindingKind.dtype_mismatch, message="m")
        assert finding.collection is None
        assert finding.array_id is None
        assert finding.path is None
        assert finding.repairable is False
        assert finding.repaired is False
        assert finding.repair_error == ""
