"""Anonymizing keeps its promises: unique fakes that fit their column, dates
that keep their order, and no personal value in the issues."""

from __future__ import annotations

import datetime

import pytest

from core import contract
from core import mapping
from core import quality
from core.mapping import compiled as mapping_compiled
from core.mapping import privacy
from core.mapping import transforms
from core.mapping import types
from tests.factories import fakes
from tests.factories import mapping_specs

field = mapping_specs.field
SECRET = b"uat-secret"


@pytest.mark.parametrize("kind", ["phone", "email", "hash"])
def test_unique_values_get_unique_fakes(kind: str) -> None:
    anonymize = mapping.anonymizer(kind, SECRET)
    count = 10**6

    fakes_ = {anonymize(f"+84{n:09d}") for n in range(count)}

    assert len(fakes_) == count


def _catalog(*fields: contract.FieldSpec) -> contract.MappingCatalog:
    return mapping_specs.catalog(
        mapping_specs.mapping(
            field("customer_key", "CUST_NO", primary_key=True), *fields
        )
    )


def _problems(*fields: contract.FieldSpec) -> list[str]:
    return [
        p.message
        for p in mapping.validate_catalog(
            _catalog(*fields), mapping_specs.registry
        )
    ]


class TestKindsFitTheirType:

    @pytest.mark.parametrize(
        ("kind", "target_type", "expected"),
        [
            ("name", "VARCHAR(5)", "use VARCHAR(15) or more"),
            ("email", "VARCHAR(20)", "use VARCHAR(41) or more"),
            ("phone", "NUMBER(15)", "use VARCHAR(20) or more"),
            ("address", "VARCHAR(10)", "use VARCHAR(29) or more"),
            ("hash", "CHAR(8)", "use VARCHAR(16) or more"),
            ("mask", "NUMBER(10)", "use a text type, or another kind"),
            ("date", "VARCHAR(10)", "DATE or TIMESTAMP"),
        ],
    )
    def test_refused(self, kind: str, target_type: str, expected: str) -> None:
        problems = _problems(
            field("x", "X", target_type=target_type, personal=kind)
        )

        assert len(problems) == 1
        assert expected in problems[0]

    @pytest.mark.parametrize(
        ("kind", "target_type"),
        [
            ("name", "VARCHAR(15)"),
            ("email", "TEXT"),
            ("phone", "VARCHAR(20)"),
            ("mask", "VARCHAR(3)"),
            ("date", "TIMESTAMP"),
            ("hash", None),
        ],
    )
    def test_accepted(self, kind: str, target_type: str | None) -> None:
        assert not _problems(
            field("x", "X", target_type=target_type, personal=kind)
        )

    def test_a_fake_too_long_is_an_issue_not_a_database_error(self) -> None:
        spec = contract.FieldSpec("nick", ("X",), "VARCHAR(5)", personal="name")
        compiled = mapping_compiled.CompiledField(
            spec,
            transforms.IDENTITY,
            transforms.IDENTITY.fn,
            types.parse_type("VARCHAR(5)"),
            anonymize=privacy.anonymizer("name", SECRET),
        )

        value, issue = compiled.apply({"X": "Ng"})

        assert value is None
        assert issue is not None
        assert issue.message.startswith("the anonymized value does not fit")
        assert issue.value is None


class TestDates:

    def _compiled(self) -> mapping.CompiledMapping:
        return mapping.compile_catalog(
            _catalog(
                field("starts", "START", target_type="DATE", personal="date"),
                field("ends", "END", target_type="DATE", personal="date"),
            ),
            mapping_specs.registry,
            anonymize=SECRET,
        )["customer"]

    def test_the_dates_of_a_record_keep_their_order_and_distance(
        self,
    ) -> None:
        compiled = self._compiled()
        shifts = set()

        for n in range(2000):
            start = datetime.date(2020, 1, 1) + datetime.timedelta(days=n)
            end = start + datetime.timedelta(days=7)
            (record,) = compiled.map({"CUST_NO": n, "START": start, "END": end})
            starts, ends = record.data["starts"], record.data["ends"]
            assert isinstance(starts, datetime.date)
            assert isinstance(ends, datetime.date)

            assert ends - starts == end - start
            shift = (starts - start).days
            assert 1 <= abs(shift) <= 180
            shifts.add(shift)

        # Records get unrelated shifts.
        assert len(shifts) > 300

    def test_a_record_always_gets_the_same_shift(self) -> None:
        compiled = self._compiled()
        row = {
            "CUST_NO": "C1",
            "START": datetime.date(2020, 1, 1),
            "END": None,
        }

        (first,) = compiled.map(row)
        (again,) = compiled.map({**row, "END": datetime.date(2021, 1, 1)})

        assert first.data["starts"] == again.data["starts"]


class TestIssuesHoldNoPersonalValue:

    def _issues(self, anonymize: bytes | None) -> list[mapping.MappingIssue]:
        compiled = mapping.compile_catalog(
            _catalog(
                field(
                    "born",
                    "DOB",
                    target_type="DATE",
                    format="dd/MM/yyyy",
                    personal="date",
                ),
                field(
                    "full_name",
                    "NAME",
                    target_type="VARCHAR(20)",
                    personal="name",
                    required=True,
                ),
                field(
                    "national_id",
                    "NID",
                    target_type="VARCHAR(12)",
                    transform="value_map",
                    params={"A": "x"},
                    personal="mask",
                ),
                field("age", "AGE", target_type="NUMBER(3)"),
            ),
            mapping_specs.registry,
            anonymize=anonymize,
        )["customer"]
        (record,) = compiled.map({
            "CUST_NO": "1",
            "DOB": "31/02/1990",
            "NAME": "Nguyen Van Rat La Dai Ten",
            "NID": "079123456789",
            "AGE": "forty",
        })
        return list(record.issues)

    @pytest.mark.parametrize("anonymize", [None, SECRET])
    def test_in_values_and_messages(self, anonymize: bytes | None) -> None:
        issues = self._issues(anonymize)

        assert [(i.field, i.message, i.value) for i in issues] == [
            (
                "born",
                "the value does not match the format dd/MM/yyyy",
                None,
            ),
            (
                "full_name",
                "25 characters, more than the 20 of VARCHAR(20)",
                None,
            ),
            ("national_id", "no translation for the value", None),
            # Not personal: the value helps whoever fixes it.
            ("age", "'forty' is not a number (NUMBER(3))", "forty"),
        ]

    def test_nor_in_what_the_gate_stores(self) -> None:
        compiled = mapping.compile_catalog(
            _catalog(
                field(
                    "born",
                    "DOB",
                    target_type="DATE",
                    format="dd/MM/yyyy",
                    personal="date",
                )
            ),
            mapping_specs.registry,
            anonymize=SECRET,
        )["customer"]
        records = compiled.map({"CUST_NO": "1", "DOB": "31/02/1990"})
        gate = quality.QualityGate(
            {
                contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                    [mapping.MappingIssuesRule()]
                )
            },
            quarantine=quality.InMemoryQuarantine(),
        )

        outcome = gate.inspect(
            contract.Checkpoint.AFTER_TRANSFORM, records, fakes.make_context()
        )

        # The samples of the report, the issues and the rejected records.
        assert outcome.report is not None
        stored = repr(outcome.report) + repr(
            [(record.data, record.issues) for record in records]
        )
        assert "born" in stored
        assert "1990" not in stored
