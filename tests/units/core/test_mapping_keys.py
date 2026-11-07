from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any
import uuid

import pytest

from core import contract
from core import keys as core_keys
from core import mapping
from core import pipeline
from core import quality
from core.mapping import catalog
from tests.factories import fakes
from tests.factories import mapping_specs

CUSTOMER = mapping_specs.mapping(
    mapping_specs.field(
        "customer_key", "CUST_NO", target_type="UUID", primary_key=True
    ),
    mapping_specs.field("name", "NAME", row=3),
    name="customer",
)
ORDER = mapping_specs.mapping(
    mapping_specs.field(
        "order_key", "ORD_NO", target_type="UUID", primary_key=True
    ),
    mapping_specs.field(
        "customer_key",
        "CUST_NO",
        target_type="UUID",
        references="customer",
        row=3,
    ),
    name="order",
)
CRM = mapping_specs.catalog(CUSTOMER, ORDER)


def _compiled(name: str, crm: Any = CRM) -> mapping.CompiledMapping:
    return mapping.compile_catalog(crm, mapping_specs.registry)[name]


def _problems(*mappings: Any) -> list[str]:
    return [
        str(problem)
        for problem in mapping.validate_catalog(
            mapping_specs.catalog(*mappings), mapping_specs.registry
        )
    ]


class TestKeys:

    def test_the_primary_key_is_derived_from_the_natural_key(self) -> None:
        record = _compiled("customer").apply(
            {"CUST_NO": " C001 ", "NAME": "Ana"}
        )
        expected = core_keys.KeyMaker(
            core_keys.key_namespace("legacy.CUST_MASTER", "crm.customer")
        ).key(("C001",))

        assert record.data["customer_key"] == expected == record.key
        assert record.natural_key == ("C001",)
        assert record.key_entry() is not None
        assert record.entity == "crm.customer"

    def test_a_foreign_key_equals_its_parent_key_without_any_lookup(
        self,
    ) -> None:
        customer = _compiled("customer").apply(
            {"CUST_NO": "C001", "NAME": "Ana"}
        )
        order = _compiled("order").apply({"ORD_NO": "O-9", "CUST_NO": "C001"})

        assert order.data["customer_key"] == customer.key

    def test_composite_natural_keys(self) -> None:
        line = mapping_specs.catalog(
            mapping_specs.mapping(
                mapping_specs.field(
                    "line_key",
                    "ORD_NO",
                    "LINE_NO",
                    target_type="UUID",
                    primary_key=True,
                ),
                name="order_line",
            )
        )
        compiled = _compiled("order_line", line)

        complete = compiled.apply({"ORD_NO": "O-9", "LINE_NO": 1})
        partial = compiled.apply({"ORD_NO": "O-9", "LINE_NO": ""})

        assert complete.natural_key == ("O-9", "1")
        assert [i.message for i in partial.issues] == [
            "incomplete natural key: LINE_NO empty"
        ]

    def test_an_empty_primary_key_is_an_issue(self) -> None:
        record = _compiled("customer").apply({"CUST_NO": " ", "NAME": "Ana"})

        assert [(i.field, i.message) for i in record.issues] == [
            ("customer_key", "is required but empty")
        ]
        assert record.key_entry() is None

    def test_an_empty_optional_reference_stays_empty(self) -> None:
        record = _compiled("order").apply({"ORD_NO": "O-9", "CUST_NO": None})

        assert record.data["customer_key"] is None
        assert record.ok

    def test_hmac_keys_need_the_secret_at_compile_time(self) -> None:
        secret = mapping_specs.catalog(
            mapping_specs.mapping(
                mapping_specs.field("id", "NATIONAL_ID", primary_key=True),
                key_hashing=contract.KeyHashing.HMAC,
            )
        )

        assert (
            mapping.validate_catalog(secret, mapping_specs.registry) == []
        )  # CI has no secret
        with pytest.raises(
            contract.MappingDefinitionError, match="pass the key secret"
        ):
            mapping.compile_catalog(secret, mapping_specs.registry)

        compiled = mapping.compile_catalog(
            secret, mapping_specs.registry, key_secret=b"from-vault"
        )["customer"]
        assert (
            uuid.UUID(
                compiled.apply({"NATIONAL_ID": "079123456789"}).key or ""
            ).version
            == 8
        )


class TestValidation:

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            (
                mapping_specs.field(
                    "k", "A", primary_key=True, transform="trim"
                ),
                "takes no transform",
            ),
            (
                mapping_specs.field("k", primary_key=True),
                "needs its natural key column(s)",
            ),
            (
                mapping_specs.field("k", "A", primary_key=True, default="x"),
                "takes no default",
            ),
            (
                mapping_specs.field(
                    "k", "A", primary_key=True, target_type="NUMBER(10)"
                ),
                "keys are UUIDs",
            ),
            (
                mapping_specs.field(
                    "k", "A", primary_key=True, target_type="VARCHAR(20)"
                ),
                "keys are UUIDs",
            ),
        ],
    )
    def test_primary_key_problems(self, spec: Any, expected: str) -> None:
        problems = _problems(mapping_specs.mapping(spec))

        assert len(problems) == 1, problems
        assert expected in problems[0], problems

    def test_one_primary_key_per_mapping(self) -> None:
        problems = _problems(
            mapping_specs.mapping(
                mapping_specs.field("a", "A", primary_key=True),
                mapping_specs.field("b", "B", primary_key=True, row=3),
            )
        )

        assert problems == [
            "crm.xlsx › customer › row 3 › Key: a mapping has one primary key:"
            " list every natural key column in the source columns of 'a'"
            " instead"
        ]

    def test_references_are_checked(self) -> None:
        keyless = mapping_specs.mapping(
            mapping_specs.field("name", "NAME"), name="customer"
        )
        problems = _problems(
            keyless,
            mapping_specs.mapping(
                mapping_specs.field(
                    "customer_key", "CUST_NO", references="custmer"
                ),
                mapping_specs.field(
                    "other", "X", "Y", references="customer", row=3
                ),
                name="order",
            ),
        )

        assert problems == [
            (
                "crm.xlsx › customer › row 2 › References: unknown mapping"
                " 'custmer'. Did you mean 'customer'?"
            ),
            (
                "crm.xlsx › customer › row 3 › References: mapping 'customer'"
                " has no primary key to reference"
            ),
        ]

    def test_reference_column_count_must_match_the_parent_key(self) -> None:
        problems = _problems(
            CUSTOMER,
            mapping_specs.mapping(
                mapping_specs.field(
                    "customer_key", "CUST_NO", "BRANCH", references="customer"
                ),
                name="order",
            ),
        )

        assert problems == [
            "crm.xlsx › customer › row 2 › Source columns: the key of"
            " 'customer' has 1 column(s) (CUST_NO), 2 given"
        ]

    def test_reference_cycles_are_reported(self) -> None:
        a = mapping_specs.mapping(
            mapping_specs.field("a_key", "A", primary_key=True),
            mapping_specs.field("b_key", "B", references="b", row=3),
            name="a",
        )
        b = mapping_specs.mapping(
            mapping_specs.field("b_key", "B", primary_key=True),
            mapping_specs.field("a_key", "A", references="a", row=3),
            name="b",
        )

        assert _problems(a, b) == [
            "references form a cycle: a -> b -> a. Load one side in two passes"
            " (reference empty first, then updated)."
        ]

    def test_self_references_are_not_cycles(self) -> None:
        employee = mapping_specs.mapping(
            mapping_specs.field("employee_key", "EMP_NO", primary_key=True),
            mapping_specs.field(
                "manager_key", "MGR_NO", references="employee", row=3
            ),
            name="employee",
        )

        assert _problems(employee) == []
        assert mapping.mapping_dependencies(
            mapping_specs.catalog(employee)
        ) == {"employee": frozenset()}


def test_dependencies_give_the_load_order() -> None:
    assert mapping.mapping_dependencies(CRM) == {
        "customer": frozenset(),
        "order": frozenset({"customer"}),
    }


def test_keys_survive_the_compiled_json() -> None:
    hashed = mapping_specs.catalog(
        mapping_specs.mapping(
            mapping_specs.field("k", "A", primary_key=True),
            key_version=2,
            key_hashing=contract.KeyHashing.HMAC,
        )
    )

    assert catalog.loads(catalog.dumps(CRM)) == CRM
    assert catalog.loads(catalog.dumps(hashed)) == hashed


class TestReferenceRules:

    def test_one_rule_per_foreign_key_and_self_references_warn(self) -> None:
        employee = mapping_specs.catalog(
            mapping_specs.mapping(
                mapping_specs.field("employee_key", "EMP_NO", primary_key=True),
                mapping_specs.field(
                    "manager_key", "MGR_NO", references="employee", row=3
                ),
                name="employee",
            )
        )
        registry_ = core_keys.InMemoryKeyRegistry()

        assert [
            (r.name, r.on_violation)
            for r in mapping.reference_rules(_compiled("order"), registry_)
        ] == [("references:order.customer_key", contract.QualityAction.REJECT)]
        assert [
            r.on_violation
            for r in mapping.reference_rules(
                _compiled("employee", employee), registry_
            )
        ] == [contract.QualityAction.WARN]

    def test_orphans_violate_known_parents_pass(self) -> None:
        keys = core_keys.InMemoryKeyRegistry()
        customer = _compiled("customer").apply(
            {"CUST_NO": "C001", "NAME": "Ana"}
        )
        entry = customer.key_entry()
        assert entry is not None
        keys.register([entry])
        (rule,) = mapping.reference_rules(_compiled("order"), keys)
        ctx = fakes.make_context()

        known = _compiled("order").apply({"ORD_NO": "1", "CUST_NO": "C001"})
        orphan = _compiled("order").apply({"ORD_NO": "2", "CUST_NO": "C404"})

        assert list(rule.check_record(known, ctx)) == []
        assert [v.message for v in rule.check_record(orphan, ctx)] == [
            "references a crm.customer that is not loaded"
        ]


class Rows(pipeline.IDataExtractor[dict[str, Any]]):

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(self, ctx: contract.ETLContext) -> Iterator[dict[str, Any]]:
        yield from self.rows


def test_parents_load_first_and_orphans_are_quarantined() -> None:
    compiled = mapping.compile_catalog(CRM, mapping_specs.registry)
    keys = core_keys.InMemoryKeyRegistry()
    quarantine = quality.InMemoryQuarantine()
    loaders = {
        name: fakes.CollectingLoader[mapping.MappedRecord]()
        for name in compiled
    }
    sources = {
        "customer": [{"CUST_NO": "C001", "NAME": "Ana"}],
        "order": [
            {"ORD_NO": "O-1", "CUST_NO": "C001"},
            {"ORD_NO": "O-2", "CUST_NO": "C404"},
        ],
    }
    dependencies = mapping.mapping_dependencies(CRM)

    def step(name: str) -> contract.PipelineStep:
        gate = quality.QualityGate(
            {
                contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                    mapping.reference_rules(compiled[name], keys)
                )
            },
            quarantine=quarantine,
        )
        job = pipeline.ETLJob(
            Rows(sources[name]),
            mapping.MappingTransformer(compiled[name]),
            core_keys.RegisteringLoader(
                loaders[name], keys, key=mapping.MappedRecord.key_entry
            ),
            gate,
        )
        return contract.PipelineStep(
            name, job, dependencies=tuple(dependencies[name])
        )

    class Crm(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            return [
                step("order"),
                step("customer"),
            ]  # declared in the "wrong" order

    result = Crm(max_workers=2).run(fakes.make_context("crm"))

    assert [r.data["customer_key"] for r in loaders["order"].rows] == [
        loaders["customer"].rows[0].key
    ]
    (orphan,) = quarantine.records
    assert orphan.issues[0].rule == "references:order.customer_key"
    assert result.step_results["order"].stats.rejected == 1
    assert result.status is contract.ETLStatus.PARTIAL_SUCCESS


class TestKeyCase:

    def _spec(self, name: str, case: contract.KeyCase, **options: Any) -> Any:
        return mapping_specs.mapping(
            mapping_specs.field(
                f"{name}_key", "CODE", target_type="UUID", primary_key=True
            ),
            *options.pop("fields", ()),
            name=name,
            key_case=case,
            **options,
        )

    def test_a_case_insensitive_source_gives_one_key_per_code(self) -> None:
        upper = mapping.compile_catalog(
            mapping_specs.catalog(
                self._spec("customer", contract.KeyCase.UPPER)
            ),
            mapping_specs.registry,
        )["customer"]

        (a,) = upper.map({"CODE": "c001"})
        (b,) = upper.map({"CODE": " C001"})

        assert a.key == b.key
        assert a.natural_key == ("C001",)

    def test_the_default_keeps_the_case(self) -> None:
        compiled = _compiled("customer")

        (a,) = compiled.map({"CUST_NO": "c001", "NAME": "x"})
        (b,) = compiled.map({"CUST_NO": "C001", "NAME": "x"})

        assert a.key != b.key

    def test_foreign_keys_follow_their_parent_case(self) -> None:
        parent = self._spec("customer", contract.KeyCase.LOWER)
        child = mapping_specs.mapping(
            mapping_specs.field(
                "order_key", "ORD_NO", target_type="UUID", primary_key=True
            ),
            mapping_specs.field(
                "customer_key",
                "CUST",
                target_type="UUID",
                references="customer",
                row=3,
            ),
            name="order",
        )
        compiled = mapping.compile_catalog(
            mapping_specs.catalog(parent, child), mapping_specs.registry
        )

        (customer,) = compiled["customer"].map({"CODE": "AbC"})
        (order,) = compiled["order"].map({"ORD_NO": "O-1", "CUST": "ABC"})

        assert order.data["customer_key"] == customer.key
        assert order.foreign_keys == {"customer_key": ("abc",)}
        # The order's own key keeps its case.
        assert order.natural_key == ("O-1",)

    def test_the_case_survives_the_compiled_json(self) -> None:
        crm = mapping_specs.catalog(
            self._spec("customer", contract.KeyCase.UPPER)
        )

        loaded = catalog.loads(catalog.dumps(crm))

        assert loaded.mappings[0].key_case is contract.KeyCase.UPPER
