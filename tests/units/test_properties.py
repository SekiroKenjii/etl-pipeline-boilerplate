"""Properties that hold for any input, checked with generated data.

Where an example-based test checks a few cases, these state the rule: the
canonical form of a value does not depend on the database it was read from,
a split covers each row once, a date written then parsed is the same date.
"""

from __future__ import annotations

from collections.abc import Callable
import datetime
import decimal
import os
import random
import re
import unicodedata
import uuid

from hypothesis import assume
from hypothesis import given
from hypothesis import seed
from hypothesis import settings
from hypothesis import strategies as st
import pytest

from core import contract
from core import cutover
from core import keys as core_keys
from core import mapping
from core import operators
from core.cutover import checksum
from core.mapping import privacy
from tests.factories import fake_ports

# CI runs the same examples at every run (`derandomize`): a failure there
# is the change's, and reproduces. Anywhere else every run explores new
# examples from a seed shown with any failure: `HYPOTHESIS_SEED=<seed>`
# explores them again. `HYPOTHESIS_PROFILE` chooses the profile.
settings.register_profile("ci", derandomize=True, max_examples=200)
settings.register_profile("explore", max_examples=200, print_blob=True)
settings.load_profile(
    os.environ.get("HYPOTHESIS_PROFILE")
    or ("ci" if os.environ.get("CI") else "explore")
)
_SEED = (
    None
    if settings().derandomize
    else int(
        os.environ.get("HYPOTHESIS_SEED")
        or random.SystemRandom().getrandbits(32)
    )
)


def _seeded[T: Callable[..., None]](test: T) -> T:
    """Explores from the seed of this run, unless derandomized."""
    return test if _SEED is None else seed(_SEED)(test)


@pytest.fixture(autouse=True)
def _show_the_seed(request: pytest.FixtureRequest) -> None:
    """Shows the seed with a failure, to explore the same examples again."""
    if _SEED is not None:
        request.node.add_report_section(
            "call", "seed", f"HYPOTHESIS_SEED={_SEED} explores them again"
        )


_DECIMALS = st.decimals(
    allow_nan=False, allow_infinity=False, min_value=-(10**12), max_value=10**12
)
_TEXT = st.text(alphabet=st.characters(exclude_categories=["Cs"]), max_size=30)
_ZONES = st.integers(min_value=-14 * 60, max_value=14 * 60).map(
    lambda minutes: datetime.timezone(datetime.timedelta(minutes=minutes))
)
_MOMENTS = st.datetimes(
    min_value=datetime.datetime(1900, 1, 2),
    max_value=datetime.datetime(2200, 1, 1),
)


class TestCanonicalValues:
    """The same value, read back from any database, compares equal."""

    @_seeded
    @given(
        st.decimals(
            allow_nan=False,
            allow_infinity=False,
            places=6,
            min_value=-(10**12),
            max_value=10**12,
        )
    )
    def test_a_decimal_at_any_scale(self, value: decimal.Decimal) -> None:
        # More places, zeros only: `NUMERIC(24,12)` reads 1.5 as 1.500000000000.
        rescaled = value.quantize(decimal.Decimal(1).scaleb(-12))

        assert cutover.canonical_value(value) == cutover.canonical_value(
            rescaled
        )

    @_seeded
    @given(st.integers(min_value=-(2**53), max_value=2**53))
    def test_an_integer_as_int_float_or_decimal(self, value: int) -> None:
        expected = cutover.canonical_value(value)

        assert cutover.canonical_value(float(value)) == expected
        assert cutover.canonical_value(decimal.Decimal(value)) == expected

    @_seeded
    @given(st.floats(allow_nan=False, allow_infinity=False, width=64))
    def test_a_float_and_its_decimal_text(self, value: float) -> None:
        assert cutover.canonical_value(value) == cutover.canonical_value(
            decimal.Decimal(repr(value))
        )

    @_seeded
    @given(_MOMENTS, _ZONES)
    def test_a_moment_in_any_time_zone(
        self, moment: datetime.datetime, zone: datetime.timezone
    ) -> None:
        aware = moment.replace(tzinfo=datetime.UTC)

        assert cutover.canonical_value(aware) == cutover.canonical_value(
            aware.astimezone(zone)
        )
        assert cutover.canonical_value(aware) == cutover.canonical_value(moment)

    @_seeded
    @given(st.dates())
    def test_a_date_and_its_midnight(self, day: datetime.date) -> None:
        midnight = datetime.datetime.combine(day, datetime.time())

        assert cutover.canonical_value(day) == cutover.canonical_value(midnight)

    @_seeded
    @given(_TEXT)
    def test_text_composed_or_decomposed(self, text: str) -> None:
        assert cutover.canonical_value(
            unicodedata.normalize("NFD", text)
        ) == cutover.canonical_value(unicodedata.normalize("NFC", text))

    @_seeded
    @given(st.uuids())
    def test_a_uuid_or_its_text(self, value: uuid.UUID) -> None:
        assert cutover.canonical_value(value) == cutover.canonical_value(
            str(value)
        )

    @_seeded
    @given(_DECIMALS)
    def test_a_number_and_its_text_differ(self, value: decimal.Decimal) -> None:
        # "Text against number always differ" (`checksum`).
        assert cutover.canonical_value(value) != cutover.canonical_value(
            str(value)
        )
        assert cutover.canonical_value(value) != cutover.canonical_value(
            format(value.normalize(), "f")
        )


# A value as two databases may read it back, each side typed its way.
type _Read = tuple[object, object]


@st.composite
def _read_twice(draw: st.DrawFn) -> _Read:
    """A value as two databases read it back: equal once canonical."""
    kind = draw(
        st.sampled_from([
            "integral",
            "boolean",
            "decimal",
            "moment",
            "date",
            "text",
            "uuid",
            "null",
        ])
    )
    match kind:
        case "integral":
            number = draw(st.integers(min_value=-(2**53), max_value=2**53))
            other: object = draw(
                st.sampled_from([
                    float(number),
                    decimal.Decimal(number).quantize(decimal.Decimal("0.01")),
                    decimal.Decimal(number),
                ])
            )
            return number, other
        case "boolean":
            flag = draw(st.booleans())
            return flag, int(flag)
        case "decimal":
            value = draw(
                st.decimals(
                    allow_nan=False,
                    allow_infinity=False,
                    places=6,
                    min_value=-(10**12),
                    max_value=10**12,
                )
            )
            as_float = float(value)
            if decimal.Decimal(repr(as_float)) == value and draw(st.booleans()):
                return value, as_float  # A DOUBLE on the other side.
            return value, value.quantize(decimal.Decimal(1).scaleb(-12))
        case "moment":
            # Aware in some zone on one side, naive UTC on the other.
            moment = draw(_MOMENTS)
            zone = draw(_ZONES)
            aware = moment.replace(tzinfo=datetime.UTC).astimezone(zone)
            return aware, moment
        case "date":
            day = draw(st.dates())
            return day, datetime.datetime.combine(day, datetime.time())
        case "text":
            text = draw(_TEXT)
            if not text:
                return "", None  # Oracle stores the empty string as NULL.
            return (
                unicodedata.normalize("NFC", text),
                unicodedata.normalize("NFD", text),
            )
        case "uuid":
            value_ = draw(st.uuids())
            return value_, str(value_)
        case _:
            return None, None


class TestDigests:
    """A row reads differently from two databases, digests the same."""

    @_seeded
    @given(st.lists(_read_twice(), min_size=1, max_size=8), st.data())
    def test_the_same_row_read_two_ways(
        self, values: list[_Read], data: st.DataObject
    ) -> None:
        columns = [f"c{i}" for i in range(len(values))]
        one = {column: values[i][0] for i, column in enumerate(columns)}
        # The other side returns its columns in another order.
        other = {
            column: values[columns.index(column)][1]
            for column in data.draw(st.permutations(columns))
        }

        first = checksum.canonical_row(one, columns)
        second = checksum.canonical_row(other, columns)

        assert checksum.digest(first) == checksum.digest(second)
        assert checksum.text(first) == checksum.text(second)
        bucket = checksum.bucket(checksum.text(first), 7)
        assert 0 <= bucket < 7
        assert bucket == checksum.bucket(checksum.text(second), 7)

    @_seeded
    @given(
        st.lists(_read_twice(), min_size=1, max_size=8),
        st.data(),
    )
    def test_a_value_changed_changes_the_digest(
        self, values: list[_Read], data: st.DataObject
    ) -> None:
        columns = [f"c{i}" for i in range(len(values))]
        row = {column: values[i][0] for i, column in enumerate(columns)}
        changed = data.draw(st.sampled_from(columns))
        before = checksum.canonical_row(row, columns)
        row[changed] = data.draw(
            st.one_of(st.integers(), _TEXT, st.none(), st.dates()).filter(
                lambda new: checksum.canonical_value(new)
                != before[columns.index(changed)]
            )
        )

        after = checksum.canonical_row(row, columns)

        assert checksum.digest(after) != checksum.digest(before)


class TestNaturalKeys:

    @_seeded
    @given(
        st.lists(
            st.one_of(
                st.integers(), _DECIMALS, _TEXT.filter(str.strip), st.dates()
            ),
            min_size=1,
            max_size=4,
        )
    )
    def test_canonical_keys_are_idempotent(
        self, components: list[object]
    ) -> None:
        once = core_keys.canonical(components)

        assert core_keys.canonical(list(once)) == once

    @_seeded
    @given(_TEXT.filter(str.strip))
    def test_key_case_folds_text(self, text: str) -> None:
        upper = core_keys.canonical([text], contract.KeyCase.UPPER)

        assert core_keys.canonical(
            [text.lower()], contract.KeyCase.UPPER
        ) == core_keys.canonical([text.upper()], contract.KeyCase.UPPER)
        assert upper == core_keys.canonical(list(upper), contract.KeyCase.UPPER)


class TestMappingTypes:

    @_seeded
    @given(st.dates(min_value=datetime.date(1000, 1, 1)))
    def test_a_date_written_then_parsed(self, day: datetime.date) -> None:
        date = mapping.parse_type("DATE")

        assert date.cast(day.strftime("%d/%m/%Y"), "dd/MM/yyyy") == day
        assert date.cast(day.isoformat()) == day

    @_seeded
    @given(st.decimals(min_value=-(10**7), max_value=10**7, places=2))
    def test_a_number_within_its_scale(self, value: decimal.Decimal) -> None:
        number = mapping.parse_type("NUMBER(12,2)")

        assert number.cast(str(value)) == value


# The kinds whose fakes come from a space large enough that another secret
# never gives the same fake (`privacy`: 64 bits or more).
_KEYED_KINDS = ["email", "phone", "hash"]
# A fake of these kinds has a form of its own.
_FORMS = {
    "name": r"\w+ \w+",
    "email": r"user\.[0-9a-f]{20}@example\.invalid",
    "phone": r"\+1555\d{15}",
    "address": r"\d+ \w+ Street, District \d+",
    "hash": r"[0-9a-f]{16}",
}


class TestAnonymization:
    """Fakes are deterministic, keyed, never the value, and fit their
    column. Unique kinds staying unique: `test_mapping_anonymization`."""

    @_seeded
    @given(
        st.sampled_from(sorted(mapping.PERSONAL_KINDS - {"mask"})),
        _TEXT.filter(str.strip),
    )
    def test_the_same_value_the_same_fake(self, kind: str, value: str) -> None:
        first = mapping.anonymizer(kind, b"secret")(value)
        # A value looking like a fake may be its own: none is generated.
        assume(not any(re.fullmatch(form, value) for form in _FORMS.values()))
        # Text of the `date` kind is masked: `*` and `a*b` mask to themselves.
        assume(kind != "date" or set(value[1:-1] or value) != {"*"})

        assert mapping.anonymizer(kind, b"secret")(value) == first
        assert first != value
        if kind in _FORMS:
            assert isinstance(first, str)
            assert re.fullmatch(_FORMS[kind], first)
            assert len(first) <= privacy.TEXT_LENGTHS[kind]

    @_seeded
    @given(st.sampled_from(_KEYED_KINDS), _TEXT.filter(str.strip))
    def test_another_secret_another_fake(self, kind: str, value: str) -> None:
        assert mapping.anonymizer(kind, b"secret")(value) != mapping.anonymizer(
            kind, b"other secret"
        )(value)

    @_seeded
    @given(_TEXT.filter(str.strip))
    def test_a_mask_keeps_the_length_and_the_ends_only(
        self, value: str
    ) -> None:
        masked = mapping.anonymizer("mask", b"secret")(value)

        assert isinstance(masked, str)
        assert len(masked) == len(value)
        kept = [i for i, char in enumerate(masked) if char != "*"]
        assert set(kept) <= {0, len(value) - 1}
        assert all(masked[i] == value[i] for i in kept)
        assert mapping.anonymizer("mask", b"secret")(value) == masked

    @_seeded
    @given(
        st.one_of(
            st.dates(datetime.date(1900, 1, 1), datetime.date(2200, 1, 1)),
            _MOMENTS,
        ),
        st.lists(_TEXT, min_size=1, max_size=3),
    )
    def test_a_date_moves_by_a_day_to_half_a_year(
        self, value: datetime.date, record: list[str]
    ) -> None:
        shifted = mapping.anonymizer("date", b"secret")(value, record)

        assert type(shifted) is type(value)
        assert isinstance(shifted, datetime.date)
        assert 1 <= abs((shifted - value).days) <= 180
        assert mapping.anonymizer("date", b"secret")(value, record) == shifted


class TestSplits:

    @_seeded
    @given(
        st.sets(st.integers(min_value=-1000, max_value=1000), max_size=200),
        st.integers(min_value=1, max_value=12),
    )
    def test_ranges_cover_every_row_once(
        self, keys: set[int], count: int
    ) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"t": [{"id": key} for key in keys]}
        )

        ranges = operators.split(reader, "t", ["id"], count)

        assert 1 <= len(ranges) <= count
        for key in keys:
            holding = [
                r
                for r in ranges
                if (r.lower is None or key >= r.lower)
                and (r.upper is None or key < r.upper)
            ]
            assert len(holding) == 1
