"""The compiled date parser reads what `strptime` reads, and refuses what
it refuses: for every format the workbook grammar allows, the same value
or the same error for any text."""

from __future__ import annotations

from collections.abc import Callable
import datetime
import re

from hypothesis import assume
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
import pytest

from core import mapping
from core.mapping import casts

_EXPLORING = settings(max_examples=300, deadline=None)


def _oracle(source_format: str, text: str) -> tuple[str, object]:
    """What the code did before: `strptime` on the translated format."""
    return _outcome(
        lambda: datetime.datetime.strptime(
            text, casts.to_strftime(source_format)
        )
    )


def _outcome(parse: Callable[[], object]) -> tuple[str, object]:
    try:
        return "value", parse()
    except ValueError:
        return "error", ValueError.__name__


# The workbook grammar: tokens, quoted literals and separators, in any
# order, as `format_problems` accepts them (one year, minutes with hours).
_TOKENS = ("yyyy", "yy", "MM", "dd", "HH", "mm", "ss")
_SEPARATORS = st.sampled_from(["/", "-", ".", " ", ":", "", "  ", ", "])
_LITERALS = st.sampled_from(["'T'", "' at '", "'h'", "'.'", "'Z'"])


@st.composite
def _workbook_formats(draw: st.DrawFn) -> str:
    year = draw(st.sampled_from(["yyyy", "yy"]))
    others = draw(
        st.lists(
            st.sampled_from([t for t in _TOKENS if t not in ("yyyy", "yy")]),
            unique=True,
            max_size=5,
        )
    )
    tokens = [year, *others]
    if "mm" in tokens and "HH" not in tokens:
        tokens.append("HH")
    tokens = list(draw(st.permutations(tokens)))
    parts: list[str] = []
    for token in tokens:
        parts.append(token)
        parts.append(
            draw(st.one_of(_SEPARATORS, _LITERALS))
            if token != tokens[-1]
            else ""
        )
    source_format = "".join(parts)
    assume(not casts.format_problems(source_format))
    return source_format


# Texts near the grammar: digits in runs of 1 to 4, the separators, letters
# in either case, spaces, and the odd character.
_TEXT_PIECES = st.one_of(
    st.integers(min_value=0, max_value=9999).map(str),
    st.integers(min_value=0, max_value=99).map(lambda n: f"{n:02d}"),
    st.sampled_from([
        "/",
        "-",
        ".",
        " ",
        ":",
        "T",
        "t",
        "Z",
        "",
        "  ",
        ",",
        "x",
        "60",
        "61",
        "00",
        "31",
        "29",
        "02",
        " 1",
    ]),
)
_TEXTS = st.lists(_TEXT_PIECES, min_size=1, max_size=9).map("".join)


_PARTS = re.compile(f"({casts._TOKENS}|'[^']*')")  # pylint: disable=protected-access


@st.composite
def _written(draw: st.DrawFn, source_format: str) -> str:
    """A moment written in `source_format` the ways `strptime` reads: 1 or 2
    digits, a day or hour padded with a space, literals in either case,
    runs of spaces for one."""
    moment = draw(
        st.datetimes(
            min_value=datetime.datetime(1, 1, 1),
            max_value=datetime.datetime(9999, 12, 31),
        )
    )
    renderings: dict[str, list[str]] = {
        "yyyy": [f"{moment.year:04d}"],
        "yy": [f"{moment.year % 100:02d}"],
        "MM": [f"{moment.month:02d}", str(moment.month)],
        "dd": [f"{moment.day:02d}", str(moment.day), f"{moment.day:2d}"],
        "HH": [f"{moment.hour:02d}", str(moment.hour), f"{moment.hour:2d}"],
        "mm": [f"{moment.minute:02d}", str(moment.minute)],
        "ss": [f"{moment.second:02d}", str(moment.second)],
    }
    text = ""
    for part in _PARTS.split(source_format):
        if part in renderings:
            text += draw(st.sampled_from(renderings[part]))
        elif part.startswith("'"):
            text += draw(st.sampled_from([part[1:-1], part[1:-1].swapcase()]))
        elif part.strip():
            text += part
        else:
            text += draw(
                st.sampled_from([part, part * 2, part.replace(" ", "\t")])
            )
    return text


class TestCompiledParserAgainstStrptime:

    @_EXPLORING
    @given(st.data())
    def test_a_moment_written_as_strptime_reads(
        self, data: st.DataObject
    ) -> None:
        source_format = data.draw(_workbook_formats())
        text = data.draw(_written(source_format))
        parse = casts.text_parser(source_format)

        assert _outcome(lambda: parse(text)) == _oracle(source_format, text)

    @_EXPLORING
    @given(_workbook_formats(), _TEXTS)
    def test_any_text(self, source_format: str, text: str) -> None:
        parse = casts.text_parser(source_format)

        assert _outcome(lambda: parse(text)) == _oracle(source_format, text)

    @_EXPLORING
    @given(
        _workbook_formats(),
        st.datetimes(
            min_value=datetime.datetime(1, 1, 1),
            max_value=datetime.datetime(9999, 12, 31),
        ),
    )
    def test_a_moment_written_in_the_format(
        self, source_format: str, moment: datetime.datetime
    ) -> None:
        text = moment.replace(microsecond=0).strftime(
            casts.to_strftime(source_format)
        )
        parse = casts.text_parser(source_format)

        assert _outcome(lambda: parse(text)) == _oracle(source_format, text)

    @_EXPLORING
    @given(
        _workbook_formats(),
        st.datetimes(
            min_value=datetime.datetime(1, 1, 1),
            max_value=datetime.datetime(9999, 12, 31),
        ),
        st.sampled_from(["", " ", "  ", "x", "0", "1", "T"]),
        st.sampled_from(["", " ", "0", "1", "x", ":00"]),
    )
    def test_a_moment_padded_or_followed_by_more(
        self,
        source_format: str,
        moment: datetime.datetime,
        before: str,
        after: str,
    ) -> None:
        text = (
            before + moment.strftime(casts.to_strftime(source_format)) + after
        )
        parse = casts.text_parser(source_format)

        assert _outcome(lambda: parse(text)) == _oracle(source_format, text)

    @pytest.mark.parametrize(
        ("source_format", "text", "expected"),
        [
            ("dd/MM/yyyy", "31/12/2020", datetime.datetime(2020, 12, 31)),
            ("dd/MM/yyyy", "1/2/2020", datetime.datetime(2020, 2, 1)),
            ("dd/MM/yyyy", " 1/2/2020", datetime.datetime(2020, 2, 1)),
            ("dd/MM/yy", "01/02/69", datetime.datetime(1969, 2, 1)),
            ("dd/MM/yy", "01/02/68", datetime.datetime(2068, 2, 1)),
            (
                "yyyy-MM-dd'T'HH:mm",
                "2020-02-01t13:45",
                datetime.datetime(2020, 2, 1, 13, 45),
            ),
            (
                "yyyy-MM-dd HH:mm:ss",
                "2020-02-01   13:45:07",
                datetime.datetime(2020, 2, 1, 13, 45, 7),
            ),
            ("yyyyMMdd", "20200201", datetime.datetime(2020, 2, 1)),
        ],
    )
    def test_accepted(
        self, source_format: str, text: str, expected: datetime.datetime
    ) -> None:
        assert casts.text_parser(source_format)(text) == expected

    @pytest.mark.parametrize(
        ("source_format", "text"),
        [
            ("dd/MM/yyyy", "31/02/2020"),
            ("dd/MM/yyyy", "31/12/2020x"),
            ("dd/MM/yyyy", "31/12/20"),
            ("dd/MM/yyyy", ""),
            ("yyyy-MM-dd HH:mm:ss", "2020-02-01 13:45:60"),
            ("dd/MM/yyyy", "01/01/0000"),
        ],
    )
    def test_refused(self, source_format: str, text: str) -> None:
        parse = casts.text_parser(source_format)

        with pytest.raises(ValueError):
            parse(text)

    def test_python_formats_and_odd_ones_keep_strptime(self) -> None:
        for source_format in ("%d-%b-%Y", "dd/MM", "yy/yyyy", "yyyy yyyy"):
            assert (
                casts._compiled_parser(  # pylint: disable=protected-access
                    casts.to_strftime(source_format)
                )
                is None
            )
        assert casts.text_parser("%d-%b-%Y")("05-Mar-2020") == (
            datetime.datetime(2020, 3, 5)
        )

    def test_the_cast_error_is_unchanged(self) -> None:
        column = mapping.parse_type("DATE")

        with pytest.raises(
            mapping.CastError,
            match="'31/02/2020' does not match the format dd/MM/yyyy",
        ) as error:
            column.cast("31/02/2020", "dd/MM/yyyy")
        assert error.value.redacted == (
            "the value does not match the format dd/MM/yyyy"
        )
