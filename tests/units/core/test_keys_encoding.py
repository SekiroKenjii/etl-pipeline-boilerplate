"""The text a surrogate key is derived from never changes: the direct
builder gives the JSON of the first release for any natural key."""

from __future__ import annotations

import json

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
import pytest

from core import keys as core_keys
from core.keys import generation


def _oracle(natural_key: list[str]) -> str:
    """What the code did before."""
    return json.dumps(natural_key, ensure_ascii=False, separators=(",", ":"))


# Any text, surrogates aside (they cannot be encoded to UTF-8 either way),
# with the characters JSON escapes made likely.
_COMPONENTS = st.one_of(
    st.text(alphabet=st.characters(exclude_categories=["Cs"]), max_size=20),
    st.text(
        alphabet=st.sampled_from([
            '"',
            "\\",
            "\n",
            "\t",
            "\x00",
            "\x1f",
            "\x7f",
            "a",
            " ",
            "é",
            "\u2028",
            "\u200b",
        ]),
        max_size=6,
    ),
)


class TestEncodedNaturalKey:

    @given(st.lists(_COMPONENTS, max_size=4))
    @settings(max_examples=1000, deadline=None)
    def test_equals_the_json_of_the_first_release(
        self, natural_key: list[str]
    ) -> None:
        assert generation.encoded_natural_key(tuple(natural_key)) == _oracle(
            natural_key
        )
        assert generation.encoded_natural_key(natural_key) == _oracle(
            natural_key
        )

    @pytest.mark.parametrize(
        "natural_key",
        [
            (),
            ("C001",),
            ("A|B", "C"),
            ('say "hi"',),
            ("back\\slash",),
            ("tab\t",),
            ("ünïcödé", "日本"),
        ],
    )
    def test_examples(self, natural_key: tuple[str, ...]) -> None:
        assert generation.encoded_natural_key(natural_key) == _oracle(
            list(natural_key)
        )

    def test_the_keys_of_the_first_release(self) -> None:
        maker = core_keys.KeyMaker(
            core_keys.key_namespace("legacy.CUST_MASTER", "crm.customer")
        )

        # Computed by the code before this change: a key must never move.
        assert maker.key(("C001",)) == "858acae5-adfc-5502-bc36-0230c61c28e2"
        assert maker.key(("O-9", "1")) == "fd1dd8bb-1395-5b26-b7ff-cb28171ffb51"
        assert (
            maker.key(('say "hi"',)) == "3a32492f-e727-514e-93a2-75374f1148ac"
        )
        assert (
            maker.key(("ünï", "日本")) == "e82b1b79-c91d-5d77-b230-da44edcf8847"
        )
        assert maker.key(("tab\t",)) == "e327e22e-0dc0-569f-91da-8fdb83df3d01"
