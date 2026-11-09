"""The `[tuning]` section: values derived from the budgets unless set, each
with its source; what the modules and the kernel get from it."""

from __future__ import annotations

import io
import pathlib
import shutil
import types
from typing import Any

import pytest

from app import bootstrap
from cli import run
import config
from config import tuning
from modules.crm import settings as crm_settings
from shared import utils

MIB = 1024**2
GIB = 1024**3
POD = utils.Limits(4 * GIB, "memory.max", 2.0, "cpu.max")
LAPTOP = utils.Limits(12 * GIB, "MemAvailable", 12.0, "sched_getaffinity")
NOTHING = utils.Limits(None, utils.NO_LIMIT, None, utils.NO_LIMIT)


def _resolve(
    limits: utils.Limits = POD, free_disk: int | None = 100 * GIB, **given: Any
) -> config.EffectiveTuning:
    return config.resolve_tuning(
        config.TuningSettings(**given), limits, free_disk
    )


class TestDerivedValues:

    def test_the_memory_budget_is_sixty_percent_of_the_limit(self) -> None:
        found = _resolve(POD)

        # In whole MiB: 0.6 * 4096 MiB = 2457.6 MiB.
        assert found.memory_budget.value == 2457 * MIB
        assert found.memory_budget.source == "60% of memory.max 4.0GiB"
        assert found.memory_budget.text == '"2457MiB"'
        assert found.memory_budget.is_derived

    def test_the_memory_budget_has_a_floor(self) -> None:
        found = _resolve(utils.Limits(128 * MIB, "memory.max", 1.0, "cpu.max"))

        assert found.memory_budget.value == 256 * MIB
        assert "at least 256MiB" in found.memory_budget.source

    def test_no_limit_found_gives_the_floor(self) -> None:
        found = _resolve(NOTHING, free_disk=None)

        assert found.memory_budget.value == 256 * MIB
        assert found.memory_budget.source == "no limit found: the floor"
        assert found.cpu_budget.value == 1
        assert found.key_cache_disk.value == 2 * GIB
        assert "unknown: the cap" in found.key_cache_disk.source

    def test_the_cpu_budget_is_the_quota_rounded_down(self) -> None:
        assert _resolve(POD).cpu_budget.value == 2
        assert _resolve(POD).cpu_budget.source == "cpu.max 2"
        half = utils.Limits(GIB, "memory.max", 0.5, "cpu.max")
        assert _resolve(half).cpu_budget.value == 1
        assert _resolve(LAPTOP).cpu_budget.source == "sched_getaffinity 12"

    def test_the_key_cache_takes_a_quarter_of_the_budget(self) -> None:
        found = _resolve(POD)

        assert found.key_cache_memory.value == 614 * MIB
        assert found.key_cache_memory.source == "25% of memory_budget"

    def test_the_key_cache_disk_is_half_the_free_space_up_to_2_gib(
        self,
    ) -> None:
        assert _resolve(free_disk=100 * GIB).key_cache_disk.value == 2 * GIB
        assert _resolve(free_disk=GIB).key_cache_disk.value == 512 * MIB
        assert _resolve(free_disk=0).key_cache_disk.value == 0

    def test_spill_rows_follow_the_budget_between_the_bounds(self) -> None:
        # 20% of the budget at 200 bytes a row: 512 MiB gives 536,870 rows.
        small = utils.Limits(512 * MIB, "memory.max", 1.0, "cpu.max")
        assert _resolve(small).spill_rows.value == 307 * MIB // 5 // 200
        # From a 4 GiB pod on, the cap.
        assert _resolve(POD).spill_rows.value == 1_000_000
        assert _resolve(LAPTOP).spill_rows.value == 1_000_000
        assert "from 10,000 to 1,000,000" in _resolve(POD).spill_rows.source
        assert _resolve(NOTHING).spill_rows.value == 268_435
        tiny = utils.Limits(MIB, "memory.max", 1.0, "cpu.max")
        assert _resolve(tiny).spill_rows.value == 268_435  # The floor's.

    def test_parallel_steps_are_the_cpu_budget(self) -> None:
        assert _resolve(POD).max_parallel_steps.value == 2
        assert _resolve(POD).max_parallel_steps.source == "cpu_budget"
        assert _resolve(LAPTOP).max_parallel_steps.value == 12

    def test_online_choices_have_no_value(self) -> None:
        found = _resolve()

        assert found.bulk_copy_rows.value is None
        assert found.bulk_copy_rows.text == '"auto"'
        assert "online" in found.bulk_copy_rows.source
        assert found.in_chunk.value is None
        assert "at most 1,000" in found.in_chunk.source

    def test_the_queue_size_default_is_neither_set_nor_derived(self) -> None:
        found = _resolve()

        assert found.queue_size == config.Chosen(10_000, "default", "10000")
        assert not found.queue_size.is_derived
        assert not found.queue_size.is_set


class TestSetValues:

    def test_set_values_are_kept_as_given(self) -> None:
        found = _resolve(
            POD,
            memory_budget="1GiB",
            cpu_budget=3,
            key_cache_memory="64MiB",
            key_cache_disk=0,
            spill_rows=20_000,
            max_parallel_steps=5,
            queue_size=500,
            bulk_copy_rows=2000,
            in_chunk=500,
        )

        assert all(chosen.is_set for _, chosen in found.items())
        assert found.memory_budget.value == GIB
        assert found.memory_budget.text == '"1GiB"'
        assert found.key_cache_memory.value == 64 * MIB
        assert found.key_cache_disk.value == 0
        assert found.spill_rows.value == 20_000
        assert found.bulk_copy_rows.value == 2000
        assert found.in_chunk.value == 500

    def test_a_set_budget_drives_the_derived_shares(self) -> None:
        found = _resolve(POD, memory_budget="512MiB", cpu_budget=3)

        assert found.key_cache_memory.value == 128 * MIB
        assert found.spill_rows.value == 512 * MIB // 5 // 200
        assert found.max_parallel_steps.value == 3

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            ("memory_budget", 0),
            ("memory_budget", "1e9"),
            ("memory_budget", "automatic"),
            ("key_cache_disk", -1),
            ("spill_rows", 0),
            ("max_parallel_steps", 0),
            ("queue_size", 99),
            ("bulk_copy_rows", 0),
            ("in_chunk", "0"),
            ("nope", 1),
        ],
    )
    def test_bad_values_are_refused(self, key: str, value: object) -> None:
        with pytest.raises(ValueError, match=key):
            config.TuningSettings.model_validate({key: value})


class TestTheBlock:

    def test_toml_lists_every_value_with_its_source(self) -> None:
        text = _resolve(POD, spill_rows=20_000).toml()

        lines = text.splitlines()
        assert lines[0] == "[tuning]"
        assert lines[1].startswith('memory_budget = "2457MiB"')
        assert lines[1].endswith("# 60% of memory.max 4.0GiB")
        assert "spill_rows = 20000" in text
        assert "# set" in text
        assert 'bulk_copy_rows = "auto"' in text
        assert len(lines) == 10

    def test_the_summary_marks_the_derived_values(self) -> None:
        summary = _resolve(POD, spill_rows=20_000).summary()

        assert 'memory_budget="2457MiB"*' in summary
        assert "spill_rows=20000," in summary
        assert "queue_size=10000," in summary


class TestInTheApplication:

    @pytest.fixture(autouse=True)
    def pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(utils, "limits", lambda root="/": POD)
        monkeypatch.setattr(
            shutil, "disk_usage", lambda path: types.SimpleNamespace(free=GIB)
        )

    def _settings(self, **values: Any) -> config.AppSettings:
        return config.load_settings(
            "test", directory="/nonexistent", overrides=values
        )

    def test_effective_is_read_once_and_kept(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        settings = self._settings()

        first = settings.tuning.effective()
        monkeypatch.setattr(utils, "limits", lambda root="/": LAPTOP)

        assert settings.tuning.effective() is first
        assert self._settings().tuning.effective().cpu_budget.value == 12

    def test_the_module_settings_default_to_the_tuning(self) -> None:
        derived = self._settings(
            databases={
                "legacy": {"url": "sqlite://"},
                "crm": {"url": "sqlite://"},
            },
            modules={"crm": {}},
        ).module_settings("crm", crm_settings.CrmSettings)
        given = self._settings(
            databases={
                "legacy": {"url": "sqlite://"},
                "crm": {"url": "sqlite://"},
            },
            tuning={"spill_rows": 30_000, "in_chunk": 400},
            modules={"crm": {"settings": {"spill_rows": 7}}},
        ).module_settings("crm", crm_settings.CrmSettings)

        assert derived.spill_rows == 1_000_000
        assert derived.in_chunk == 1000
        assert (given.spill_rows, given.in_chunk) == (7, 400)
        # Without the application's settings: today's numbers.
        alone = crm_settings.CrmSettings()
        assert (alone.spill_rows, alone.in_chunk) == (50_000, 1000)

    def test_the_kernel_caps_the_steps_at_the_cpu_budget(
        self, tmp_path: pathlib.Path
    ) -> None:
        directory = tmp_path / "config"
        directory.mkdir()
        (directory / "test.toml").write_text(
            f'[bookkeeping]\ndirectory = "{tmp_path / "var"}"\n',
            encoding="utf-8",
        )

        with bootstrap.bootstrap(
            "test", directory=directory, logs=False
        ) as application:
            derived = application.kernel.max_parallel_steps
        with bootstrap.bootstrap(
            "test",
            directory=directory,
            logs=False,
            overrides={"tuning": {"max_parallel_steps": 7}},
        ) as application:
            given = application.kernel.max_parallel_steps

        assert (derived, given) == (2, 7)

    def test_config_tuning_prints_the_block(
        self, tmp_path: pathlib.Path
    ) -> None:
        (tmp_path / "test.toml").write_text(
            '[tuning]\nspill_rows = 20000\nkey_cache_memory = "64MiB"\n',
            encoding="utf-8",
        )
        out = io.StringIO()

        code = run.main(
            ["--env", "test", "--config", str(tmp_path), "config", "tuning"],
            out,
        )

        text = out.getvalue()
        assert code == 0
        assert text.startswith("[tuning]\n")
        assert 'key_cache_memory = "64MiB"' in text
        assert "spill_rows = 20000" in text
        assert "# 60% of memory.max 4.0GiB" in text
        assert tuning.MIN_MEMORY_BUDGET == 256 * MIB
