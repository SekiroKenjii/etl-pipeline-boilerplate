from __future__ import annotations

import io
import json
import pathlib

import pydantic
import pytest

from cli import run
import config


def _write(directory: pathlib.Path, **files: str) -> pathlib.Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        (directory / f"{name}.toml").write_text(text, encoding="utf-8")
    return directory


BASE = """
service_name = "crm-etl"
[databases.crm]
url = "postgresql+psycopg://etl:${env:CRM_PW}@db/crm"
options = { pool_size = 5, echo = false }
[bookkeeping]
database = "crm"
[modules.crm]
module = "modules.crm:module"
settings = { batch_size = 500 }
"""


class TestLayers:

    def test_files_then_environment_then_arguments(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = _write(
            tmp_path,
            base=BASE,
            prod=(
                '[logging]\nformat = "json"\n[databases.crm]\noptions = {'
                " pool_size = 20 }\n"
            ),
        )
        monkeypatch.setenv("CRM_PW", "s3cret")
        monkeypatch.setenv("ETL__LOGGING__LEVEL", "DEBUG")

        settings = config.load_settings(
            "prod", directory=directory, overrides={"service_name": "cli"}
        )

        crm = settings.databases["crm"]
        assert (
            crm.url.get_secret_value()
            == "postgresql+psycopg://etl:s3cret@db/crm"
        )
        # Deep merge: prod changes the pool, keeps the rest of base.
        assert crm.options.arguments() == {"pool_size": 20, "echo": False}
        assert (settings.logging.format, settings.logging.level) == (
            "json",
            "DEBUG",
        )
        assert (settings.env, settings.service_name) == ("prod", "cli")

    def test_the_environment_variable_chooses_the_environment(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = _write(tmp_path, test='service_name = "from-test"\n')
        monkeypatch.setenv("ETL_ENV", "test")

        settings = config.load_settings(directory=directory)

        assert (settings.env, settings.service_name) == ("test", "from-test")

    def test_no_file_gives_the_defaults(self, tmp_path: pathlib.Path) -> None:
        settings = config.load_settings("dev", directory=tmp_path)

        assert settings.databases == {}
        assert settings.logging.format == "console"


class TestSecrets:

    def test_secrets_are_hidden_when_shown(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        secret = tmp_path / "key"
        secret.write_text("k3y\n", encoding="utf-8")
        directory = _write(
            tmp_path / "config",
            # Before the first table, as a top-level key.
            base=f'key_secret = "${{file:{secret}}}"\n' + BASE,
        )
        monkeypatch.setenv("CRM_PW", "s3cret")

        settings = config.load_settings("dev", directory=directory)

        assert settings.key_secret is not None
        assert settings.key_secret.get_secret_value() == "k3y"
        shown = json.dumps(settings.redacted())
        assert "s3cret" not in shown
        assert "k3y" not in shown
        assert "s3cret" not in repr(settings)

    def test_custom_providers_and_literal_dollars(
        self, tmp_path: pathlib.Path
    ) -> None:
        config.register_provider("vault", lambda ref: f"from-vault:{ref}")
        directory = _write(
            tmp_path,
            base=(
                '[databases.a]\nurl = "${vault:crm/db}"\n'
                '[databases.b]\nurl = "sqlite:///$${not-a-secret}.db"\n'
            ),
        )

        settings = config.load_settings("dev", directory=directory)

        assert (
            settings.databases["a"].url.get_secret_value()
            == "from-vault:crm/db"
        )
        assert settings.databases["b"].url.get_secret_value() == (
            "sqlite:///${not-a-secret}.db"
        )

    def test_a_missing_secret_names_its_setting(
        self, tmp_path: pathlib.Path
    ) -> None:
        directory = _write(
            tmp_path, base='[databases.crm]\nurl = "${env:NOT_SET_ANYWHERE}"\n'
        )

        with pytest.raises(config.ConfigError, match="databases.crm.url"):
            config.load_settings("dev", directory=directory)


class TestErrors:

    def test_every_problem_is_listed_with_its_path(
        self, tmp_path: pathlib.Path
    ) -> None:
        directory = _write(
            tmp_path,
            base=(
                '[logging]\nformatt = "json"\n[databases.crm]\npool = 1\n'
                '[stores.landing]\nkind = "local"\n'
            ),
        )

        with pytest.raises(config.ConfigError) as caught:
            config.load_settings("dev", directory=directory)

        problems = caught.value.problems
        assert "logging.formatt: Extra inputs are not permitted" in problems
        assert "databases.crm.url: Field required" in problems
        assert any(
            p.startswith("stores.landing") and "path" in p for p in problems
        )

    def test_bookkeeping_must_name_a_database(
        self, tmp_path: pathlib.Path
    ) -> None:
        directory = _write(tmp_path, base='[bookkeeping]\ndatabase = "nope"\n')

        with pytest.raises(config.ConfigError, match="'nope' is not one of"):
            config.load_settings("dev", directory=directory)

    def test_debezium_changes_need_a_known_kafka_cluster(
        self, tmp_path: pathlib.Path
    ) -> None:
        legacy = '[databases.legacy]\nurl = "sqlite://"\n'
        incomplete = _write(
            tmp_path / "a",
            base=legacy + '[databases.legacy.changes]\nkind = "debezium"\n',
        )
        unknown = _write(
            tmp_path / "b",
            base=legacy
            + "[databases.legacy.changes]\n"
            + 'kind = "debezium"\nkafka = "main"\ntopic_prefix = "legacy"\n',
        )

        with pytest.raises(config.ConfigError, match="need a kafka cluster"):
            config.load_settings("dev", directory=incomplete)
        with pytest.raises(config.ConfigError, match="'main' is not one of"):
            config.load_settings("dev", directory=unknown)

    def test_kafka_credentials_are_hidden(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("KAFKA_PW", "s3cret")
        directory = _write(
            tmp_path,
            base=(
                '[kafka.main]\nbootstrap_servers = "k1:9092,k2:9092"\n'
                'options = { "security.protocol" = "SASL_SSL" }\n'
                'credentials = { "sasl.password" = "${env:KAFKA_PW}" }\n'
            ),
        )

        settings = config.load_settings("dev", directory=directory)

        assert settings.kafka["main"].librdkafka() == {
            "bootstrap.servers": "k1:9092,k2:9092",
            "security.protocol": "SASL_SSL",
            "sasl.password": "s3cret",
        }
        assert "s3cret" not in json.dumps(settings.redacted())

    def test_anonymizing_needs_a_secret_and_never_runs_in_prod(
        self, tmp_path: pathlib.Path
    ) -> None:
        unkeyed = _write(tmp_path / "a", base="[privacy]\nanonymize = true\n")
        keyed = _write(
            tmp_path / "b",
            base='[privacy]\nanonymize = true\nsecret = "s3cret"\n',
        )

        with pytest.raises(config.ConfigError, match="needs privacy.secret"):
            config.load_settings("uat", directory=unkeyed)
        with pytest.raises(config.ConfigError, match="refused in prod"):
            config.load_settings("prod", directory=keyed)
        assert (
            config.load_settings("uat", directory=keyed).anonymization_secret()
            == b"s3cret"
        )
        assert (
            config.load_settings(
                "uat", directory=unkeyed / "none"
            ).anonymization_secret()
            is None
        )


class _CrmSettings(pydantic.BaseModel):
    batch_size: int = 1000
    source: str


def test_modules_validate_their_own_section(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CRM_PW", "x")
    settings = config.load_settings(
        "dev", directory=_write(tmp_path, base=BASE)
    )

    with pytest.raises(config.ConfigError, match="modules.crm.settings.source"):
        settings.module_settings("crm", _CrmSettings)

    monkeypatch.setenv("ETL__MODULES__CRM__SETTINGS__SOURCE", "legacy")
    settings = config.load_settings(
        "dev", directory=_write(tmp_path, base=BASE)
    )
    assert settings.module_settings("crm", _CrmSettings) == _CrmSettings(
        batch_size=500, source="legacy"
    )


class TestCli:

    def test_config_show(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CRM_PW", "s3cret")
        out = io.StringIO()

        code = run.main(
            [
                "--env",
                "dev",
                "--config",
                str(_write(tmp_path, base=BASE)),
                "config",
                "show",
            ],
            out,
        )

        assert code == 0
        shown = json.loads(out.getvalue())
        assert shown["databases"]["crm"]["url"] == "**********"
        assert "s3cret" not in out.getvalue()

    def test_invalid_settings_exit_with_2(self, tmp_path: pathlib.Path) -> None:
        out = io.StringIO()
        directory = _write(tmp_path, base="nonsense = 1\n")

        assert (
            run.main(["--config", str(directory), "config", "show"], out) == 2
        )
        assert "nonsense: Extra inputs are not permitted" in out.getvalue()
