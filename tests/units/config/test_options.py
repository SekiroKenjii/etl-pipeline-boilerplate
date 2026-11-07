"""Free-form options: checked when the settings load, secrets never shown."""

from __future__ import annotations

import io
import json
import pathlib
from typing import Any

import pytest

from app import bootstrap
from cli import run
import config
from modules.crm import settings as crm_settings


def _load(**values: Any) -> config.AppSettings:
    return config.load_settings(
        "test", directory="/nonexistent", overrides=values
    )


def _write(directory: pathlib.Path, text: str) -> pathlib.Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "base.toml").write_text(text, encoding="utf-8")
    return directory


class TestSecretsInOptions:

    def test_config_show_hides_secrets_of_every_free_form_section(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("APP_NAME", "from-the-environment")
        directory = _write(
            tmp_path,
            "[databases.crm]\n"
            'url = "postgresql+psycopg://db/crm"\n'
            "[databases.crm.options.connect_args]\n"
            'password = "S3cret-db"\n'
            'application_name = "${env:APP_NAME}"\n'
            "[kafka.main]\n"
            'bootstrap_servers = "k:9092"\n'
            'options = { "sasl.password" = "S3cret-kafka" }\n'
            "[lineage]\n"
            'endpoint = "http://etl:S3cret-url@marquez:5000"\n'
            "[modules.crm]\n"
            'settings = { api_token = "S3cret-module" }\n',
        )
        out = io.StringIO()

        code = run.main(
            ["--env", "dev", "--config", str(directory), "config", "show"], out
        )

        shown = out.getvalue()
        assert code == 0
        assert "S3cret" not in shown
        assert "from-the-environment" not in shown
        data = json.loads(shown)
        crm = data["databases"]["crm"]["options"]["connect_args"]
        assert crm == {
            "password": "**********",
            # Shown as written: the placeholder, not what it resolves to.
            "application_name": "${env:APP_NAME}",
        }
        assert data["kafka"]["main"]["options"]["sasl.password"] == "**********"
        assert data["modules"]["crm"]["settings"]["api_token"] == "**********"

    def test_secrets_reach_the_drivers(self) -> None:
        settings = _load(
            databases={
                "crm": {
                    "url": "postgresql+psycopg://db/crm",
                    "options": {
                        "pool_size": "7",
                        "connect_args": {
                            "password": "pw",
                            "sslmode": "require",
                        },
                    },
                },
                "docs": {
                    "url": "mongodb://db/docs",
                    "options": {"maxPoolSize": 4, "password": "mongo-pw"},
                },
            },
            kafka={
                "main": {
                    "bootstrap_servers": "k:9092",
                    "options": {"sasl.password": "k-pw", "acks": "all"},
                }
            },
        )

        assert settings.databases["crm"].options.arguments() == {
            "pool_size": 7,
            "connect_args": {"password": "pw", "sslmode": "require"},
        }
        assert settings.databases["docs"].options.arguments() == {
            "maxPoolSize": 4,
            "password": "mongo-pw",
        }
        assert settings.kafka["main"].librdkafka() == {
            "bootstrap.servers": "k:9092",
            "sasl.password": "k-pw",
            "acks": "all",
        }
        assert "pw" not in repr(settings.databases["crm"].options)

    def test_the_settings_hash_ignores_secrets(self) -> None:
        def meta(password: str) -> str:
            settings = _load(
                databases={
                    "crm": {
                        "url": "postgresql+psycopg://db/crm",
                        "options": {"connect_args": {"password": password}},
                    }
                },
                modules={"crm": {"settings": {"api_token": password}}},
            )
            return bootstrap.run_meta(settings)["settings"]

        assert meta("one") == meta("two")

    @pytest.mark.parametrize(
        "key",
        [
            "password",
            "sasl.password",
            "client_secret",
            "x-api-key",
            "apiKey",
            "tlsCertificateKeyFilePassword",
            "ssl.key.pem",
            "credentials",
            "sig",
        ],
    )
    def test_secret_names(self, key: str) -> None:
        assert config.is_secret_name(key)

    @pytest.mark.parametrize(
        "key", ["key", "signal", "pool_size", "application_name", "sslmode"]
    )
    def test_not_secret_names(self, key: str) -> None:
        assert not config.is_secret_name(key)


class TestCheckedOptions:

    def test_a_mistyped_option_fails_when_the_settings_load(
        self, tmp_path: pathlib.Path
    ) -> None:
        directory = _write(
            tmp_path,
            '[databases.crm]\nurl = "sqlite://"\n'
            "options = { pool_sizee = 5, pool_timeout = -1 }\n",
        )
        out = io.StringIO()

        code = run.main(["--config", str(directory), "config", "show"], out)

        assert code == 2
        assert (
            "databases.crm.options.pool_sizee: Extra inputs are not permitted"
            in out.getvalue()
        )
        assert "databases.crm.options.pool_timeout" in out.getvalue()

    def test_mongodb_takes_the_options_of_its_client(self) -> None:
        with pytest.raises(config.ConfigError, match="options.pool_size"):
            _load(
                databases={
                    "docs": {
                        "url": "mongodb://db/x",
                        "options": {"pool_size": 5},
                    }
                }
            )

    def test_options_of_one_dialect_go_through(self) -> None:
        settings = _load(
            databases={
                "crm": {
                    "url": "postgresql+psycopg://db/crm",
                    "options": {"dialect_options": {"client_encoding": "utf8"}},
                }
            }
        )

        assert settings.databases["crm"].options.arguments() == {
            "client_encoding": "utf8"
        }
        with pytest.raises(config.ConfigError, match="options of their own"):
            _load(
                databases={
                    "crm": {
                        "url": "postgresql+psycopg://db/crm",
                        "options": {"dialect_options": {"pool_size": 3}},
                    }
                }
            )

    def test_poll_seconds_is_gone(self) -> None:
        with pytest.raises(config.ConfigError, match="runs.poll_seconds"):
            _load(runs={"poll_seconds": 5})

    def test_page_size(self) -> None:
        assert _load(databases={"a": {"url": "sqlite://"}}).databases[
            "a"
        ].page_size == (1_000)
        with pytest.raises(config.ConfigError, match="page_size"):
            _load(databases={"a": {"url": "sqlite://", "page_size": 0}})


class TestPools:

    def _crm(self, **values: Any) -> crm_settings.CrmSettings:
        settings = _load(
            databases={
                "legacy": {"url": "mssql+pyodbc://db/legacy"},
                "crm": {
                    "url": "mssql+pyodbc://db/crm",
                    **values.pop("crm", {}),
                },
            },
            bookkeeping={"database": "crm"},
            **values,
        )
        return settings.module_settings("crm", crm_settings.CrmSettings)

    def test_parallel_ranges_need_a_pool_that_holds_them(self) -> None:
        with pytest.raises(config.ConfigError) as caught:
            self._crm(modules={"crm": {"settings": {"partitions": 16}}})

        message = str(caught.value)
        # 16 ranges: a transaction and a bulk copy each, plus the records.
        assert "databases.crm: its pool opens 15 connections" in message
        assert "need 34" in message
        assert "databases.crm.options.max_overflow to 29 or more" in message
        assert "databases.legacy.options.max_overflow to 11" in message

    def test_a_large_enough_pool_or_fewer_steps_pass(self) -> None:
        ranges = {"modules": {"crm": {"settings": {"partitions": 16}}}}

        assert self._crm(**ranges, runs={"max_parallel_steps": 4}).partitions
        with pytest.raises(config.ConfigError, match="legacy"):
            self._crm(**ranges, crm={"options": {"max_overflow": 29}})
        assert self._crm(
            modules={"crm": {"settings": {"partitions": 6}}},
            crm={"options": {"pool_size": 10, "max_overflow": 4}},
        )

    def test_unlimited_pools_and_sqlite_pass(self) -> None:
        ranges = {"modules": {"crm": {"settings": {"partitions": 64}}}}
        settings = _load(
            databases={
                "legacy": {"url": "sqlite:///legacy.db"},
                "crm": {
                    "url": "mssql+pyodbc://db/crm",
                    "options": {"max_overflow": -1},
                },
            },
            **ranges,
        )

        assert settings.module_settings(
            "crm", crm_settings.CrmSettings
        ).partitions == (64)


class TestProduction:

    @pytest.mark.parametrize(
        "env", ["prod", "production", "PRD", "prod-eu", "eu_prd"]
    )
    def test_production_is_known_by_its_name(self, env: str) -> None:
        with pytest.raises(config.ConfigError, match="refused in production"):
            config.load_settings(
                env,
                directory="/nonexistent",
                overrides={"privacy": {"anonymize": True, "secret": "s"}},
            )

    @pytest.mark.parametrize("env", ["uat", "preprod", "nonprod", "dev"])
    def test_other_environments_may_anonymize(self, env: str) -> None:
        settings = config.load_settings(
            env,
            directory="/nonexistent",
            overrides={"privacy": {"anonymize": True, "secret": "s"}},
        )

        assert not settings.is_production

    def test_or_by_its_flag(self) -> None:
        with pytest.raises(config.ConfigError, match="refused in production"):
            config.load_settings(
                "live",
                directory="/nonexistent",
                overrides={
                    "production": True,
                    "privacy": {"anonymize": True, "secret": "s"},
                },
            )
