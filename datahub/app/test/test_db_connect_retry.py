"""Tests for MongoWatcher.connect_to_db retry (datahub side, mirrors #149)."""

from unittest.mock import MagicMock

import pytest

from app.lib.db_watcher import mongoengine_tool
from app.lib.db_watcher.mongoengine_tool import MongoWatcher


def _patch_config(monkeypatch):
    cfg = MagicMock()
    cfg.MONGODB_HOST = "mongo"
    cfg.MONGODB_PORT = 27017
    cfg.MONGODB_DB = "caifubao"
    cfg.MONGODB_USERNAME = "root"
    cfg.MONGODB_PASSWORD = "pw"
    monkeypatch.setattr(mongoengine_tool, "app_config", cfg)


def _patch_conn_test(monkeypatch):
    monkeypatch.setattr(
        mongoengine_tool,
        "ConnTestDocument",
        type("FakeConnTest", (), {"objects": MagicMock()}),
    )


def test_connect_succeeds_on_first_attempt(monkeypatch):
    _patch_config(monkeypatch)
    watcher = MongoWatcher()
    fake_conn = MagicMock()
    monkeypatch.setattr(mongoengine_tool, "connect", lambda **kw: fake_conn)
    _patch_conn_test(monkeypatch)

    watcher.connect_to_db()

    assert watcher.db_conn is fake_conn


def test_connect_retries_then_succeeds(monkeypatch):
    _patch_config(monkeypatch)
    monkeypatch.setattr(mongoengine_tool, "MONGODB_CONNECT_RETRY_DELAY_SECONDS", 0)
    watcher = MongoWatcher()
    fake_conn = MagicMock()
    calls = {"n": 0}

    def flaky_connect(**kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("connection refused")
        return fake_conn

    monkeypatch.setattr(mongoengine_tool, "connect", flaky_connect)
    _patch_conn_test(monkeypatch)

    watcher.connect_to_db()

    assert calls["n"] == 3
    assert watcher.db_conn is fake_conn


def test_connect_exits_after_all_attempts_fail(monkeypatch):
    _patch_config(monkeypatch)
    monkeypatch.setattr(mongoengine_tool, "MONGODB_CONNECT_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(mongoengine_tool, "MONGODB_CONNECT_RETRIES", 3)
    watcher = MongoWatcher()
    calls = {"n": 0}

    def always_fail(**kwargs):
        calls["n"] += 1
        raise RuntimeError("connection refused")

    monkeypatch.setattr(mongoengine_tool, "connect", always_fail)

    with pytest.raises(SystemExit) as excinfo:
        watcher.connect_to_db()

    assert excinfo.value.code == 1
    assert calls["n"] == 3
