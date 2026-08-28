"""Tests for the MongoDB connection retry in MongoWatcher.connect_to_db."""

from unittest.mock import MagicMock

import pytest


def _make_config(host="mongo", port=27017, name="caifubao", user="root", password="pw"):
    cfg = MagicMock()
    cfg.MONGODB_HOST = host
    cfg.MONGODB_PORT = port
    cfg.MONGODB_DB = name
    cfg.MONGODB_USERNAME = user
    cfg.MONGODB_PASSWORD = password
    cfg.MONGODB_CONNECT_TIMEOUT_MS = 1000
    cfg.MONGODB_SERVER_SELECTION_TIMEOUT_MS = 1000
    return cfg


def _patch_config(monkeypatch, cfg):
    from app.lib.db_watcher import mongoengine_tool

    monkeypatch.setattr(mongoengine_tool, "app_config", cfg)


def test_connect_succeeds_on_first_attempt(monkeypatch):
    from app.lib.db_watcher import mongoengine_tool
    from app.lib.db_watcher.mongoengine_tool import MongoWatcher

    _patch_config(monkeypatch, _make_config())
    watcher = MongoWatcher()
    fake_conn = MagicMock()
    monkeypatch.setattr(mongoengine_tool, "connect", lambda **kw: fake_conn)
    monkeypatch.setattr(
        mongoengine_tool,
        "ConnTestDocument",
        type("FakeConnTest", (), {"objects": MagicMock()}),
    )

    watcher.connect_to_db()

    assert watcher.db_conn is fake_conn


def test_connect_retries_then_succeeds(monkeypatch):
    from app.lib.db_watcher import mongoengine_tool
    from app.lib.db_watcher.mongoengine_tool import MongoWatcher

    _patch_config(monkeypatch, _make_config())
    monkeypatch.setattr(
        mongoengine_tool,
        "MONGODB_CONNECT_RETRY_DELAY_SECONDS",
        0,
    )
    watcher = MongoWatcher()
    fake_conn = MagicMock()
    calls = {"n": 0}

    def flaky_connect(**kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("connection refused")
        return fake_conn

    monkeypatch.setattr(mongoengine_tool, "connect", flaky_connect)
    monkeypatch.setattr(
        mongoengine_tool,
        "ConnTestDocument",
        type("FakeConnTest", (), {"objects": MagicMock()}),
    )

    watcher.connect_to_db()

    assert calls["n"] == 3
    assert watcher.db_conn is fake_conn


def test_connect_exits_after_all_attempts_fail(monkeypatch):
    from app.lib.db_watcher import mongoengine_tool
    from app.lib.db_watcher.mongoengine_tool import MongoWatcher

    _patch_config(monkeypatch, _make_config())
    monkeypatch.setattr(
        mongoengine_tool,
        "MONGODB_CONNECT_RETRY_DELAY_SECONDS",
        0,
    )
    monkeypatch.setattr(
        mongoengine_tool,
        "MONGODB_CONNECT_RETRIES",
        3,
    )
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
