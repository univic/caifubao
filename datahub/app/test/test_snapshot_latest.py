# -*- coding: utf-8 -*-
"""Latest-snapshot discovery for object-store imports."""

import datetime
from pathlib import Path

from app.lib.datahub import snapshot_transfer as st

UTC = datetime.timezone.utc


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return self._pages


class _FakeLister:
    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self._pages)


def _object(key, moment):
    return {"Key": key, "LastModified": moment}


def _patch_lister(monkeypatch, pages):
    monkeypatch.setattr(
        st, "_build_s3_client", lambda *_args, **_kwargs: _FakeLister(pages)
    )
    for name in (
        "DATA_LAKE_ENDPOINT_URL",
        "DATA_LAKE_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(name, raising=False)


def test_list_snapshot_uris_returns_newest_first(monkeypatch):
    pages = [
        {
            "Contents": [
                _object(
                    "snapshots/snapshot-old/manifest.json",
                    datetime.datetime(2026, 10, 1, tzinfo=UTC),
                ),
                _object(
                    "snapshots/snapshot-old/finance_market.jsonl.gz",
                    datetime.datetime(2026, 10, 1, tzinfo=UTC),
                ),
                _object(
                    "snapshots/snapshot-new/manifest.json",
                    datetime.datetime(2026, 10, 22, tzinfo=UTC),
                ),
                _object(
                    "snapshots/snapshot-new/finance_market.jsonl.gz",
                    datetime.datetime(2026, 10, 22, 1, tzinfo=UTC),
                ),
                _object(
                    "snapshots/notes.txt", datetime.datetime(2026, 10, 23, tzinfo=UTC)
                ),
            ]
        }
    ]
    _patch_lister(monkeypatch, pages)

    uris = st.list_snapshot_uris("s3://bucket/snapshots")

    assert uris == [
        "s3://bucket/snapshots/snapshot-new",
        "s3://bucket/snapshots/snapshot-old",
    ]


def test_list_snapshot_uris_accepts_an_exact_snapshot_uri(monkeypatch):
    pages = [
        {
            "Contents": [
                _object(
                    "base/snapshot-exact/manifest.json",
                    datetime.datetime(2026, 10, 22, tzinfo=UTC),
                )
            ]
        }
    ]
    _patch_lister(monkeypatch, pages)

    assert st.list_snapshot_uris("s3://bucket/base/snapshot-exact") == [
        "s3://bucket/base/snapshot-exact"
    ]


def test_list_snapshot_uris_returns_empty_on_empty_prefix(monkeypatch):
    _patch_lister(monkeypatch, [{"Contents": []}])

    assert st.list_snapshot_uris("s3://bucket/snapshots") == []


def test_import_runner_resolves_latest_uri(monkeypatch, tmp_path):
    from app.jobs import snapshot_import_runner

    monkeypatch.setattr(
        st, "list_snapshot_uris", lambda uri: ["s3://bucket/base/snapshot-new"]
    )
    captured = {}

    def fake_download(uri, dest):
        captured["uri"] = uri
        captured["dest"] = Path(dest)
        return Path(dest)

    monkeypatch.setattr(st, "download_snapshot", fake_download)
    monkeypatch.setattr(
        st,
        "download_dir_for_uri",
        lambda base, uri: Path(tmp_path) / "downloads" / Path(uri).name,
    )
    monkeypatch.setattr(st, "_get_local_db", lambda: object())
    monkeypatch.setattr(
        st, "run_import", lambda **kwargs: {"snapshot_id": "snapshot-new"}
    )

    args = type(
        "Args",
        (),
        {
            "snapshot_dir": None,
            "snapshot_uri": "s3://bucket/base",
            "snapshot_id": "latest",
            "dry_run": True,
        },
    )()
    result = snapshot_import_runner.run_import(args)

    assert captured["uri"] == "s3://bucket/base/snapshot-new"
    assert result == {"snapshot_id": "snapshot-new"}
