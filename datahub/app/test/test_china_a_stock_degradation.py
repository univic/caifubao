# -*- coding: utf-8 -*-
"""New-listing accounting for the bounded quote degradation policy."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from app.lib.datahub.processors import china_a_stock


class _LocalList(list):
    def count(self):
        return len(self)


def test_new_listing_is_an_attempted_symbol() -> None:
    processor = object.__new__(china_a_stock.ChinaAStock)
    processor.market = SimpleNamespace(name="ChinaAStock")
    processor.perform_date_check = lambda: None
    processor.most_recent_trading_day = None
    processor.check_data_freshness = lambda stock_obj, precomputed_latest_date=None: (
        "SKIP"
    )
    processor.sync_stock_data_capabilities = lambda stock_obj: False
    processor.perform_stock_name_check = lambda stock_obj, name: None
    processor.handle_new_stock = lambda **kwargs: {
        "code": "GOOD",
        "written_count": 1,
        "validated_count": 1,
        "freshness_status": "OK",
    }

    captured = {}

    def fake_evaluate(**kwargs):
        captured.update(kwargs)
        return {
            "degraded": False,
            "failure_reason": None,
            "attempted_symbols": kwargs["attempted_symbols"],
            "validated_symbols": kwargs["validated_symbols"],
            "failed_count": len(kwargs["failed_codes"]),
            "failed_codes": list(kwargs["failed_codes"]),
            "coverage_rate": 1.0,
            "strict": False,
        }

    local_stock = SimpleNamespace(
        code="sh600000",
        name="existing",
        active_status=0,
        data_capabilities=None,
        save=lambda: None,
    )
    remote = pd.DataFrame(
        [
            {"code": "sh600000", "name": "existing", "close": 10.0},
            {"code": "sh600519", "name": "new", "close": 10.0},
        ]
    )
    with (
        patch.object(
            china_a_stock.data_asset_status_helper,
            "read_quote_status_map",
            return_value={},
        ),
        patch.object(
            china_a_stock.pipeline_degradation,
            "evaluate_validation",
            side_effect=fake_evaluate,
        ),
    ):
        processor.check_data_integrity(
            obj_type="stock",
            local_data_list=_LocalList([local_stock]),
            remote_data_df=remote,
            hist_handler="get_hist_stock_quote_data",
            allow_update=True,
        )

    assert captured["attempted_codes"] == {"sh600519"}
    assert captured["validated_symbols"] == 1
    assert captured["attempted_symbols"] == 1
    assert not captured["failed_codes"]
