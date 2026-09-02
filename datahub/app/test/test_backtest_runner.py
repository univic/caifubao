from types import SimpleNamespace

import pytest

from app.jobs import backtest_runner


@pytest.mark.parametrize(
    "strategies",
    [
        ("SCORE_THRESHOLD",),
        ("SCORE_MOMENTUM",),
        ("MULTI_HORIZON_CONSENSUS",),
        ("TOP_N_ROTATION",),
        ("BUY_HOLD", "SCORE_THRESHOLD"),
    ],
)
def test_score_driven_cli_requires_model_version(strategies):
    args = SimpleNamespace(model_version=None)

    with pytest.raises(ValueError, match="model_version is required"):
        backtest_runner._require_score_model_version(args, *strategies)


def test_non_score_cli_does_not_require_model_version():
    args = SimpleNamespace(model_version=None)

    assert backtest_runner._require_score_model_version(args, "BUY_HOLD") is None


def test_score_driven_cli_accepts_explicit_model_version():
    args = SimpleNamespace(model_version="score_v2_202605b")

    assert (
        backtest_runner._require_score_model_version(args, "SCORE_THRESHOLD")
        == "score_v2_202605b"
    )
