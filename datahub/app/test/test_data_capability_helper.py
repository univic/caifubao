from types import SimpleNamespace


def _stock(code, daily_quote=None, fq_factor=None, ma_factor=None):
    if daily_quote is None and fq_factor is None and ma_factor is None:
        capabilities = None
    else:
        capabilities = SimpleNamespace(
            daily_quote=daily_quote,
            fq_factor=fq_factor,
            ma_factor=ma_factor,
        )
    return SimpleNamespace(code=code, data_capabilities=capabilities)


def test_bse_codes_default_to_unsupported_capabilities():
    from app.lib.utilities import data_capability_helper

    assert data_capability_helper.default_capability_payload("bj920118") == {
        "daily_quote": False,
        "fq_factor": False,
        "ma_factor": False,
    }
    assert data_capability_helper.default_capability_payload("920118") == {
        "daily_quote": False,
        "fq_factor": False,
        "ma_factor": False,
    }


def test_sh_sz_codes_default_to_supported_capabilities():
    from app.lib.utilities import data_capability_helper

    assert data_capability_helper.default_capability_payload("sh600000") == {
        "daily_quote": True,
        "fq_factor": True,
        "ma_factor": True,
    }
    assert data_capability_helper.default_capability_payload("sz000001") == {
        "daily_quote": True,
        "fq_factor": True,
        "ma_factor": True,
    }


def test_explicit_stock_capabilities_override_code_fallback():
    from app.lib.utilities import data_capability_helper

    assert (
        data_capability_helper.stock_supports(
            _stock("sh600000", daily_quote=False, fq_factor=True, ma_factor=True),
            "daily_quote",
        )
        is False
    )
    assert (
        data_capability_helper.stock_supports(_stock("bj920118"), "daily_quote")
        is False
    )
