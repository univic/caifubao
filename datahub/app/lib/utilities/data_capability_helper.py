import re


CAPABILITY_DAILY_QUOTE = "daily_quote"
CAPABILITY_FQ_FACTOR = "fq_factor"
CAPABILITY_MA_FACTOR = "ma_factor"
SUPPORTED_CAPABILITIES = (
    CAPABILITY_DAILY_QUOTE,
    CAPABILITY_FQ_FACTOR,
    CAPABILITY_MA_FACTOR,
)

BSE_CODE_PATTERN = re.compile(r"^(?:bj\d{6}|[489]\d{5})$", re.IGNORECASE)


def is_bse_stock_code(code):
    code_text = (code or "").strip().lower()
    return bool(code_text and BSE_CODE_PATTERN.match(code_text))


def default_capability_payload(code):
    is_supported = not is_bse_stock_code(code)
    return {capability: is_supported for capability in SUPPORTED_CAPABILITIES}


def stock_supports(stock, capability):
    capabilities = getattr(stock, "data_capabilities", None)
    value = getattr(capabilities, capability, None) if capabilities else None
    if value is None:
        value = default_capability_payload(getattr(stock, "code", "")).get(capability)
    return bool(value)


def supports_all(stock, capabilities=SUPPORTED_CAPABILITIES):
    return all(stock_supports(stock, capability) for capability in capabilities)
