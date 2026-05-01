import re
import logging


logger = logging.getLogger(__name__)


def add_market_prefix(stock_code):
    new_stock_code = None
    stock_code_str = str(stock_code)

    if stock_code_str.startswith(("sh", "sz", "bj")):
        return stock_code_str

    sh_re_pattern = r"6[0-9]{5}"
    sz_re_pattern = r"[03][0-9]{5}"
    bj_re_pattern = r"[489][0-9]{5}"
    if re.match(sh_re_pattern, stock_code_str):
        new_stock_code = "sh" + stock_code_str
    elif re.match(sz_re_pattern, stock_code_str):
        new_stock_code = "sz" + stock_code_str
    elif re.match(bj_re_pattern, stock_code_str):
        new_stock_code = "bj" + stock_code_str
    else:
        logger.warning(f"invalid stock code pattern {stock_code}")
    return new_stock_code
