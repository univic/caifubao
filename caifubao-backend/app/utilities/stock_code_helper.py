import re
import logging


logger = logging.getLogger()


def add_market_prefix(stock_code):
    new_stock_code = None
    sh_re_pattern = r"6[0-9]{5}"
    sz_re_pattern = r"[03][0-9]{5}"
    bj_re_pattern = r"[48][0-9]{5}"
    if re.match(sh_re_pattern, stock_code):
        new_stock_code = "sh" + str(stock_code)
    elif re.match(sz_re_pattern, stock_code):
        new_stock_code = "sz" + str(stock_code)
    elif re.match(bj_re_pattern, stock_code):
        new_stock_code = "bj" + str(stock_code)
    else:
        logger.warning(f'invalid stock code pattern')
    return new_stock_code
