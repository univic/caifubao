import re
import logging
import datetime
import baostock


logger = logging.getLogger()


# class BaostockConn(object):
#     def __init__(self):
#         self.conn_obj = None
#
#     def __enter__(self):
#         self.conn_obj = self.establish_baostock_conn()
#         return self.conn_obj
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.conn_obj.logout()
#
#     @classmethod


def establish_baostock_conn():
    bs_conn = baostock.login()
    if bs_conn.error_code != "0":
        logger.error(f"Error connecting Baostock: {bs_conn.error_msg}")
    return bs_conn


def terminate_baostock_conn():
    baostock.logout()


def get_zh_a_stock_hist_k_data(code, start_date=None, end_date=None):
    """
    获取历史K线，返回Dataframe

    """
    # convert date, assign the default data
    if start_date is None:
        start_date = "1990-01-01"
    if end_date is None:
        end_date = datetime.date.today().strftime('%Y-%m-%d')

    # convert stock code
    regex_pattern = r"(^[a-zA-Z]{2})"
    new_code = re.sub(regex_pattern, r"\1.", code)
    res_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus," \
                 "pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST"
    result = baostock.query_history_k_data_plus(new_code,
                                                res_fields,
                                                start_date=start_date,
                                                end_date=end_date,
                                                frequency="d",
                                                adjustflag="3")

    return result.get_data()


if __name__ == '__main__':
    bs_conn = establish_baostock_conn()
    res = get_zh_a_stock_hist_k_data('sz000001')
    terminate_baostock_conn()
    print(res)