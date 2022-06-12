import re
import logging
import datetime
import baostock as bs


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
    conn = bs.login()
    if conn.error_code != "0":
        logger.error(f"Error connecting Baostock: {conn.error_msg}")
    return conn


def terminate_baostock_conn():
    bs.logout()


def get_zh_a_stock_hist_k_data(code, start_date=None, end_date=None, adjustflag="3"):
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
    result = bs.query_history_k_data_plus(new_code,
                                          res_fields,
                                          start_date=start_date,
                                          end_date=end_date,
                                          frequency="d",
                                          adjustflag=adjustflag)

    return result.get_data()


if __name__ == '__main__':
    bs_conn = establish_baostock_conn()
    res = get_zh_a_stock_hist_k_data('sh601166', adjustflag="1")
    terminate_baostock_conn()
    print(res)
