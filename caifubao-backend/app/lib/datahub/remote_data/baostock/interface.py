import logging
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


def query_history_k_data():
    """
    获取历史K线，返回Dataframe

    """
    bs_conn = establish_baostock_conn()
    res_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus," \
                 "pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST"
    res = baostock.query_history_k_data_plus("sz.000001",
                                             res_fields,
                                             start_date='2012-07-01',
                                             end_date='2015-12-31',
                                             frequency="d",
                                             adjustflag="3")
    terminate_baostock_conn()
    return res.get_data()


if __name__ == '__main__':
    res = query_history_k_data()
    print(res)
