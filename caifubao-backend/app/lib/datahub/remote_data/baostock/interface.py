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


def query_history_k_data(code, start_date, end_date):
    """
    获取历史K线，返回Dataframe

    """
    # TODO: DO STOCK CODE CONVERSION
    res_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus," \
                 "pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST"
    result = baostock.query_history_k_data_plus(code,
                                                res_fields,
                                                start_date=start_date,
                                                end_date=end_date,
                                                frequency="d",
                                                adjustflag="3")

    return result.get_data()


if __name__ == '__main__':
    bs_conn = establish_baostock_conn()
    res = query_history_k_data('sz000001', "2021-01-01", "2021-09-05")
    terminate_baostock_conn()
    print(res)
