import akshare


def get_trade_date_hist():
    df = akshare.tool_trade_date_hist_sina()
    return df


def stock_sse_summary():
    df = akshare.stock_sse_summary()
    return df


def sse_daily_summary():
    df = akshare.stock_sse_deal_daily(date="20170608")
    return df


def zh_stock_index_spot():
    """
    单次返回所有指数的实时行情数据
    :return:
    """
    df = akshare.stock_zh_index_spot()
    return df


def stock_zh_index_daily(code):
    """
    单次返回指定指数的所有历史行情数据
    :return:
    """
    df = akshare.stock_zh_index_daily(code)
    return df


def stock_zh_a_spot():
    """
    单次返回所有沪深京 A 股上市公司的实时行情数据（新浪接口）
    :return:
    """
    df = akshare.stock_zh_a_spot()
    return df


def stock_zh_a_spot_em():
    """
    单次返回所有沪深京 A 股上市公司的实时行情数据（东财接口）
    :return:
    """
    df = akshare.stock_zh_a_spot_em()
    return df


def stock_zh_a_hist(code, start_date=None, end_date=None):
    """单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据"""
    if start_date:
        df = akshare.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date)
    else:
        df = akshare.stock_zh_a_hist(symbol=code, period="daily")
    return df


def stock_zh_a_hist_163(code, start_date=None, end_date=None):
    """单次返回指定沪深 A 股（不包含北交所）上市公司指定日期间的历史行情日频率数据, 该接口只返回未复权数据"""
    if start_date:
        df = akshare.stock_zh_a_hist_163(symbol=code, start_date=start_date, end_date=end_date)
    else:
        df = akshare.stock_zh_a_hist_163(symbol=code)
    return df


if __name__ == "__main__":
    o = stock_zh_a_spot_em()
    print(o)
