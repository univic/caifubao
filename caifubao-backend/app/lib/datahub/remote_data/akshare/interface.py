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


def stock_zh_a_spot_em():
    """
    单次返回所有沪深京 A 股上市公司的实时行情数据
    :return:
    """
    df = akshare.stock_zh_a_spot_em()
    return df


if __name__ == "__main__":
    o = zh_stock_index_spot()
    print(o)
