
from app.conf import app_config
from app.lib.db_tool.pymongo_tool import get_mongo_client


def create_ts_collection():
    """
    doesn't work due to the data granularity don't match
    :return:
    """
    db_client = get_mongo_client()
    db_name = app_config.MONGODB_SETTINGS['db']
    db = db_client[db_name]
    col_list = db.list_collection_names()
    if "stock_daily_quote" in col_list:
        print("Collection stock_daily_quote already exist.")
    else:
        db.create_collection("stock_daily_quote",
                             time_series={
                                 "timeseries": {
                                     "timeField": "date",
                                     "metaField": "metadata",
                                     "granularity": "minutes"       # don't have minute level data for now
                                 }
                             })
        print("Created time-series collection stock_daily_quote.")


def run_setup():
    create_ts_collection()


if __name__ == '__main__':
    run_setup()
