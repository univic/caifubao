import datetime


def update_metadata(data, df, column):
    max_data = max(df[column])
    data.meta_data.last_update = datetime.datetime.now()
    data.meta_data.date_of_most_recent_data = max_data
