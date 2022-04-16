from mongoengine import EmbeddedDocument, DatetimeField


class MetaData(EmbeddedDocument):
    last_update = DatetimeField()
    date_of_most_recent_daily_quote = DatetimeField()

