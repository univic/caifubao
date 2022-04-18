from mongoengine import EmbeddedDocument, DateTimeField


class MetaData(EmbeddedDocument):
    last_update = DateTimeField()
    date_of_most_recent_daily_quote = DateTimeField()

