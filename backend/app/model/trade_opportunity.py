from mongoengine import Document, StringField, DateTimeField


class TradeOpportunity(Document):
    name = StringField()
    date = DateTimeField()
    stock_code = StringField()
    direction = StringField()  # LONG or SHORT
