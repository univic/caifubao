from mongoengine import Document, StringField


class Portfolio(Document):
    name = StringField()


class PortfolioTransaction(Document):
    pass
