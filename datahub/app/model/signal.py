from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    FloatField,
    GenericLazyReferenceField,
    ReferenceField,
    StringField,
)

from app.model.stock import BasicStock


class Signal(Document):
    """
    category: 0: spot, 1: cont
    """

    code = StringField()
    name = StringField()
    category = StringField()


class SignalData(Document):
    # meta = {
    #     'allow_inheritance': True,
    #     # 'indexes': [
    #     #     '-date',
    #     #     ('name', 'code')
    #     # ]
    # }
    stock = ReferenceField(BasicStock)
    stock_name = StringField()
    stock_code = StringField()
    name = StringField(unique_with=["date", "code"])
    code = StringField()
    date = DateTimeField()


class StockSignalDaily(Document):
    meta = {
        "collection": "stock_signal_daily",
        "indexes": [
            {"fields": ["stock_code", "date", "signal_name"], "unique": True},
            ("date", "signal_name"),
            ("stock_code", "-date"),
            ("signal_name", "-date"),
            ("direction", "-date"),
        ],
    }

    stock = GenericLazyReferenceField()
    stock_code = StringField(required=True)
    stock_name = StringField()
    category = StringField(default="stock")
    date = DateTimeField(required=True)
    signal_name = StringField(required=True)
    signal_version = StringField(default="v1")
    direction = StringField(required=True)
    signal_type = StringField(required=True)
    strength = FloatField()
    reason = StringField()
    price_snapshot = DictField()
    factor_snapshot = DictField()
    source_freshness = DictField()
    generated_at = DateTimeField()
