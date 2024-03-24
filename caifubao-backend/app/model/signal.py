from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField
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
    name = StringField(unique_with=['date', 'code'])
    code = StringField()
    date = DateTimeField()


# class SpotSignalData(SignalData):
#     pass
#
#
# class ContinuousSignalData(SignalData):
#     active_status = StringField()
#     expire_date = StringField()
