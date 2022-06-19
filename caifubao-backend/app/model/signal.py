from mongoengine import Document, StringField, EmbeddedDocumentListField, DateTimeField, ReferenceField, ListField, \
    EmbeddedDocument, FloatField, IntField, EmbeddedDocumentField


class Signal(Document):
    """
    category: 0: one-time, 1: cont
    """
    code = StringField()
    name = StringField()
    category = StringField


class SignalDataEntry(Document):
    meta = {
        'allow_inheritance': True,
        'indexes': [
            '-date',
            ('code', 'target_code')
        ]
    }
    code = StringField()
    signal = ReferenceField(Signal)
    date = DateTimeField()
    target_code = StringField()
    category = IntField()


class OneTimeSignalDataEntry(SignalDataEntry):
    pass


class ContinuousSignalDataEntry(SignalDataEntry):
    active_status = StringField()
    expire_date = StringField()
