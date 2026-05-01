import datetime
from mongoengine import Document, StringField, DateTimeField


class EventLog(Document):
    """
    status: CRTD-created, PEND-pending, FAIL-failed, COMP-completed
    """

    code = StringField(required=True)
    name = StringField(required=True)
    # stock_obj = EmbeddedDocumentField(BasicStock)
    module = StringField(required=True)
    meta_type = StringField()
    meta_name = StringField()
    log_level = StringField()
    message = StringField()
    created_at = DateTimeField(default=datetime.datetime.now())
