import datetime
from mongoengine import Document, StringField, DateTimeField, FloatField


class BackTest(Document):
    name = StringField(unique=True)
    strategy = StringField()
    start_date = DateTimeField()
    end_date = DateTimeField()
    created_at = DateTimeField(default=datetime.datetime.now())
    started_at = DateTimeField()
    completed_at = DateTimeField()
    status = StringField(default="CRTD")
    exec_result = StringField()
    exec_msg = StringField()
    earning_rate = FloatField()
