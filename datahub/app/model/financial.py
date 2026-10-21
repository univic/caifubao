# -*- coding: utf-8 -*-
"""Point-in-time financial indicator snapshots (research-only).

One document per ``(code, end_date, report_type, ann_date)`` vintage. Keeping
the announcement date in the identity preserves restatements: a research join
as of D can only use vintages with ``ann_date <= D``.
"""

from mongoengine import DateTimeField, Document, FloatField, IntField, StringField


class StockFinancialIndicator(Document):
    meta = {
        "collection": "stock_financial_indicator",
        "indexes": [
            {
                "fields": ["code", "end_date", "report_type", "ann_date"],
                "unique": True,
            },
            ("code", "-ann_date"),
            "-ann_date",
        ],
    }
    code = StringField(required=True)
    ann_date = DateTimeField(required=True)
    end_date = DateTimeField(required=True)
    report_type = IntField(required=True)
    roe = FloatField()
    roe_waa = FloatField()
    roa = FloatField()
    grossprofit_margin = FloatField()
    netprofit_margin = FloatField()
    assets_turn = FloatField()
    debt_to_assets = FloatField()
    netprofit_yoy = FloatField()
    or_yoy = FloatField()
    tr_yoy = FloatField()
    ocf_yoy = FloatField()
    q_ocf_to_sales = FloatField()
