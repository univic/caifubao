# -*- coding: utf-8 -*-
"""JSON-friendly conversion for mongoengine documents (legacy db_util)."""


def dbo_better_json(db_object):
    db_dict = db_object.to_mongo().to_dict()
    return dbo_drop_id(db_dict)


def dbo_drop_id(db_dict):
    db_dict["id"] = str(db_dict["_id"])
    db_dict.pop("_id")
    return db_dict
