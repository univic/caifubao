# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-07-20

def dbo_better_json(db_object):
    db_dict = db_object.to_mongo().to_dict()
    db_object = dbo_drop_id(db_dict)
    return db_object


# convert _id to serializable id str
def dbo_drop_id(db_dict):
    db_dict['id'] = str(db_dict['_id'])
    db_dict.pop('_id')
    return db_dict
