# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-02-03

from flask_security import MongoEngineUserDatastore, Security
from app.lib.database import db
from app.model.user_role import UserRole
from app.model.user_model import User


def init_flask_security(app):
    user_datastore = MongoEngineUserDatastore(db, User, UserRole)
    security = Security(app, user_datastore)
