# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-02-03

# Flask-Security password salt (required for argon2 hash)
import os

SECURITY_PASSWORD_SALT = os.getenv(
    "SECURITY_PASSWORD_SALT", "default-salt-change-in-production"
)

# Use sha256_crypt instead of argon2 for simpler deployment (argon2 requires SECURITY_PASSWORD_SALT)
SECURITY_PASSWORD_HASH = os.getenv("SECURITY_PASSWORD_HASH", "sha256_crypt")

# no forms so no concept of flashing
SECURITY_FLASH_MESSAGES = False

# Need to be able to route backend flask API calls. Use 'accounts'
# to be the Flask-Security endpoints.
SECURITY_URL_PREFIX = "/api/accounts"

# Turn on all the great Flask-Security features
SECURITY_RECOVERABLE = True
SECURITY_TRACKABLE = True
SECURITY_CHANGEABLE = True
SECURITY_CONFIRMABLE = True
SECURITY_REGISTERABLE = True
SECURITY_UNIFIED_SIGNIN = True

# These need to be defined to handle redirects
# As defined in the API documentation - they will receive the relevant context
SECURITY_POST_CONFIRM_VIEW = "/confirmed"
SECURITY_CONFIRM_ERROR_VIEW = "/confirm-error"
SECURITY_RESET_VIEW = "/reset-password"
SECURITY_RESET_ERROR_VIEW = "/reset-password"
SECURITY_REDIRECT_BEHAVIOR = "spa"

# CSRF protection is critical for all session-based browser UIs

# enforce CSRF protection for session / browser - but allow token-based
# API calls to go through
SECURITY_CSRF_PROTECT_MECHANISMS = ["session", "basic"]
SECURITY_CSRF_IGNORE_UNAUTH_ENDPOINTS = True

# Send Cookie with csrf-token. This is the default for Axios and Angular.
SECURITY_CSRF_COOKIE = {"key": "XSRF-TOKEN"}
WTF_CSRF_CHECK_DEFAULT = False
WTF_CSRF_TIME_LIMIT = None


# Optionally define and set unauthorized callbacks
# In your app
# Enable CSRF on all api endpoints.
# flask_wtf.CSRFProtect(app)
#
# # Initialize Flask-Security
# user_datastore = SQLAlchemyUserDatastore(db, User, Role)
# security = Security(app, user_datastore)
