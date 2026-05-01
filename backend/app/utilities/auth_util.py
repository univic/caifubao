# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

import hashlib
import secrets
import datetime
from app.model.service_token import ServiceToken


def hash_token(token: str) -> str:
    """
    Hash a plain text token using SHA-256.
    In a real production system, you might want to use a salt from config.
    """
    return hashlib.sha256(token.encode()).hexdigest()


def generate_service_token(name: str, scopes: list = None, expires_in_days: int = 365):
    """
    Generate a new service token and save it to the database.
    Returns (plain_token, service_token_doc)
    """
    plain_token = f"st_{secrets.token_urlsafe(32)}"
    token_hash = hash_token(plain_token)

    expires_at = None
    if expires_in_days:
        expires_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
            days=expires_in_days
        )

    doc = ServiceToken(
        name=name,
        token_hash=token_hash,
        scopes=scopes or ["openclaw:data-read"],
        expires_at=expires_at,
        status="active",
    )
    doc.save()

    return plain_token, doc


def verify_service_token(plain_token: str, required_scope: str = None):
    """
    Verify a plain text token against the database.
    Returns (ServiceToken, error_message)
    """
    if not plain_token:
        return None, "No token provided"

    token_hash = hash_token(plain_token)
    token_doc = ServiceToken.objects(token_hash=token_hash, status="active").first()

    if not token_doc:
        return None, "Invalid or inactive token"

    is_valid, error = token_doc.is_valid(required_scope)
    if not is_valid:
        return None, error

    return token_doc, None
