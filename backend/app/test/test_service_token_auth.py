# -*- coding: utf-8 -*-
import datetime
from unittest.mock import patch, MagicMock
from app.utilities.auth_util import hash_token, verify_service_token


class TestServiceTokenModel:
    """Test ServiceToken model methods (Internal logic only, no DB)"""

    def test_is_valid_active(self):
        # Avoid spec=ServiceToken to prevent connection attempts
        token = MagicMock()
        token.status = "active"
        token.expires_at = None
        token.scopes = ["openclaw:data-read"]

        # Use the actual implementation logic
        from app.model.service_token import ServiceToken

        valid, error = ServiceToken.is_valid(token)
        assert valid
        assert error is None

    def test_is_valid_revoked(self):
        token = MagicMock()
        token.status = "revoked"

        from app.model.service_token import ServiceToken

        valid, error = ServiceToken.is_valid(token)
        assert not valid
        assert "revoked" in error

    def test_is_valid_expired(self):
        token = MagicMock()
        token.status = "active"
        token.expires_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            days=1
        )

        from app.model.service_token import ServiceToken

        valid, error = ServiceToken.is_valid(token)
        assert not valid
        assert "expired" in error


class TestAuthUtil:
    """Test auth utility functions"""

    def test_hash_token(self):
        t1 = "test_token"
        h1 = hash_token(t1)
        h2 = hash_token(t1)
        assert h1 == h2
        assert h1 != t1

    @patch("app.utilities.auth_util.ServiceToken")
    def test_verify_service_token_success(self, mock_service_token_class):
        mock_token = MagicMock()
        mock_token.status = "active"
        mock_token.is_valid.return_value = (True, None)

        # Mock the chain: ServiceToken.objects(hash=..., status=...).first()
        mock_service_token_class.objects.return_value.first.return_value = mock_token

        token_doc, error = verify_service_token("some_token")
        assert token_doc == mock_token
        assert error is None

    @patch("app.utilities.auth_util.ServiceToken")
    def test_verify_service_token_not_found(self, mock_service_token_class):
        # Mock the chain to return None
        mock_service_token_class.objects.return_value.first.return_value = None

        token_doc, error = verify_service_token("wrong_token")
        assert token_doc is None
        assert "Invalid" in error
