# -*- coding: utf-8 -*-
import datetime
from unittest.mock import patch, MagicMock

import flask
import pytest

from app.lib.auth_decorators import block_service_tokens
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


# ---------------------------------------------------------------------------
# block_service_tokens — 403 guard tests
# ---------------------------------------------------------------------------


class TestBlockServiceTokens:
    """Verify block_service_tokens returns 403 for service-token requests."""

    @pytest.fixture
    def app(self):
        """Minimal Flask app with a guarded test blueprint."""
        app = flask.Flask(__name__)
        test_bp = flask.Blueprint("test_guard", __name__)
        test_bp.before_request(block_service_tokens)

        @test_bp.route("/test-guard", methods=["GET"])
        def test_route():
            return flask.jsonify({"success": True, "data": "ok"})

        app.register_blueprint(test_bp)
        return app

    def test_blocks_bearer_st_prefix(self, app):
        """Bearer st_... returns 403 with correct shape."""
        client = app.test_client()
        resp = client.get(
            "/test-guard",
            headers={"Authorization": "Bearer st_abc123"},
        )
        assert resp.status_code == 403
        body = resp.get_json()
        assert body["success"] is False
        assert "Service tokens are not allowed" in body["message"]
        assert body["error_code"] == "SERVICE_TOKEN_BLOCKED"
        assert "request_id" in body

    def test_blocks_lowercase_bearer(self, app):
        """Case-insensitive: bearer st_... also returns 403."""
        client = app.test_client()
        resp = client.get(
            "/test-guard",
            headers={"Authorization": "bearer st_abc123"},
        )
        assert resp.status_code == 403
        body = resp.get_json()
        assert body["error_code"] == "SERVICE_TOKEN_BLOCKED"

    def test_allows_no_auth_header(self, app):
        """Requests without Authorization header pass through."""
        client = app.test_client()
        resp = client.get("/test-guard")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True

    def test_allows_jwt_tokens(self, app):
        """Bearer tokens not starting with st_ pass through."""
        client = app.test_client()
        resp = client.get(
            "/test-guard",
            headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9..."},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["success"] is True
