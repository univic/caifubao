# backend/app/test/conftest.py
import os
import sys

# Fix for Python 3.12+: Ensure pkg_resources is available before any imports
# This must be done before importing pytest or any other modules
try:
    import pkg_resources  # noqa: F401
except ImportError:
    import importlib.metadata
    import types

    # Create a fake pkg_resources module
    fake_pkg_resources = types.ModuleType("pkg_resources")

    def parse_version(version_string):
        """Simple version parser that returns the string as-is for comparison"""
        return version_string

    fake_pkg_resources.parse_version = parse_version
    fake_pkg_resources.get_distribution = lambda name: type(
        "Distribution", (), {"version": importlib.metadata.version(name)}
    )()
    fake_pkg_resources.resource_exists = lambda package, resource: False
    fake_pkg_resources.resource_filename = lambda package, resource: ""
    fake_pkg_resources.resource_string = lambda package, resource: b""
    fake_pkg_resources.iter_entry_points = lambda group=None: []

    sys.modules["pkg_resources"] = fake_pkg_resources

import pytest
from unittest.mock import patch

# Set test environment
os.environ["APP_ENV"] = "test"

# Register a mock mongoengine connection before any Document class is imported.
# Without this, importing mongoengine Document subclasses (e.g. BacktestResult)
# raises ConnectionFailure("You have not defined a default connection").
# connect() with only db + alias is lazy (does not actually open a socket).
from mongoengine import connect as _mongo_connect  # noqa: E402

_mongo_connect(db="testdb", alias="default")
# Also register aliases used by datahub jobs
for _alias in ("datahub_db", "data_sync_db"):
    _mongo_connect(db="testdb", alias=_alias)


@pytest.fixture(autouse=True)
def _ensure_default_connection():
    """Guarantee a registered 'default' mongoengine connection per test.

    Some API tests reach mongoengine's get_connection("default") indirectly
    (e.g. replacing Document.objects with a fake touches _get_collection).
    If an earlier test or module import disconnected the alias, the query
    fails with "You have not defined a default connection" instead of a
    connection error. Re-register lazily when missing.
    """
    from mongoengine import get_connection

    try:
        get_connection("default")
    except Exception:
        _mongo_connect(db="testdb", alias="default")
    yield


@pytest.fixture
def mock_mongodb():
    """Mock MongoDB connection"""
    with patch("app.lib.db_watcher.mongoengine_tool.db") as mock:
        yield mock


@pytest.fixture
def app():
    """Create Flask app for testing"""
    from app.lib.web_server import create_web_app

    app = create_web_app()
    app.config["TESTING"] = True
    app.config["JWT_SECRET_KEY"] = "test-secret-key"
    return app


@pytest.fixture
def client(app):
    """Flask test client"""
    return app.test_client()


@pytest.fixture
def auth_headers():
    """Generate auth headers for testing"""
    # This would need proper JWT token generation
    return {"Authorization": "Bearer test-token"}
