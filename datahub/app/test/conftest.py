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
from unittest.mock import MagicMock, patch

# Set test environment
os.environ["APP_ENV"] = "test"


@pytest.fixture
def mock_mongo_connection():
    """Mock MongoDB connection for all tests."""
    with patch("app.lib.db_watcher.mongoengine_tool.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        yield mock_connect


@pytest.fixture
def mock_mongo_disconnect():
    """Mock MongoDB disconnect."""
    with patch("app.lib.db_watcher.mongoengine_tool.disconnect") as mock:
        yield mock


@pytest.fixture
def sample_trade_calendar():
    """Provide a sample trading calendar for tests."""
    import datetime

    return [
        datetime.datetime(2024, 1, 2),
        datetime.datetime(2024, 1, 3),
        datetime.datetime(2024, 1, 4),
        datetime.datetime(2024, 1, 5),
        datetime.datetime(2024, 1, 8),
        datetime.datetime(2024, 1, 9),
        datetime.datetime(2024, 1, 10),
    ]


@pytest.fixture
def sample_stock_quote_df():
    """Provide a sample stock quote DataFrame for tests."""
    import pandas as pd

    return pd.DataFrame(
        {
            "date": ["2024-01-08", "2024-01-09", "2024-01-10"],
            "open": [100.0, 101.0, 102.0],
            "high": [105.0, 106.0, 107.0],
            "low": [99.0, 100.0, 101.0],
            "close": [103.0, 104.0, 105.0],
            "volume": [1000000, 1100000, 1200000],
            "code": ["sh600000", "sh600000", "sh600000"],
        }
    )


@pytest.fixture
def mock_akshare_module():
    """Mock akshare module for data source tests."""
    with patch(
        "app.lib.datahub.data_source.interface.akshare_interface.akshare"
    ) as mock:
        yield mock


@pytest.fixture
def mock_baostock_module():
    """Mock baostock module for data source tests."""
    with patch("app.lib.datahub.data_source.interface.baostock_interface.bs") as mock:
        yield mock


@pytest.fixture
def mock_freshness_meta_helper():
    """Mock freshness_meta_helper for tests."""
    with patch("app.utilities.freshness_meta_helper") as mock:
        mock.read_freshness_meta.return_value = None
        yield mock


@pytest.fixture
def mock_stock_code_helper():
    """Mock stock_code_helper for tests."""
    with patch("app.utilities.stock_code_helper") as mock:
        mock.add_market_prefix.side_effect = lambda x: (
            f"sh{x}" if x.startswith("6") else f"sz{x}"
        )
        yield mock
