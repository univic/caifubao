import sys
import types
from unittest.mock import Mock

import pytest


def _install_apscheduler_stubs(monkeypatch):
    apscheduler = types.ModuleType("apscheduler")
    schedulers = types.ModuleType("apscheduler.schedulers")
    background = types.ModuleType("apscheduler.schedulers.background")
    triggers = types.ModuleType("apscheduler.triggers")
    cron = types.ModuleType("apscheduler.triggers.cron")
    jobstores = types.ModuleType("apscheduler.jobstores")
    base = types.ModuleType("apscheduler.jobstores.base")
    mongodb = types.ModuleType("apscheduler.jobstores.mongodb")
    executors = types.ModuleType("apscheduler.executors")
    pool = types.ModuleType("apscheduler.executors.pool")

    background.BackgroundScheduler = object
    cron.CronTrigger = object
    base.JobLookupError = Exception
    mongodb.MongoDBJobStore = object
    pool.ThreadPoolExecutor = object

    monkeypatch.setitem(sys.modules, "apscheduler", apscheduler)
    monkeypatch.setitem(sys.modules, "apscheduler.schedulers", schedulers)
    monkeypatch.setitem(sys.modules, "apscheduler.schedulers.background", background)
    monkeypatch.setitem(sys.modules, "apscheduler.triggers", triggers)
    monkeypatch.setitem(sys.modules, "apscheduler.triggers.cron", cron)
    monkeypatch.setitem(sys.modules, "apscheduler.jobstores", jobstores)
    monkeypatch.setitem(sys.modules, "apscheduler.jobstores.base", base)
    monkeypatch.setitem(sys.modules, "apscheduler.jobstores.mongodb", mongodb)
    monkeypatch.setitem(sys.modules, "apscheduler.executors", executors)
    monkeypatch.setitem(sys.modules, "apscheduler.executors.pool", pool)


def _install_app_conf_stub(monkeypatch):
    conf = types.ModuleType("app.conf")
    conf.app_config = types.SimpleNamespace(
        MONGODB_HOST="localhost",
        MONGODB_PORT=27017,
        MONGODB_DB="test",
        MONGODB_USERNAME="root",
        MONGODB_PASSWORD="pass",
    )
    monkeypatch.setitem(sys.modules, "app.conf", conf)


def _install_datahub_dependency_stubs(monkeypatch):
    processors = types.ModuleType("app.lib.datahub.processors")
    processors.registry = {}

    task_controller_module = types.ModuleType("app.lib.task_controller")
    task_controller_module.task_controller = types.SimpleNamespace(create_task=Mock())

    mongoengine_tool = types.ModuleType("app.lib.db_watcher.mongoengine_tool")
    mongoengine_tool.mongo_watcher = types.SimpleNamespace(get_db_connection=Mock())

    trading_helper = types.ModuleType("app.lib.utilities.trading_day_helper")
    trading_helper.is_trading_day = lambda *_args, **_kwargs: True
    trading_helper.get_a_stock_market_trade_calendar = lambda: []

    monkeypatch.setitem(sys.modules, "app.lib.datahub.processors", processors)
    monkeypatch.setitem(sys.modules, "app.lib.task_controller", task_controller_module)
    monkeypatch.setitem(
        sys.modules, "app.lib.db_watcher.mongoengine_tool", mongoengine_tool
    )
    monkeypatch.setitem(
        sys.modules, "app.lib.utilities.trading_day_helper", trading_helper
    )


@pytest.fixture(autouse=True)
def isolated_scheduler_dependencies(monkeypatch):
    """Keep scheduler dependency stubs scoped to this test module."""
    _install_apscheduler_stubs(monkeypatch)
    _install_app_conf_stub(monkeypatch)
    _install_datahub_dependency_stubs(monkeypatch)
    monkeypatch.delitem(sys.modules, "app.lib.datahub", raising=False)


def test_scheduled_job_reraises_failures(monkeypatch, caplog):
    from app.lib.datahub import scheduled_job

    failing_connection = Mock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "app.lib.datahub.mongo_watcher.get_db_connection", failing_connection
    )

    with pytest.raises(RuntimeError, match="boom"):
        scheduled_job()

    assert "Error in scheduled job datahub_index_daily_job: boom" in caplog.text


def test_start_scheduled_removes_legacy_job(monkeypatch):
    from app.lib.datahub import Datahub

    scheduler = Mock()
    scheduler.get_job.return_value = object()

    monkeypatch.setattr("app.lib.datahub.MongoDBJobStore", Mock())
    monkeypatch.setattr("app.lib.datahub.ThreadPoolExecutor", Mock())
    monkeypatch.setattr("app.lib.datahub.CronTrigger", Mock())
    monkeypatch.setattr(
        "app.lib.datahub.app_config",
        types.SimpleNamespace(
            MONGODB_HOST="localhost",
            MONGODB_PORT=27017,
            MONGODB_DB="test",
            MONGODB_USERNAME="root",
            MONGODB_PASSWORD="pass",
        ),
    )
    monkeypatch.setattr(
        "app.lib.datahub.BackgroundScheduler", Mock(return_value=scheduler)
    )
    monkeypatch.setenv("DATAHUB_ENABLE_LEGACY_SCHEDULER", "true")

    Datahub().start_scheduled()

    scheduler.start.assert_called_once_with(paused=True)
    scheduler.remove_job.assert_called_once_with("datahub_daily_job")
    scheduler.resume.assert_called_once_with()


def test_start_scheduled_can_be_disabled(monkeypatch):
    from app.lib.datahub import Datahub

    monkeypatch.setenv("DATAHUB_ENABLE_LEGACY_SCHEDULER", "false")

    result = Datahub().start_scheduled()

    assert result["status"] == "DISABLED"
