import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

from app.model.event_log import EventLog


_STANDARD_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class CLSJSONFormatter(logging.Formatter):
    def __init__(self, service_name="caifubao-backend"):
        super().__init__()
        self.service_name = service_name
        self.pod_name = os.getenv("POD_NAME", "local")
        self.namespace = os.getenv("POD_NAMESPACE", "default")

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.upper(),
            "message": record.getMessage(),
            "service": self.service_name,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "namespace": self.namespace,
            "pod_name": self.pod_name,
            "env": os.getenv("APP_ENV", "unknown"),
        }

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            log_obj["exc_type"] = exc_type.__name__ if exc_type else None
            log_obj["exc_message"] = str(exc_value) if exc_value else None
            log_obj["traceback"] = "".join(
                traceback.format_exception(exc_type, exc_value, exc_tb)
            ).strip()

        extra_fields = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_FIELDS and not key.startswith("_")
        }
        if extra_fields:
            log_obj.update(extra_fields)

        return json.dumps(log_obj, ensure_ascii=False, default=str)


class TextFormatter(logging.Formatter):
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class HealthEndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "/health" not in message


def is_health_log_enabled() -> bool:
    value = os.getenv("HEALTH_CHECK_LOG_ENABLED", "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


def create_logger():
    logger = logging.getLogger()
    if logger.handlers:
        return logger

    log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    log_format = os.getenv("LOG_FORMAT", "json").lower()

    logger.setLevel(log_level)

    formatter = TextFormatter() if log_format == "text" else CLSJSONFormatter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    logger.propagate = False

    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.setLevel(logging.INFO)
    if not is_health_log_enabled():
        werkzeug_logger.addFilter(HealthEndpointFilter())

    return logger


class EventLogger:
    def __init__(self):
        self.logger = None

    def get_logger(self, module_name):
        if not self.logger:
            self.logger = logging.getLogger(module_name)

    def record_event(
        self, module_name, code, name, meta_type, meta_name, log_level, message
    ):
        log_level_list = ["debug", "info", "warning", "error", "critical"]
        if log_level not in log_level_list:
            self.logger.warning(
                "Invalid log level: %s, resetting log level to warning", log_level
            )
            log_level = "warning"
        self.get_logger(module_name)

        event_log = EventLog()
        event_log.code = code
        event_log.name = name
        event_log.module = module_name
        event_log.meta_type = meta_type
        event_log.meta_name = meta_name
        event_log.log_level = log_level
        event_log.message = message
        event_log.save()

        logger_message = f"{code}-{name}-{meta_type}-{meta_name}-{message}"
        logger_func = getattr(self.logger, log_level)
        logger_func(
            logger_message,
            extra={
                "event_code": code,
                "event_name": name,
                "meta_type": meta_type,
                "meta_name": meta_name,
            },
        )


event_logger = EventLogger()
