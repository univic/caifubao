# -*- coding: utf-8 -*-
"""
DataHub 日志工具 - K8S + CLS 兼容版

改动说明:
- 移除 RotatingFileHandler (K8S 不友好)
- JSONFormatter 输出到 stdout
- 支持 LOG_LEVEL 环境变量
- CLS 兼容字段 (service, namespace, pod_name)
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timezone

# 保持与旧代码的兼容性 - 从 app.conf 导入配置
try:
    from app.conf import app_config
except ImportError:
    app_config = None


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
    """
    腾讯云 CLS 兼容的 JSON 格式化器

    输出字段:
    - timestamp: ISO 格式时间 (UTC)
    - level: 日志级别
    - service: 服务名称 (CLS 必填)
    - logger: logger 名称
    - module: 模块名
    - function: 函数名
    - line: 行号
    - message: 日志消息
    - namespace: K8S 命名空间
    - pod_name: Pod 名称
    """

    def __init__(self, service_name="caifubao-datahub"):
        super().__init__()
        self.service_name = service_name
        self.pod_name = os.getenv("POD_NAME", "local")
        self.namespace = os.getenv("POD_NAMESPACE", "default")

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            # CLS 必填字段
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.upper(),
            "message": record.getMessage(),
            # CLS 建议字段 (便于检索)
            "service": self.service_name,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            # K8S 上下文
            "namespace": self.namespace,
            "pod_name": self.pod_name,
            "env": os.getenv("APP_ENV", "unknown"),
        }

        # 结构化异常信息 (避免多行 traceback)
        if record.exc_info:
            exc_info = record.exc_info
            try:
                log_obj["exc_type"] = exc_info[0].__name__ if exc_info[0] else None
            except AttributeError:
                log_obj["exc_type"] = type(exc_info[0]).name if exc_info[0] else None
            log_obj["exc_message"] = str(exc_info[1]) if exc_info[1] else None
            log_obj["traceback"] = "".join(
                traceback.format_exception(*exc_info)
            ).strip()

        extra_fields = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_FIELDS and not key.startswith("_")
        }
        if extra_fields:
            log_obj.update(extra_fields)

        return json.dumps(log_obj, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """
    传统文本格式 (用于本地调试或 LOG_FORMAT=text)
    """

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def create_logger(name: str = None) -> logging.Logger:
    """
    创建 K8S + CLS 兼容的 logger

    Args:
        name: logger 名称，默认使用 root logger

    Returns:
        配置完成的 logger 实例
    """
    logger = logging.getLogger(name)

    # 避免重复添加 handler
    if logger.handlers:
        return logger

    # 从环境变量读取日志级别
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

    # 决定输出格式: JSON (K8S/CLS) 或 Text (本地调试)
    log_format = os.getenv("LOG_FORMAT", "json").lower()

    if log_format == "text":
        # 传统文本格式 - 用于本地调试
        formatter = TextFormatter()
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
    else:
        # JSON 格式 - K8S + CLS 兼容
        formatter = CLSJSONFormatter()
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)  # 由 logger 级别控制实际输出
        console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    # 禁止向上传播 (避免重复输出)
    logger.propagate = False

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    获取 logger 的便捷方法 (兼容原有 EventLogger)
    """
    return logging.getLogger(name)


def is_health_log_enabled() -> bool:
    """Whether to emit INFO logs for incoming health check requests."""
    value = os.getenv("HEALTH_CHECK_LOG_ENABLED", "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


# ==================== EventLogger 改造 ====================


class EventLogger:
    """
    事件日志记录器

    改造说明:
    - 直接使用标准 logging (不再单独创建 logger)
    - record_event() 同时输出结构化日志
    """

    def __init__(self):
        self._logger = None

    @property
    def logger(self):
        if self._logger is None:
            # 使用 create_logger 确保 JSON formatter 生效
            self._logger = create_logger("app.event")
        return self._logger

    def record_event(
        self,
        module_name: str,
        code: str,
        name: str,
        meta_type: str,
        meta_name: str,
        log_level: str,
        message: str,
    ):
        """
        记录结构化事件

        Args:
            module_name: 模块名称
            code: 事件代码
            name: 事件名称
            meta_type: 元数据类型
            meta_name: 元数据名称
            log_level: 日志级别 (debug/info/warning/error/critical)
            message: 日志消息
        """
        # 验证日志级别
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        log_level_upper = log_level.upper() if log_level else "INFO"
        if log_level_upper not in valid_levels:
            self.logger.warning(
                f"Invalid log level: {log_level}, resetting to WARNING",
                extra={"original_level": log_level},
            )
            log_level_upper = "WARNING"

        # 构建日志消息
        log_message = f"[{code}] {name} | {meta_type}:{meta_name} | {message}"

        # 获取对应的 logging 方法
        log_func = getattr(self.logger, log_level_upper.lower(), self.logger.info)

        # 输出日志 (包含结构化字段)
        log_func(
            log_message,
            extra={
                "event_code": code,
                "event_name": name,
                "meta_type": meta_type,
                "meta_name": meta_name,
            },
        )


# 全局单例
event_logger = EventLogger()


# ==================== 保留旧函数以兼容 ====================


def check_log_dir(log_file_path):
    """
    保留旧函数以兼容旧代码调用
    注意: K8S 环境下不再需要创建本地日志目录
    """
    log_dir_path = os.path.dirname(log_file_path)
    if not os.path.exists(log_dir_path):
        print(f"LOG FILE DIR {log_dir_path} DOES NOT EXIST, CREATING...")
        os.makedirs(log_dir_path, exist_ok=True)
        print(f"LOG FILE DIR {log_dir_path} CREATED")
