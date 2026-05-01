# -*- coding: utf-8 -*-

import os
import logging
import sys
from app.lib.utilities.logger_utils import CLSJSONFormatter, TextFormatter


# 配置 K8S + CLS 兼容的 JSON 日志格式
def setup_logging():
    """配置日志输出到 stdout (K8S 标准)，支持 LOG_FORMAT 环境变量"""
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    pymongo_level = getattr(
        logging, os.getenv("PYMONGO_LOG_LEVEL", "INFO").upper(), logging.INFO
    )

    # 支持 LOG_FORMAT 环境变量: json (默认/K8S) 或 text (本地调试)
    log_format = os.getenv("LOG_FORMAT", "json").lower()
    if log_format == "text":
        formatter = TextFormatter()
    else:
        formatter = CLSJSONFormatter(service_name="caifubao-datahub")

    # 配置 root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除已有的 handlers（避免重复）
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 添加 stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)  # 由 logger 级别控制实际输出
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)

    # 禁止向上传播（避免重复输出）
    root_logger.propagate = False

    # 压低 pymongo 心跳日志噪音，避免 CLS 被 DEBUG heartbeat 淹没
    for logger_name in ("pymongo", "pymongo.topology", "pymongo.connection"):
        logging.getLogger(logger_name).setLevel(pymongo_level)

    return root_logger


# 初始化日志配置
setup_logging()

logger = logging.getLogger(__name__)

# 延迟导入 app_config，避免循环导入
try:
    from app.conf import app_config

    logger.info(f"Datahub service starting in {app_config.__class__.__name__} mode")
except ImportError:
    logger.info("Datahub service starting (app_config not available)")
# Force rebuild Sat Mar 28 07:17:00 UTC 2026
