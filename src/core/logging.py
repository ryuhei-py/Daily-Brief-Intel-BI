import logging
import os
import re
from typing import Any, Mapping

SENSITIVE_PATTERN = re.compile(r"(TOKEN|KEY|SECRET|PASSWORD)", re.IGNORECASE)


def _mask_value(value: str) -> str:
    if value is None:
        return "***"
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def mask_secrets(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy with sensitive env values masked."""
    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, str) and SENSITIVE_PATTERN.search(key):
            sanitized[key] = _mask_value(value)
        else:
            sanitized[key] = value
    return sanitized


def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.getenv("APP_LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger
