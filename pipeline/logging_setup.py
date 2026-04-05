from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key in ("stage", "run_id"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=True)


def build_run_logger(logs_dir: Path, run_id: str, verbose: bool = False) -> tuple[logging.Logger, Path]:
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"{run_id}.log"

    logger = logging.getLogger(f"dyson.pipeline.{run_id}")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    json_handler = logging.FileHandler(log_file, encoding="utf-8")
    json_handler.setLevel(logging.DEBUG)
    json_handler.setFormatter(JsonFormatter())

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.addHandler(json_handler)
    logger.addHandler(console)
    return logger, log_file
