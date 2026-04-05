from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class StageResult:
    stage: str
    status: str
    started_at: str
    ended_at: str
    duration_seconds: float
    metrics: dict[str, Any] = field(default_factory=dict)
    outputs: list[str] = field(default_factory=list)
    error: str | None = None


class StageBase:
    stage_name = "base"

    def __init__(self, config, logger, run_id: str, dry_run: bool = False) -> None:
        self.config = config
        self.logger = logger
        self.run_id = run_id
        self.dry_run = dry_run

    def run_stage(self, force: bool = False) -> dict[str, Any]:
        raise NotImplementedError

    def output_paths(self) -> list[str]:
        return []

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        if self.dry_run:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        tmp.replace(path)

    def execute(self, force: bool = False) -> StageResult:
        started_ts = datetime.now(timezone.utc).isoformat()
        started = time.perf_counter()

        self.logger.info(
            f"Starting stage {self.stage_name}",
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        try:
            metrics = self.run_stage(force=force)
            ended_ts = datetime.now(timezone.utc).isoformat()
            duration = time.perf_counter() - started

            result = StageResult(
                stage=self.stage_name,
                status="succeeded",
                started_at=started_ts,
                ended_at=ended_ts,
                duration_seconds=round(duration, 3),
                metrics=metrics,
                outputs=self.output_paths(),
            )
            self.logger.info(
                f"Completed stage {self.stage_name} in {result.duration_seconds:.3f}s",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )
            return result
        except Exception as exc:
            ended_ts = datetime.now(timezone.utc).isoformat()
            duration = time.perf_counter() - started
            self.logger.error(
                f"Stage {self.stage_name} failed: {exc}",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )
            return StageResult(
                stage=self.stage_name,
                status="failed",
                started_at=started_ts,
                ended_at=ended_ts,
                duration_seconds=round(duration, 3),
                metrics={},
                outputs=[],
                error=str(exc),
            )
