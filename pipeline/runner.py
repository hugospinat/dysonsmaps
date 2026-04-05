from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .config import PipelineConfig
from .logging_setup import build_run_logger
from .stages import CrawlMapsStage, DownloadImagesStage, ExtractImagesStage, FetchHtmlStage, GeneratePreviewsStage


class PipelineRunner:
    def __init__(self, config: PipelineConfig, verbose: bool = False, dry_run: bool = False) -> None:
        self.config = config
        self.config.ensure_dirs()
        self.verbose = verbose
        self.dry_run = dry_run
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]

        self.logger, self.log_file = build_run_logger(
            logs_dir=self.config.workspace_root / self.config.logs_dir,
            run_id=self.run_id,
            verbose=verbose,
        )

    def _live_status_path(self) -> Path:
        return self.config.workspace_root / self.config.logs_dir / "latest_status.json"

    def _write_live_status(
        self,
        *,
        mode: str,
        status: str,
        stage_results: list[dict],
        current_stage: str = "",
        started_at: str = "",
        ended_at: str = "",
    ) -> None:
        if self.dry_run:
            return

        payload = {
            "run_id": self.run_id,
            "mode": mode,
            "dry_run": self.dry_run,
            "status": status,
            "current_stage": current_stage,
            "started_at": started_at,
            "ended_at": ended_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "log_file": str(self.log_file),
            "stage_results": stage_results,
        }

        path = self._live_status_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        tmp.replace(path)

    def run(self, full: bool = False, stage_filter: list[str] | None = None) -> dict:
        mode = "full" if full else "incremental"
        started_at = datetime.now(timezone.utc).isoformat()
        stages = [
            CrawlMapsStage(
                config=self.config,
                logger=self.logger,
                run_id=self.run_id,
                dry_run=self.dry_run,
            ),
            FetchHtmlStage(
                config=self.config,
                logger=self.logger,
                run_id=self.run_id,
                dry_run=self.dry_run,
            ),
            ExtractImagesStage(
                config=self.config,
                logger=self.logger,
                run_id=self.run_id,
                dry_run=self.dry_run,
            ),
            DownloadImagesStage(
                config=self.config,
                logger=self.logger,
                run_id=self.run_id,
                dry_run=self.dry_run,
            ),
            GeneratePreviewsStage(
                config=self.config,
                logger=self.logger,
                run_id=self.run_id,
                dry_run=self.dry_run,
            ),
        ]

        if stage_filter:
            allowed = set(stage_filter)
            stages = [stage for stage in stages if stage.stage_name in allowed]

        stage_results = []
        self._write_live_status(
            mode=mode,
            status="running",
            stage_results=stage_results,
            current_stage="",
            started_at=started_at,
        )

        for stage in stages:
            self._write_live_status(
                mode=mode,
                status="running",
                stage_results=stage_results,
                current_stage=stage.stage_name,
                started_at=started_at,
            )

            result = stage.execute(force=full)
            stage_results.append(asdict(result))

            self._write_live_status(
                mode=mode,
                status="running" if result.status == "succeeded" else "failed",
                stage_results=stage_results,
                current_stage=stage.stage_name,
                started_at=started_at,
            )

            if result.status != "succeeded":
                break

        final_status = "failed" if any(item.get("status") != "succeeded" for item in stage_results) else "completed"
        ended_at = datetime.now(timezone.utc).isoformat()
        self._write_live_status(
            mode=mode,
            status=final_status,
            stage_results=stage_results,
            current_stage="",
            started_at=started_at,
            ended_at=ended_at,
        )

        summary = {
            "run_id": self.run_id,
            "mode": mode,
            "dry_run": self.dry_run,
            "log_file": str(self.log_file),
            "stage_results": stage_results,
        }

        if not self.dry_run:
            summary_path = self.config.workspace_root / self.config.logs_dir / f"{self.run_id}_summary.json"
            summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

            summary_txt = self.config.workspace_root / self.config.logs_dir / f"{self.run_id}_summary.txt"
            lines = [
                f"Run: {self.run_id}",
                f"Mode: {summary['mode']}",
                f"Dry-run: {self.dry_run}",
                f"Log: {self.log_file}",
                "",
                "Stage results:",
            ]
            for stage_result in summary["stage_results"]:
                metrics = stage_result.get("metrics", {})
                extra_bits = []
                for key in (
                    "total_rows",
                    "discovered_rows",
                    "carried_forward",
                    "new",
                    "updated",
                    "unchanged",
                    "html_rows",
                    "target_rows",
                    "processed_targets",
                    "downloaded",
                    "parsed_ok",
                    "failed",
                    "skipped_existing",
                    "rows_with_candidates",
                    "candidate_rows",
                    "queue_rows",
                    "assets_total",
                    "assets_ok",
                    "assets_failed",
                    "moved_existing",
                    "already_exists",
                    "image_rows",
                    "queued_jobs",
                    "processed_rows",
                    "failed_rows",
                    "missing_originals",
                    "bw_detected",
                    "bw_from_tags",
                    "previews_generated",
                    "previews_reused",
                ):
                    if key in metrics:
                        extra_bits.append(f"{key}={metrics.get(key)}")
                lines.append(
                    (
                        f"- {stage_result['stage']}: {stage_result['status']} "
                        f"({stage_result['duration_seconds']}s) "
                        + " ".join(extra_bits)
                    )
                )
            summary_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")

        return summary
