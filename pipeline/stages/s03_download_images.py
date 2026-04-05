from __future__ import annotations

import re
import shutil
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

from .stage_base import StageBase

IMAGE_EXTS = {"png", "jpg", "jpeg", "webp", "gif"}
ARCHIVE_EXTS = {"zip", "7z", "rar", "tar", "gz"}
EXT_PREFERENCE = {
    "png": 0,
    "jpg": 1,
    "jpeg": 1,
    "webp": 2,
    "gif": 3,
}
NOISE_PATTERNS = (
    "banner",
    "avatar",
    "gravatar",
    "blavatar",
    "cropped-new-dice-header",
)


class DownloadImagesStage(StageBase):
    stage_name = "s03_download_images"
    progress_log_every = 100

    def output_paths(self) -> list[str]:
        return [
            str(self.config.download_queue_csv),
            str(self.config.download_summary_csv),
            str(self.config.s03_state_file),
            str(self.config.downloads_output_root),
        ]

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DysonAssetDownloader/1.0)"})
        return session

    def _ext_from_url(self, url: str) -> str:
        path = urlparse(url).path.lower()
        if "." not in path:
            return ""
        return path.rsplit(".", 1)[-1]

    def _filename_from_url(self, url: str) -> str:
        return Path(urlparse(url).path).name.strip()

    def _stem_from_filename(self, filename: str) -> str:
        return Path(filename).stem.strip().lower()

    def _is_noise_url(self, url: str) -> bool:
        lower = str(url or "").lower()
        return any(pattern in lower for pattern in NOISE_PATTERNS)

    def _has_maps_tag(self, tags_value: str) -> bool:
        if not isinstance(tags_value, str) or not tags_value.strip():
            return False
        for chunk in re.split(r"\||,", tags_value):
            token = chunk.strip().strip("[]'\" ").lower()
            if token == "maps":
                return True
        return False

    def _candidate_url(self, row: pd.Series) -> str:
        for col in ("candidate_href", "candidate_orig", "candidate_src"):
            value = str(row.get(col, "") or "").strip()
            if value:
                return value
        return ""

    def _source_file_for_row(self, row: pd.Series) -> str:
        for col in ("source_file", "file", "html_file", "html_path", "canonical_url", "url"):
            value = str(row.get(col, "") or "").strip()
            if value:
                return value
        return "unknown_page"

    def _safe_folder_name(self, source_file: str) -> str:
        stem = Path(str(source_file or "")).stem.strip()
        if not stem:
            stem = "unknown_page"
        stem = re.sub(r'[<>:"/\\|?*]+', "_", stem)
        return stem[:180]

    def _build_queue(self, df: pd.DataFrame) -> pd.DataFrame:
        work_df = df.copy()
        if self.config.s03_require_maps_tag:
            if "tags" not in work_df.columns:
                work_df["tags"] = ""
            work_df = work_df[work_df["tags"].apply(self._has_maps_tag)].copy()

        rows: list[dict] = []
        for _, row in work_df.iterrows():
            url = self._candidate_url(row)
            if not url:
                continue

            ext = self._ext_from_url(url)
            if ext not in IMAGE_EXTS:
                continue
            if self._is_noise_url(url):
                continue

            file_name = self._filename_from_url(url)
            if not file_name:
                continue

            rows.append(
                {
                    "asset_type": "image",
                    "url": url,
                    "file_name": file_name,
                    "file_stem": self._stem_from_filename(file_name),
                    "file_ext": ext,
                    "source_file": self._source_file_for_row(row),
                    "canonical_url": str(row.get("canonical_url", "") or ""),
                    "title": str(row.get("title", "") or ""),
                    "tags": str(row.get("tags", "") or ""),
                    "candidate_rank": pd.to_numeric(row.get("candidate_rank", ""), errors="coerce"),
                    "candidate_score": pd.to_numeric(row.get("candidate_score", ""), errors="coerce"),
                    "is_best_candidate": pd.to_numeric(row.get("is_best_candidate", 0), errors="coerce"),
                }
            )

        image_df = pd.DataFrame(rows)
        if image_df.empty:
            return pd.DataFrame(
                columns=[
                    "asset_type",
                    "url",
                    "file_name",
                    "file_stem",
                    "file_ext",
                    "source_file",
                    "canonical_url",
                    "title",
                    "tags",
                    "candidate_rank",
                    "candidate_score",
                    "is_best_candidate",
                ]
            )

        image_df["candidate_score"] = image_df["candidate_score"].fillna(-10000)
        image_df["is_best_candidate"] = image_df["is_best_candidate"].fillna(0)
        image_df["candidate_rank"] = image_df["candidate_rank"].fillna(9999)
        image_df["ext_pref"] = image_df["file_ext"].map(EXT_PREFERENCE).fillna(99)

        image_df = image_df.sort_values(
            by=["source_file", "file_stem", "ext_pref", "candidate_score", "is_best_candidate", "candidate_rank"],
            ascending=[True, True, True, False, False, True],
        )
        image_df = image_df.drop_duplicates(subset=["source_file", "file_stem"], keep="first")
        image_df = image_df.drop(columns=["ext_pref"])

        return image_df.sort_values(
            by=["source_file", "file_stem", "file_name", "url"],
            ascending=[True, True, True, True],
        ).reset_index(
            drop=True
        )

    def _build_existing_file_index(self, downloads_root: Path) -> dict[str, list[Path]]:
        out: dict[str, list[Path]] = defaultdict(list)
        if not downloads_root.exists():
            return out

        for file_path in downloads_root.rglob("*"):
            if file_path.is_file():
                out[file_path.name.lower()].append(file_path)

        for key in list(out.keys()):
            out[key] = sorted(out[key], key=lambda path: (len(path.parts), str(path).lower()))

        return out

    def _pop_existing_source(self, index: dict[str, list[Path]], file_name: str, target_path: Path) -> Path | None:
        key = file_name.lower()
        candidates = index.get(key, [])
        keep: list[Path] = []

        found: Path | None = None
        for candidate in candidates:
            if not candidate.exists():
                continue
            if candidate.resolve() == target_path.resolve():
                keep.append(candidate)
                continue
            if found is None:
                found = candidate
            else:
                keep.append(candidate)

        index[key] = keep
        return found

    def _write_progress_state(self, metrics: dict[str, int], processed_assets: int) -> None:
        if self.dry_run:
            return
        payload = {
            "stage": self.stage_name,
            "run_id": self.run_id,
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "processed_assets": int(processed_assets),
            "metrics": metrics,
            "output_files": [
                str(self.config.download_queue_csv),
                str(self.config.download_summary_csv),
                str(self.config.downloads_output_root),
            ],
        }
        self._write_json(self.config.s03_state_file, payload)

    def run_stage(self, force: bool = False) -> dict[str, int]:
        if not self.config.image_inventory_csv.exists():
            raise RuntimeError(f"Missing input: {self.config.image_inventory_csv}")

        inventory_df = pd.read_csv(self.config.image_inventory_csv, dtype=str).fillna("")
        if inventory_df.empty:
            raise RuntimeError("image_inventory.csv is empty")

        queue_df = self._build_queue(inventory_df)
        if not self.dry_run:
            queue_df.to_csv(self.config.download_queue_csv, index=False, encoding="utf-8-sig")

        queue_df = queue_df[queue_df["asset_type"].str.lower() == "image"].copy() if not queue_df.empty else queue_df

        max_assets = int(getattr(self.config, "s03_max_assets", 0) or 0)
        if max_assets > 0 and len(queue_df) > max_assets:
            queue_df = queue_df.head(max_assets).copy()
            if not self.dry_run:
                queue_df.to_csv(self.config.download_queue_csv, index=False, encoding="utf-8-sig")

        metrics = {
            "inventory_rows": int(len(inventory_df)),
            "queue_rows": int(len(queue_df)),
            "pages": 0,
            "assets_total": 0,
            "assets_ok": 0,
            "assets_failed": 0,
            "downloaded": 0,
            "moved_existing": 0,
            "already_exists": 0,
            "dry_run_planned": 0,
            "pages_success": 0,
            "pages_partial": 0,
            "pages_failed": 0,
            "pages_no_images": 0,
        }

        summary_rows: list[dict[str, str | int]] = []

        if queue_df.empty:
            if not self.dry_run:
                summary_df = pd.DataFrame(
                    summary_rows,
                    columns=["source_https", "dossier", "nombre_image", "tags", "status"],
                )
                summary_df.to_csv(self.config.download_summary_csv, index=False, encoding="utf-8-sig")
                payload = {
                    "stage": self.stage_name,
                    "run_id": self.run_id,
                    "ran_at": datetime.now(timezone.utc).isoformat(),
                    "status": "completed",
                    "metrics": metrics,
                    "output_files": [
                        str(self.config.download_queue_csv),
                        str(self.config.download_summary_csv),
                        str(self.config.downloads_output_root),
                    ],
                }
                self._write_json(self.config.s03_state_file, payload)
            return metrics

        legacy_root = self.config.downloads_legacy_root
        output_root = self.config.downloads_output_root
        output_root.mkdir(parents=True, exist_ok=True)

        existing_index = self._build_existing_file_index(legacy_root)
        if output_root.resolve() != legacy_root.resolve():
            output_index = self._build_existing_file_index(output_root)
            for file_name, paths in output_index.items():
                existing_index[file_name].extend(paths)
        session = self._build_session()

        grouped = queue_df.groupby("source_file", dropna=False)
        processed_assets = 0
        started = time.perf_counter()

        for source_file, group in grouped:
            metrics["pages"] += 1

            folder_name = self._safe_folder_name(str(source_file))
            folder_path = output_root / folder_name
            if not self.dry_run:
                folder_path.mkdir(parents=True, exist_ok=True)

            source_https = str(group["canonical_url"].iloc[0] or "").strip()
            tags = str(group["tags"].iloc[0] or "").strip()

            total = 0
            ok = 0
            errors = 0

            for _, row in group.iterrows():
                url = str(row.get("url", "") or "").strip()
                file_name = str(row.get("file_name", "") or "").strip()
                if not url or not file_name:
                    continue

                total += 1
                metrics["assets_total"] += 1
                processed_assets += 1
                target_path = folder_path / file_name

                status = ""
                if target_path.exists():
                    status = "exists"
                    ok += 1
                    metrics["assets_ok"] += 1
                    metrics["already_exists"] += 1
                else:
                    source_path = self._pop_existing_source(existing_index, file_name=file_name, target_path=target_path)
                    if source_path is not None:
                        if self.dry_run:
                            status = "dry-run-move"
                            ok += 1
                            metrics["assets_ok"] += 1
                            metrics["dry_run_planned"] += 1
                        else:
                            try:
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                shutil.move(str(source_path), str(target_path))
                                status = "moved_existing"
                                ok += 1
                                metrics["assets_ok"] += 1
                                metrics["moved_existing"] += 1
                                existing_index[target_path.name.lower()].append(target_path)
                            except Exception as exc:
                                status = f"move_error: {exc}"
                                errors += 1
                                metrics["assets_failed"] += 1
                    else:
                        if self.dry_run:
                            status = "dry-run-download"
                            ok += 1
                            metrics["assets_ok"] += 1
                            metrics["dry_run_planned"] += 1
                        else:
                            try:
                                response = session.get(url, timeout=self.config.s03_timeout_seconds)
                                response.raise_for_status()
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                target_path.write_bytes(response.content)
                                status = "downloaded"
                                ok += 1
                                metrics["assets_ok"] += 1
                                metrics["downloaded"] += 1
                                existing_index[target_path.name.lower()].append(target_path)
                                if self.config.s03_delay_seconds > 0:
                                    time.sleep(self.config.s03_delay_seconds)
                            except Exception as exc:
                                status = f"download_error: {exc}"
                                errors += 1
                                metrics["assets_failed"] += 1

                if processed_assets % self.progress_log_every == 0:
                    elapsed = time.perf_counter() - started
                    self.logger.info(
                        (
                            f"Image download progress: assets={processed_assets}/{len(queue_df)} "
                            f"ok={metrics['assets_ok']} failed={metrics['assets_failed']} "
                            f"downloaded={metrics['downloaded']} moved={metrics['moved_existing']} "
                            f"exists={metrics['already_exists']} elapsed={elapsed:.1f}s"
                        ),
                        extra={"stage": self.stage_name, "run_id": self.run_id},
                    )
                    self._write_progress_state(metrics=metrics, processed_assets=processed_assets)

                if status.startswith("move_error") or status.startswith("download_error"):
                    self.logger.warning(
                        f"Asset error for {file_name}: {status}",
                        extra={"stage": self.stage_name, "run_id": self.run_id},
                    )

            if total == 0:
                page_status = "no-images"
                metrics["pages_no_images"] += 1
            elif errors == 0:
                page_status = "success"
                metrics["pages_success"] += 1
            elif ok > 0:
                page_status = "partial"
                metrics["pages_partial"] += 1
            else:
                page_status = "failed"
                metrics["pages_failed"] += 1

            summary_rows.append(
                {
                    "source_https": source_https,
                    "dossier": str(folder_path.resolve()),
                    "nombre_image": int(ok),
                    "tags": tags,
                    "status": page_status,
                }
            )

        if not self.dry_run:
            summary_df = pd.DataFrame(summary_rows)
            summary_df.to_csv(self.config.download_summary_csv, index=False, encoding="utf-8-sig")

            payload = {
                "stage": self.stage_name,
                "run_id": self.run_id,
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "status": "completed",
                "metrics": metrics,
                "output_files": [
                    str(self.config.download_queue_csv),
                    str(self.config.download_summary_csv),
                    str(self.config.downloads_output_root),
                ],
            }
            self._write_json(self.config.s03_state_file, payload)

        self.logger.info(
            (
                f"s03_download_images stats: queue={metrics['queue_rows']} assets_total={metrics['assets_total']} "
                f"ok={metrics['assets_ok']} failed={metrics['assets_failed']} downloaded={metrics['downloaded']} "
                f"moved={metrics['moved_existing']} exists={metrics['already_exists']}"
            ),
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return metrics
