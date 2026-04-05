from __future__ import annotations

import hashlib
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .stage_base import StageBase

CANONICAL_RE = re.compile(r'<link\s+rel="canonical"\s+href="([^"]+)"', re.IGNORECASE)


def _safe_int(value: str) -> int | None:
    try:
        return int(float(value))
    except Exception:
        return None


def _slugify(value: str, max_length: int = 120) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    text = text.strip("-")
    return text[:max_length] or "page"


class FetchHtmlStage(StageBase):
    stage_name = "s01_fetch_html"
    progress_log_every = 20
    slow_request_seconds = 8.0

    def _checkpoint_manifest_path(self) -> Path:
        return self.config.blog_index_html_csv.with_name("blog_index_html_checkpoint.csv")

    def output_paths(self) -> list[str]:
        return [
            str(self.config.blog_index_html_csv),
            str(self.config.s01_state_file),
            str(self.config.html_cache_root),
        ]

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": self.config.user_agent})

        retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _normalize_url(self, url: str) -> str:
        value = str(url or "").strip()
        return value.rstrip("/")

    def _extract_canonical(self, html: str) -> str:
        match = CANONICAL_RE.search(str(html or ""))
        if not match:
            return ""
        return self._normalize_url(match.group(1))

    def _sha256_text(self, html: str) -> str:
        return hashlib.sha256(str(html or "").encode("utf-8", errors="ignore")).hexdigest()

    def _load_existing_manifest_meta(self) -> dict[str, dict[str, str]]:
        path = self.config.blog_index_html_csv
        out: dict[str, dict[str, str]] = {}

        def _merge_from(path_obj: Path) -> None:
            if not path_obj.exists():
                return

            manifest = pd.read_csv(path_obj, dtype=str).fillna("")
            if manifest.empty or "url" not in manifest.columns:
                return

            for col in (
                "etag",
                "last_modified",
                "html_sha256",
                "canonical_url",
                "canonical_match",
                "http_status",
                "validation_result",
            ):
                if col not in manifest.columns:
                    manifest[col] = ""

            for _, row in manifest.iterrows():
                key = self._normalize_url(str(row.get("url", "")))
                if not key:
                    continue
                out[key] = {
                    "etag": str(row.get("etag", "")),
                    "last_modified": str(row.get("last_modified", "")),
                    "html_sha256": str(row.get("html_sha256", "")),
                    "canonical_url": str(row.get("canonical_url", "")),
                    "canonical_match": str(row.get("canonical_match", "")),
                    "http_status": str(row.get("http_status", "")),
                    "validation_result": str(row.get("validation_result", "")),
                }

        _merge_from(path)
        # Prefer checkpoint metadata if available from an interrupted run.
        _merge_from(self._checkpoint_manifest_path())
        return out

    def _write_progress_checkpoint(self, manifest_rows: list[dict[str, str]], metrics: dict[str, int]) -> None:
        if self.dry_run or not manifest_rows:
            return

        checkpoint_path = self._checkpoint_manifest_path()
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(manifest_rows).to_csv(checkpoint_path, index=False, encoding="utf-8-sig")

        state_payload = {
            "stage": self.stage_name,
            "run_id": self.run_id,
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "metrics": metrics,
            "processed_rows": int(len(manifest_rows)),
            "output_files": [str(checkpoint_path)],
        }
        self._write_json(self.config.s01_state_file, state_payload)

    def _conditional_headers(self, meta: dict[str, str]) -> dict[str, str]:
        headers: dict[str, str] = {}
        etag = str(meta.get("etag", "")).strip()
        last_modified = str(meta.get("last_modified", "")).strip()
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        return headers

    def _fetch(self, session: requests.Session, url: str, headers: dict[str, str] | None = None) -> requests.Response:
        started = time.perf_counter()
        response = session.get(url, timeout=self.config.timeout_seconds, headers=headers or {})
        if response.status_code not in {200, 304}:
            response.raise_for_status()
        elapsed = time.perf_counter() - started
        if elapsed >= self.slow_request_seconds:
            self.logger.warning(
                f"Slow request ({elapsed:.1f}s): {url}",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )
        return response

    def _html_filename(self, index: int, row: pd.Series) -> str:
        page_id = _safe_int(str(row.get("id", "")))
        prefix = f"{page_id:04d}" if page_id is not None else f"{index + 1:04d}"
        slug = _slugify(str(row.get("name", "")))
        return f"{prefix}_{slug}.html"

    def _load_delta_map(self) -> dict[str, str]:
        if not self.config.blog_delta_csv.exists():
            return {}

        delta = pd.read_csv(self.config.blog_delta_csv, dtype=str).fillna("")
        if delta.empty or "url" not in delta.columns:
            return {}

        out: dict[str, str] = {}
        for _, row in delta.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                continue
            change = str(row.get("change_type", "")).strip().lower()
            out[url] = change
        return out

    def run_stage(self, force: bool = False) -> dict[str, int]:
        if not self.config.blog_index_csv.exists():
            raise RuntimeError(f"Missing input: {self.config.blog_index_csv}")

        df = pd.read_csv(self.config.blog_index_csv, dtype=str).fillna("")
        if df.empty:
            raise RuntimeError("blog_index.csv is empty")

        for col in ("id", "name", "url", "tags", "published_date", "crawl_ts", "source_hash"):
            if col not in df.columns:
                df[col] = ""

        delta_map = self._load_delta_map()
        existing_meta = self._load_existing_manifest_meta()
        session = self._build_session()
        html_root = self.config.html_cache_root

        metrics = {
            "total_rows": int(len(df)),
            "target_rows": 0,
            "downloaded": 0,
            "validated_modified": 0,
            "downloaded_without_validator": 0,
            "failed": 0,
            "skipped_existing": 0,
            "skipped_not_target": 0,
            "missing_url": 0,
            "validated_not_modified": 0,
            "refreshed_existing": 0,
            "replaced_mismatch": 0,
            "canonical_mismatch": 0,
        }

        manifest_rows: list[dict[str, str]] = []
        started = time.perf_counter()

        for index, row in df.iterrows():
            url = str(row.get("url", "")).strip()
            norm_url = self._normalize_url(url)
            file_name = self._html_filename(index, row)
            local_path = html_root / file_name
            html_exists_before = local_path.exists()

            prev_meta = existing_meta.get(norm_url, {})
            etag = str(prev_meta.get("etag", ""))
            last_modified = str(prev_meta.get("last_modified", ""))
            html_sha256 = str(prev_meta.get("html_sha256", ""))
            canonical_url = str(prev_meta.get("canonical_url", ""))
            canonical_match = str(prev_meta.get("canonical_match", ""))
            http_status = ""
            validation_result = "skipped"

            change_type = delta_map.get(url, "")
            if force:
                # Full mode can skip already-cached HTML unless explicitly enabled.
                target = bool(self.config.s01_refetch_existing_on_full or not html_exists_before)
            else:
                target = bool(change_type in {"new", "updated"} or (not delta_map and not html_exists_before))

            if not url:
                metrics["missing_url"] += 1
                target = False

            if target:
                metrics["target_rows"] += 1

                if local_path.exists():
                    headers = self._conditional_headers(prev_meta)
                    had_validator_headers = bool(headers)
                    try:
                        response = self._fetch(session, url, headers=headers)
                        http_status = str(response.status_code)
                        should_delay = response.status_code == 200

                        if response.status_code == 304:
                            metrics["validated_not_modified"] += 1
                            validation_result = "not_modified"

                            local_html = local_path.read_text(encoding="utf-8", errors="ignore")
                            canonical_url = self._extract_canonical(local_html)
                            canonical_match = "1" if canonical_url and canonical_url == norm_url else "0"
                            html_sha256 = self._sha256_text(local_html)

                            if canonical_match != "1":
                                metrics["canonical_mismatch"] += 1
                                refresh = self._fetch(session, url, headers={})
                                http_status = str(refresh.status_code)
                                if refresh.status_code == 200:
                                    new_html = refresh.text
                                    canonical_url = self._extract_canonical(new_html)
                                    canonical_match = "1" if canonical_url and canonical_url == norm_url else "0"
                                    html_sha256 = self._sha256_text(new_html)
                                    etag = str(refresh.headers.get("ETag", ""))
                                    last_modified = str(refresh.headers.get("Last-Modified", ""))

                                    if not self.dry_run:
                                        local_path.parent.mkdir(parents=True, exist_ok=True)
                                        local_path.write_text(new_html, encoding="utf-8")

                                    metrics["downloaded"] += 1
                                    metrics["downloaded_without_validator"] += 1
                                    metrics["replaced_mismatch"] += 1
                                    validation_result = "refreshed_after_mismatch"
                                    should_delay = True
                        else:
                            html = response.text
                            canonical_url = self._extract_canonical(html)
                            canonical_match = "1" if canonical_url and canonical_url == norm_url else "0"
                            html_sha256 = self._sha256_text(html)
                            etag = str(response.headers.get("ETag", ""))
                            last_modified = str(response.headers.get("Last-Modified", ""))
                            validation_result = "updated"

                            if canonical_match != "1":
                                metrics["canonical_mismatch"] += 1

                            if not self.dry_run:
                                local_path.parent.mkdir(parents=True, exist_ok=True)
                                local_path.write_text(html, encoding="utf-8")

                            metrics["downloaded"] += 1
                            if had_validator_headers:
                                metrics["validated_modified"] += 1
                            else:
                                metrics["downloaded_without_validator"] += 1
                            metrics["refreshed_existing"] += 1

                        if should_delay and self.config.request_delay_seconds > 0:
                            time.sleep(self.config.request_delay_seconds)
                    except Exception as exc:
                        metrics["failed"] += 1
                        validation_result = "failed"
                        self.logger.warning(
                            f"Failed to validate existing HTML: {url} -> {exc}",
                            extra={"stage": self.stage_name, "run_id": self.run_id},
                        )
                else:
                    try:
                        response = self._fetch(session, url, headers={})
                        http_status = str(response.status_code)
                        html = response.text
                        canonical_url = self._extract_canonical(html)
                        canonical_match = "1" if canonical_url and canonical_url == norm_url else "0"
                        html_sha256 = self._sha256_text(html)
                        etag = str(response.headers.get("ETag", ""))
                        last_modified = str(response.headers.get("Last-Modified", ""))
                        validation_result = "fetched"

                        if canonical_match != "1":
                            metrics["canonical_mismatch"] += 1

                        if not self.dry_run:
                            local_path.parent.mkdir(parents=True, exist_ok=True)
                            local_path.write_text(html, encoding="utf-8")
                        metrics["downloaded"] += 1
                        metrics["downloaded_without_validator"] += 1
                        if self.config.request_delay_seconds > 0:
                            time.sleep(self.config.request_delay_seconds)
                    except Exception as exc:
                        metrics["failed"] += 1
                        self.logger.warning(
                            f"Failed to fetch post HTML: {url} -> {exc}",
                            extra={"stage": self.stage_name, "run_id": self.run_id},
                        )
            else:
                metrics["skipped_not_target"] += 1
                if local_path.exists():
                    metrics["skipped_existing"] += 1

            html_exists_after = local_path.exists() if not self.dry_run else html_exists_before
            manifest_rows.append(
                {
                    "id": str(row.get("id", "")),
                    "name": str(row.get("name", "")),
                    "url": url,
                    "tags": str(row.get("tags", "")),
                    "published_date": str(row.get("published_date", "")),
                    "crawl_ts": str(row.get("crawl_ts", "")),
                    "source_hash": str(row.get("source_hash", "")),
                    "html_file": file_name,
                    "html_path": str(local_path.resolve()) if html_exists_after else "",
                    "html_exists": "1" if html_exists_after else "0",
                    "change_type": change_type,
                    "etag": etag,
                    "last_modified": last_modified,
                    "http_status": http_status,
                    "validation_result": validation_result,
                    "canonical_url": canonical_url,
                    "canonical_match": canonical_match,
                    "html_sha256": html_sha256,
                }
            )

            processed_rows = len(manifest_rows)
            if processed_rows > 0 and processed_rows % self.progress_log_every == 0:
                elapsed = time.perf_counter() - started
                self.logger.info(
                    (
                        f"HTML fetch progress: targets={metrics['target_rows']} "
                        f"downloaded={metrics['downloaded']} failed={metrics['failed']} "
                        f"validated_modified={metrics['validated_modified']} "
                        f"downloaded_without_validator={metrics['downloaded_without_validator']} "
                        f"not_modified={metrics['validated_not_modified']} "
                        f"mismatch={metrics['canonical_mismatch']} "
                        f"elapsed={elapsed:.1f}s"
                    ),
                    extra={"stage": self.stage_name, "run_id": self.run_id},
                )
                self._write_progress_checkpoint(manifest_rows=manifest_rows, metrics=metrics)

        manifest_df = pd.DataFrame(manifest_rows)

        if not self.dry_run:
            self.config.blog_index_html_csv.parent.mkdir(parents=True, exist_ok=True)
            manifest_df.to_csv(self.config.blog_index_html_csv, index=False, encoding="utf-8-sig")

            checkpoint_path = self._checkpoint_manifest_path()
            if checkpoint_path.exists():
                checkpoint_path.unlink()

            state_payload = {
                "stage": self.stage_name,
                "run_id": self.run_id,
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "status": "completed",
                "metrics": metrics,
                "output_files": [str(self.config.blog_index_html_csv)],
            }
            self._write_json(self.config.s01_state_file, state_payload)

        self.logger.info(
            (
                f"s01_fetch_html stats: total={metrics['total_rows']} targets={metrics['target_rows']} "
                f"downloaded={metrics['downloaded']} failed={metrics['failed']} "
                f"validated_modified={metrics['validated_modified']} "
                f"downloaded_without_validator={metrics['downloaded_without_validator']} "
                f"skip_existing={metrics['skipped_existing']} "
                f"not_modified={metrics['validated_not_modified']} "
                f"replaced_mismatch={metrics['replaced_mismatch']} "
                f"canonical_mismatch={metrics['canonical_mismatch']}"
            ),
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return metrics
