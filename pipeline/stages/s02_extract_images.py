from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

from .stage_base import StageBase

IMAGE_EXTS = {"png", "jpg", "jpeg", "webp", "gif"}
ARCHIVE_EXTS = {"zip", "7z", "rar", "tar", "gz"}
NOISE_PATTERNS = {
    "patreon",
    "banner",
    "avatar",
    "cropped-new-dice-header",
    "become-a-patron",
    "gravatar",
    "blavatar",
}


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _get_ext(url: str | None) -> str:
    if not url:
        return ""
    path = urlparse(url).path.lower()
    if "." not in path:
        return ""
    return path.rsplit(".", 1)[-1]


def _is_image_url(url: str | None) -> bool:
    return _get_ext(url) in IMAGE_EXTS


def _is_archive_url(url: str | None) -> bool:
    return _get_ext(url) in ARCHIVE_EXTS


def _looks_like_noise(url: str | None, alt: str | None = None) -> bool:
    text = " ".join(x for x in [url or "", alt or ""]).lower()
    return any(pattern in text for pattern in NOISE_PATTERNS)


class ExtractImagesStage(StageBase):
    stage_name = "s02_extract_images"
    progress_log_every = 100

    def output_paths(self) -> list[str]:
        return [
            str(self.config.image_inventory_json),
            str(self.config.image_inventory_csv),
            str(self.config.s02_state_file),
        ]

    def _normalize_url(self, value: str) -> str:
        return str(value or "").strip().rstrip("/")

    def _load_delta_map(self) -> dict[str, str]:
        path = self.config.blog_delta_csv
        if not path.exists():
            return {}

        delta = pd.read_csv(path, dtype=str).fillna("")
        if delta.empty or "url" not in delta.columns:
            return {}

        out: dict[str, str] = {}
        for _, row in delta.iterrows():
            key = self._normalize_url(str(row.get("url", "")))
            if not key:
                continue
            out[key] = str(row.get("change_type", "")).strip().lower()
        return out

    def _normalize_previous_page(self, page: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(page.get("id", "")),
            "name": str(page.get("name", "")),
            "url": str(page.get("url", "")),
            "published_date": str(page.get("published_date", "")),
            "html_file": str(page.get("html_file", "")),
            "html_path": str(page.get("html_path", "")),
            "title": str(page.get("title", "")),
            "canonical_url": str(page.get("canonical_url", "")),
            "tags": [str(x) for x in (page.get("tags", []) or []) if str(x).strip()],
            "zip_links": [str(x) for x in (page.get("zip_links", []) or []) if str(x).strip()],
            "best_candidate": page.get("best_candidate") or {},
            "candidates": page.get("candidates") or [],
            "error": str(page.get("error", "")),
        }

    def _load_previous_inventory(self) -> dict[str, dict[str, Any]]:
        path = self.config.image_inventory_json
        if not path.exists():
            return {}

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

        if not isinstance(payload, list):
            return {}

        out: dict[str, dict[str, Any]] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            key = self._normalize_url(str(item.get("url", "")))
            if not key:
                continue
            out[key] = self._normalize_previous_page(item)
        return out

    def _extract_tags(self, article) -> list[str]:
        tags: list[str] = []

        for tag_link in article.select(".post-extras a[rel='tag']"):
            text = _clean_text(tag_link.get_text(" ", strip=True))
            if text:
                tags.append(text)

        for class_name in article.get("class", []) or []:
            if class_name.startswith("tag-"):
                text = class_name[4:].replace("-", " ").strip()
                if text:
                    tags.append(text)

        return _dedupe_keep_order(tags)

    def _score_candidate(self, candidate: dict[str, Any]) -> int:
        score = 0

        href = str(candidate.get("href", "") or "")
        orig = str(candidate.get("orig", "") or "")
        src = str(candidate.get("src", "") or "")
        alt = str(candidate.get("alt", "") or "")
        caption = str(candidate.get("caption", "") or "")

        if _is_archive_url(href):
            score += 120
        if _is_image_url(orig):
            score += 90
        if _is_image_url(href):
            score += 60
        if _is_image_url(src):
            score += 20

        if orig and "/wp-content/uploads/" in orig:
            score += 15
        if href and "/wp-content/uploads/" in href:
            score += 15

        if caption:
            score += 10
        if alt:
            score += 5

        if _looks_like_noise(orig or href or src, alt):
            likely_real_map = (
                (_is_image_url(orig) or _is_image_url(href) or _is_image_url(src))
                and (
                    (orig and "/wp-content/uploads/" in orig)
                    or (href and "/wp-content/uploads/" in href)
                    or (src and "/wp-content/uploads/" in src)
                )
            )
            score -= 80 if likely_real_map else 500

        return score

    def _extract_candidates(self, post_entry) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []

        for container in post_entry.select("figure, div.wp-caption"):
            img = container.select_one("img")
            if not img:
                continue

            parent_link = img.find_parent("a")
            href = parent_link.get("href") if parent_link else ""
            caption_el = container.select_one("figcaption, .wp-caption-text")
            candidate = {
                "href": href or "",
                "href_type": (
                    "archive"
                    if _is_archive_url(href)
                    else "image"
                    if _is_image_url(href)
                    else "other"
                    if href
                    else ""
                ),
                "orig": str(img.get("data-orig-file") or ""),
                "src": str(img.get("src") or ""),
                "alt": _clean_text(img.get("alt")),
                "title": _clean_text(img.get("data-image-title")),
                "caption": _clean_text(caption_el.get_text(" ", strip=True) if caption_el else ""),
            }
            candidate["score"] = self._score_candidate(candidate)
            if int(candidate["score"]) > -100:
                candidates.append(candidate)

        if not candidates:
            for img in post_entry.select("img"):
                parent_link = img.find_parent("a")
                href = parent_link.get("href") if parent_link else ""
                candidate = {
                    "href": href or "",
                    "href_type": (
                        "archive"
                        if _is_archive_url(href)
                        else "image"
                        if _is_image_url(href)
                        else "other"
                        if href
                        else ""
                    ),
                    "orig": str(img.get("data-orig-file") or ""),
                    "src": str(img.get("src") or ""),
                    "alt": _clean_text(img.get("alt")),
                    "title": _clean_text(img.get("data-image-title")),
                    "caption": "",
                }
                candidate["score"] = self._score_candidate(candidate)
                if int(candidate["score"]) > -100:
                    candidates.append(candidate)

        candidates.sort(key=lambda item: int(item.get("score", -10000)), reverse=True)
        return candidates

    def _empty_page(self, meta: dict[str, str], error: str) -> dict[str, Any]:
        return {
            "id": str(meta.get("id", "")),
            "name": str(meta.get("name", "")),
            "url": str(meta.get("url", "")),
            "published_date": str(meta.get("published_date", "")),
            "html_file": str(meta.get("html_file", "")),
            "html_path": str(meta.get("html_path", "")),
            "title": str(meta.get("name", "")),
            "canonical_url": "",
            "tags": [],
            "zip_links": [],
            "best_candidate": {},
            "candidates": [],
            "error": error,
        }

    def _parse_html_file(self, meta: dict[str, str], html_path: Path) -> dict[str, Any]:
        html = html_path.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")

        canonical = soup.select_one("link[rel='canonical']")
        canonical_url = str(canonical.get("href") or "") if canonical else ""

        article = soup.select_one("article.post") or soup.select_one("article")
        if not article:
            return self._empty_page(meta, "article_not_found")

        title_el = article.select_one("header.post-title h1") or article.select_one("h1")
        page_title = _clean_text(title_el.get_text(" ", strip=True) if title_el else meta.get("name", ""))

        post_entry = article.select_one("div.post-entry, div.post-content, div.entry-content") or article
        if not post_entry:
            return self._empty_page(meta, "content_not_found")

        tags = self._extract_tags(article)
        candidates = self._extract_candidates(post_entry)

        zip_links = [str(c.get("href", "")) for c in candidates if _is_archive_url(c.get("href"))]
        zip_links.extend(
            str(link.get("href") or "")
            for link in post_entry.select("a[href]")
            if _is_archive_url(link.get("href"))
        )
        zip_links = _dedupe_keep_order([value for value in zip_links if value])

        return {
            "id": str(meta.get("id", "")),
            "name": str(meta.get("name", "")),
            "url": str(meta.get("url", "")),
            "published_date": str(meta.get("published_date", "")),
            "html_file": str(meta.get("html_file", "")),
            "html_path": str(meta.get("html_path", "")),
            "title": page_title,
            "canonical_url": canonical_url,
            "tags": tags,
            "zip_links": zip_links,
            "best_candidate": candidates[0] if candidates else {},
            "candidates": candidates,
            "error": "",
        }

    def _parse_task_safe(self, task: tuple[int, dict[str, str], Path]) -> tuple[int, dict[str, Any]]:
        index, meta, html_path = task
        try:
            return index, self._parse_html_file(meta, html_path)
        except Exception as exc:
            return index, self._empty_page(meta, f"parse_error: {exc}")

    def _write_progress_state(self, metrics: dict[str, int], processed_targets: int) -> None:
        if self.dry_run:
            return
        payload = {
            "stage": self.stage_name,
            "run_id": self.run_id,
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "processed_targets": int(processed_targets),
            "metrics": metrics,
            "output_files": [str(self.config.image_inventory_json), str(self.config.image_inventory_csv)],
        }
        self._write_json(self.config.s02_state_file, payload)

    def _csv_rows_from_pages(self, pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out_rows: list[dict[str, Any]] = []

        for page in pages:
            base = {
                "id": str(page.get("id", "")),
                "name": str(page.get("name", "")),
                "url": str(page.get("url", "")),
                "published_date": str(page.get("published_date", "")),
                "html_file": str(page.get("html_file", "")),
                "html_path": str(page.get("html_path", "")),
                "title": str(page.get("title", "")),
                "canonical_url": str(page.get("canonical_url", "")),
                "tags": " | ".join(str(x) for x in (page.get("tags") or [])),
                "zip_links": " | ".join(str(x) for x in (page.get("zip_links") or [])),
                "error": str(page.get("error", "")),
            }

            candidates = page.get("candidates") or []
            if candidates:
                for rank, candidate in enumerate(candidates, start=1):
                    out_rows.append(
                        {
                            **base,
                            "candidate_rank": rank,
                            "candidate_score": candidate.get("score", ""),
                            "is_best_candidate": 1 if rank == 1 else 0,
                            "candidate_href": candidate.get("href", ""),
                            "candidate_href_type": candidate.get("href_type", ""),
                            "candidate_orig": candidate.get("orig", ""),
                            "candidate_src": candidate.get("src", ""),
                            "candidate_alt": candidate.get("alt", ""),
                            "candidate_title": candidate.get("title", ""),
                            "candidate_caption": candidate.get("caption", ""),
                        }
                    )
            else:
                out_rows.append(
                    {
                        **base,
                        "candidate_rank": "",
                        "candidate_score": "",
                        "is_best_candidate": 0,
                        "candidate_href": "",
                        "candidate_href_type": "",
                        "candidate_orig": "",
                        "candidate_src": "",
                        "candidate_alt": "",
                        "candidate_title": "",
                        "candidate_caption": "",
                    }
                )

        return out_rows

    def run_stage(self, force: bool = False) -> dict[str, int]:
        if not self.config.blog_index_html_csv.exists():
            raise RuntimeError(f"Missing input: {self.config.blog_index_html_csv}")

        manifest = pd.read_csv(self.config.blog_index_html_csv, dtype=str).fillna("")
        if manifest.empty:
            raise RuntimeError("blog_index_html.csv is empty")

        for col in ("id", "name", "url", "published_date", "html_file", "html_path", "html_exists", "change_type"):
            if col not in manifest.columns:
                manifest[col] = ""

        delta_map = self._load_delta_map()
        previous_map = self._load_previous_inventory()

        metrics: dict[str, int] = {
            "total_rows": int(len(manifest)),
            "html_rows": 0,
            "target_rows": 0,
            "processed_targets": 0,
            "carried_forward": 0,
            "parsed_ok": 0,
            "failed": 0,
            "html_missing": 0,
            "rows_with_candidates": 0,
            "rows_with_zip_links": 0,
            "candidate_rows": 0,
        }

        ordered_pages: list[dict[str, Any] | None] = [None] * int(len(manifest))
        tasks: list[tuple[int, dict[str, str], Path]] = []

        for index, row in manifest.iterrows():
            meta = {
                "id": str(row.get("id", "")),
                "name": str(row.get("name", "")),
                "url": str(row.get("url", "")),
                "published_date": str(row.get("published_date", "")),
                "html_file": str(row.get("html_file", "")),
                "html_path": str(row.get("html_path", "")),
            }

            url_key = self._normalize_url(meta["url"])
            raw_path = meta["html_path"].strip()
            html_path: Path | None = None
            if raw_path:
                html_path = Path(raw_path)
                if not html_path.is_absolute():
                    html_path = (self.config.workspace_root / raw_path).resolve()
            elif meta["html_file"].strip():
                html_path = self.config.html_cache_root / meta["html_file"].strip()

            if html_path is None or not html_path.exists() or not html_path.is_file():
                metrics["html_missing"] += 1
                ordered_pages[int(index)] = self._empty_page(meta, "html_missing")
                continue

            metrics["html_rows"] += 1
            change_type = str(row.get("change_type", "")).strip().lower()
            should_parse = bool(
                force
                or not delta_map
                or change_type in {"new", "updated"}
                or (url_key and url_key not in previous_map)
            )

            if should_parse:
                metrics["target_rows"] += 1
                tasks.append((int(index), meta, html_path))
            else:
                previous_page = previous_map.get(url_key)
                if previous_page:
                    ordered_pages[int(index)] = previous_page
                    metrics["carried_forward"] += 1
                else:
                    metrics["target_rows"] += 1
                    tasks.append((int(index), meta, html_path))

        started = time.perf_counter()
        if tasks:
            max_workers = max(1, min(int(self.config.s02_max_workers), len(tasks)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for done, (index, page) in enumerate(executor.map(self._parse_task_safe, tasks), start=1):
                    ordered_pages[index] = page
                    metrics["processed_targets"] = done

                    if done % self.progress_log_every == 0 or done == len(tasks):
                        elapsed = time.perf_counter() - started
                        self.logger.info(
                            (
                                f"Image extract progress: targets={len(tasks)} "
                                f"processed={done} carried_forward={metrics['carried_forward']} "
                                f"elapsed={elapsed:.1f}s"
                            ),
                            extra={"stage": self.stage_name, "run_id": self.run_id},
                        )
                        self._write_progress_state(metrics=metrics, processed_targets=done)

        for idx, page in enumerate(ordered_pages):
            if page is None:
                fallback_meta = {
                    "id": str(manifest.iloc[idx].get("id", "")),
                    "name": str(manifest.iloc[idx].get("name", "")),
                    "url": str(manifest.iloc[idx].get("url", "")),
                    "published_date": str(manifest.iloc[idx].get("published_date", "")),
                    "html_file": str(manifest.iloc[idx].get("html_file", "")),
                    "html_path": str(manifest.iloc[idx].get("html_path", "")),
                }
                ordered_pages[idx] = self._empty_page(fallback_meta, "unresolved_row")

        page_results: list[dict[str, Any]] = [page for page in ordered_pages if page is not None]
        for page in page_results:
            if page.get("error"):
                metrics["failed"] += 1
            else:
                metrics["parsed_ok"] += 1
            if page.get("candidates"):
                metrics["rows_with_candidates"] += 1
            if page.get("zip_links"):
                metrics["rows_with_zip_links"] += 1

        csv_rows = self._csv_rows_from_pages(page_results)
        metrics["candidate_rows"] = int(len(csv_rows))

        if not self.dry_run:
            self.config.image_inventory_json.parent.mkdir(parents=True, exist_ok=True)
            self.config.image_inventory_json.write_text(
                json.dumps(page_results, indent=2, ensure_ascii=True),
                encoding="utf-8",
            )
            pd.DataFrame(csv_rows).to_csv(self.config.image_inventory_csv, index=False, encoding="utf-8-sig")

            state_payload = {
                "stage": self.stage_name,
                "run_id": self.run_id,
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "status": "completed",
                "metrics": metrics,
                "output_files": [str(self.config.image_inventory_json), str(self.config.image_inventory_csv)],
            }
            self._write_json(self.config.s02_state_file, state_payload)

        self.logger.info(
            (
                f"s02_extract_images stats: total={metrics['total_rows']} html_rows={metrics['html_rows']} "
                f"targets={metrics['target_rows']} processed={metrics['processed_targets']} "
                f"carried_forward={metrics['carried_forward']} parsed_ok={metrics['parsed_ok']} "
                f"failed={metrics['failed']} candidates={metrics['rows_with_candidates']}"
            ),
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return metrics
