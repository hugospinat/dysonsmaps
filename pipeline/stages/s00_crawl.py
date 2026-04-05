from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .stage_base import StageBase


URL_DATE_RE = re.compile(r"/(20\d{2})/(\d{2})/(\d{2})/")
POST_URL_RE = re.compile(r"^https?://dysonlogos\.blog/20\d{2}/\d{2}/\d{2}/[^/]+/?$")

GROUP_LABEL_KEYWORDS = {
    "geomorph",
    "downloadable",
    "adventures",
    "dysons delve",
    "dyson's delve",
    "kevin campbell",
    "urban",
    "cities",
    "towns",
    "multi-map",
    "multi-page",
    "megadelve",
    "jakalla",
    "darkling",
    "barrier peaks",
    "sewers",
}


def _slug(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip())
    return value


def _extract_date_from_url(url: str) -> str:
    match = URL_DATE_RE.search(url or "")
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"


def _extract_post_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    seen: set[str] = set()

    selectors = [
        "article h2.entry-title a[href]",
        "article h1.entry-title a[href]",
        "article a[rel='bookmark'][href]",
    ]

    for selector in selectors:
        for anchor in soup.select(selector):
            href = str(anchor.get("href", "")).strip()
            if not href:
                continue
            url = urljoin(base_url, href)
            if "/20" not in url:
                continue
            if url in seen:
                continue
            title = _slug(anchor.get_text(" ", strip=True))
            if not title:
                continue
            seen.add(url)
            items.append((url, title))

    # Fallback for pages where map links are rendered without entry-title wrappers.
    if not items:
        for anchor in soup.find_all("a", href=True):
            href = str(anchor.get("href", "")).strip()
            if not href:
                continue
            url = urljoin(base_url, href)
            if not POST_URL_RE.match(url):
                continue
            if url in seen:
                continue
            seen.add(url)
            title = _slug(anchor.get_text(" ", strip=True))
            items.append((url, title))

    return items


def _normalize_label(value: str) -> str:
    text = _slug(value).lower()
    text = re.sub(r"[^a-z0-9\s!']+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _looks_like_group_label(label: str) -> bool:
    norm = _normalize_label(label)
    return any(keyword in norm for keyword in GROUP_LABEL_KEYWORDS)


def _extract_group_archive_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    group_links: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        label = _slug(anchor.get_text(" ", strip=True))
        if not href or not label:
            continue

        url = urljoin(base_url, href)
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.rstrip("/").lower()

        if parsed.query or parsed.fragment:
            continue
        if "dysonlogos.blog" not in host:
            continue
        if POST_URL_RE.match(url):
            continue
        if path in {"", "/maps"}:
            continue

        in_maps_section = path.startswith("/maps/")
        explicit_group_label = _looks_like_group_label(label)
        explicit_group_path = "/zerobarrier/dysons-delves" in path

        if not (in_maps_section or explicit_group_label or explicit_group_path):
            continue

        if url in seen:
            continue

        seen.add(url)
        group_links.append((url, label))

    return group_links


def _find_next_page(soup: BeautifulSoup, current_url: str) -> str | None:
    selectors = [
        "a.next.page-numbers[href]",
        "a.next[href]",
        "a[rel='next'][href]",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node and node.get("href"):
            return urljoin(current_url, str(node.get("href")))
    return None


def _record_hash(url: str, title: str, tags: str, published_date: str) -> str:
    value = "||".join([url.strip(), title.strip(), tags.strip(), published_date.strip()])
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


class CrawlMapsStage(StageBase):
    stage_name = "s00_crawl"
    progress_log_every = 10
    slow_request_seconds = 8.0

    def output_paths(self) -> list[str]:
        return [
            str(self.config.blog_index_csv),
            str(self.config.blog_delta_csv),
            str(self.config.s00_state_file),
        ]

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": self.config.user_agent})

        retry = Retry(total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _fetch(self, session: requests.Session, url: str) -> str:
        started = time.perf_counter()
        response = session.get(url, timeout=self.config.timeout_seconds)
        response.raise_for_status()
        elapsed = time.perf_counter() - started
        if elapsed >= self.slow_request_seconds:
            self.logger.warning(
                f"Slow request ({elapsed:.1f}s): {url}",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )
        return response.text

    def _title_from_url(self, url: str) -> str:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        return slug.replace("-", " ").strip() or url

    def _crawl_posts(self, session: requests.Session) -> list[dict[str, str]]:
        now_iso = datetime.now(timezone.utc).isoformat()
        posts: list[dict[str, str]] = []
        seen_urls: set[str] = set()
        crawl_started = time.perf_counter()

        archive_queue: list[str] = [self.config.maps_index_url]
        seen_archive_urls: set[str] = set()
        page_count = 0

        while archive_queue and page_count < self.config.max_pages:
            page_url = archive_queue.pop(0)
            if page_url in seen_archive_urls:
                continue

            seen_archive_urls.add(page_url)
            page_count += 1
            self.logger.info(
                f"Crawling archive page {page_count}: {page_url} (queue_remaining={len(archive_queue)})",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )
            html = self._fetch(session, page_url)
            soup = BeautifulSoup(html, "html.parser")

            if page_url == self.config.maps_index_url:
                group_links = _extract_group_archive_links(soup, page_url)
                for group_url, _ in group_links:
                    if group_url not in seen_archive_urls and group_url not in archive_queue:
                        archive_queue.append(group_url)
                if group_links:
                    self.logger.info(
                        (
                            f"Discovered {len(group_links)} group archive links from maps page: "
                            + ", ".join(name for _, name in group_links)
                        ),
                        extra={"stage": self.stage_name, "run_id": self.run_id},
                    )

            links = _extract_post_links(soup, page_url)
            self.logger.info(
                (
                    f"Archive page {page_count} discovered {len(links)} candidate posts "
                    f"(unique collected: {len(seen_urls)})"
                ),
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )

            for url, title in links:
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                if self.config.max_posts > 0 and len(posts) >= self.config.max_posts:
                    break

                # Index-only crawl: do not request individual post pages here.
                title = _slug(title) or self._title_from_url(url)
                published = _extract_date_from_url(url)
                tags = ""

                source_hash = _record_hash(url=url, title=title, tags=tags, published_date=published)
                posts.append(
                    {
                        "name": title,
                        "url": url,
                        "tags": tags,
                        "published_date": published,
                        "crawl_ts": now_iso,
                        "source_hash": source_hash,
                    }
                )

                if len(posts) % self.progress_log_every == 0:
                    elapsed = time.perf_counter() - crawl_started
                    self.logger.info(
                        (
                            f"Crawl progress: processed={len(posts)} posts "
                            f"after {elapsed:.1f}s, latest_date={published or 'unknown'}"
                        ),
                        extra={"stage": self.stage_name, "run_id": self.run_id},
                    )

            if self.config.max_posts > 0 and len(posts) >= self.config.max_posts:
                break

            next_page = _find_next_page(soup, page_url)
            if next_page and next_page != page_url:
                if next_page not in seen_archive_urls and next_page not in archive_queue:
                    archive_queue.append(next_page)
                    self.logger.info(
                        f"Queued pagination page: {next_page}",
                        extra={"stage": self.stage_name, "run_id": self.run_id},
                    )

            if not archive_queue:
                elapsed = time.perf_counter() - crawl_started
                self.logger.info(
                    f"Archive queue exhausted after page {page_count}; crawl stopping at {len(posts)} posts in {elapsed:.1f}s",
                    extra={"stage": self.stage_name, "run_id": self.run_id},
                )
                break

            time.sleep(self.config.request_delay_seconds)

        elapsed = time.perf_counter() - crawl_started
        self.logger.info(
            f"Crawl loop finished: pages={page_count}, posts={len(posts)}, duration={elapsed:.1f}s",
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return posts

    def _with_ids(self, rows: list[dict[str, str]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["id", "name", "url", "tags", "published_date", "crawl_ts", "source_hash"])

        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["url"], keep="first").copy()
        df = df.sort_values(by=["published_date", "url"], ascending=[False, True]).reset_index(drop=True)
        df.insert(0, "id", df.index.astype(int))
        return df

    def _enrich_with_previous_metadata(self, crawled_df: pd.DataFrame, previous_df: pd.DataFrame | None) -> pd.DataFrame:
        if previous_df is None or previous_df.empty:
            return crawled_df

        prev = previous_df.copy()
        for col in ("name", "url", "tags", "published_date", "crawl_ts", "source_hash"):
            if col not in prev.columns:
                prev[col] = ""

        prev_by_url = prev.set_index("url").to_dict(orient="index")
        out = crawled_df.copy()

        for idx, row in out.iterrows():
            url = str(row.get("url", "")).strip()
            if not url or url not in prev_by_url:
                continue

            prev_row = prev_by_url[url]

            if not str(row.get("name", "")).strip() and str(prev_row.get("name", "")).strip():
                out.at[idx, "name"] = str(prev_row.get("name", ""))

            if not str(row.get("published_date", "")).strip() and str(prev_row.get("published_date", "")).strip():
                out.at[idx, "published_date"] = str(prev_row.get("published_date", ""))

            # Keep richer tags/source hash from previous run when available.
            if str(prev_row.get("tags", "")).strip():
                out.at[idx, "tags"] = str(prev_row.get("tags", ""))
            if str(prev_row.get("source_hash", "")).strip():
                out.at[idx, "source_hash"] = str(prev_row.get("source_hash", ""))

        return out

    def _merge_with_history(self, crawled_df: pd.DataFrame, previous_df: pd.DataFrame | None) -> tuple[pd.DataFrame, int]:
        if previous_df is None or previous_df.empty or not self.config.carry_forward_missing:
            return crawled_df, 0

        prev = previous_df.copy()
        for col in ("name", "url", "tags", "published_date", "crawl_ts", "source_hash"):
            if col not in prev.columns:
                prev[col] = ""

        crawled_urls = set(crawled_df["url"].astype(str).str.strip())
        carry_df = prev[~prev["url"].astype(str).str.strip().isin(crawled_urls)].copy()

        if carry_df.empty:
            return crawled_df, 0

        for idx, row in carry_df.iterrows():
            if not str(row.get("published_date", "")).strip():
                carry_df.at[idx, "published_date"] = _extract_date_from_url(str(row.get("url", "")))
            if not str(row.get("source_hash", "")).strip():
                carry_df.at[idx, "source_hash"] = _record_hash(
                    url=str(row.get("url", "")),
                    title=str(row.get("name", "")),
                    tags=str(row.get("tags", "")),
                    published_date=str(carry_df.at[idx, "published_date"]),
                )

        crawled_no_id = crawled_df.drop(columns=["id"], errors="ignore")
        merged = pd.concat([crawled_no_id, carry_df[["name", "url", "tags", "published_date", "crawl_ts", "source_hash"]]], ignore_index=True)
        merged = merged.drop_duplicates(subset=["url"], keep="first").copy()
        merged = merged.sort_values(by=["published_date", "url"], ascending=[False, True]).reset_index(drop=True)
        merged = merged.drop(columns=["id"], errors="ignore")
        merged.insert(0, "id", merged.index.astype(int))
        return merged, int(len(carry_df))

    def _build_delta(self, current_df: pd.DataFrame, previous_df: pd.DataFrame | None) -> tuple[pd.DataFrame, dict[str, int]]:
        if previous_df is None or previous_df.empty:
            delta = current_df[["url", "name", "published_date", "source_hash"]].copy()
            delta.insert(0, "change_type", "new")
            return delta, {"new": len(delta), "updated": 0, "unchanged": 0}

        prev_map = previous_df.set_index("url")["source_hash"].to_dict()
        current_map = current_df.set_index("url")["source_hash"].to_dict()

        new_urls = sorted(set(current_map.keys()) - set(prev_map.keys()))
        common_urls = sorted(set(current_map.keys()) & set(prev_map.keys()))

        updated_urls = [url for url in common_urls if current_map[url] != prev_map[url]]
        unchanged_count = len(common_urls) - len(updated_urls)

        rows: list[dict[str, Any]] = []
        for url in new_urls:
            record = current_df[current_df["url"] == url].iloc[0]
            rows.append(
                {
                    "change_type": "new",
                    "url": url,
                    "name": record.get("name", ""),
                    "published_date": record.get("published_date", ""),
                    "old_source_hash": "",
                    "new_source_hash": current_map[url],
                }
            )
        for url in updated_urls:
            record = current_df[current_df["url"] == url].iloc[0]
            rows.append(
                {
                    "change_type": "updated",
                    "url": url,
                    "name": record.get("name", ""),
                    "published_date": record.get("published_date", ""),
                    "old_source_hash": prev_map[url],
                    "new_source_hash": current_map[url],
                }
            )

        delta_df = pd.DataFrame(rows)
        if delta_df.empty:
            delta_df = pd.DataFrame(
                columns=["change_type", "url", "name", "published_date", "old_source_hash", "new_source_hash"]
            )

        return delta_df, {"new": len(new_urls), "updated": len(updated_urls), "unchanged": unchanged_count}

    def _load_previous_index(self) -> pd.DataFrame | None:
        if not self.config.blog_index_csv.exists():
            if self.config.legacy_seed_csv.exists():
                legacy = pd.read_csv(self.config.legacy_seed_csv, dtype=str).fillna("")
                for col in ("published_date", "crawl_ts", "source_hash"):
                    if col not in legacy.columns:
                        legacy[col] = ""
                keep = ["id", "name", "url", "tags", "published_date", "crawl_ts", "source_hash"]
                return legacy[keep].copy()
            return None

        prev = pd.read_csv(self.config.blog_index_csv, dtype=str).fillna("")
        for col in ("id", "name", "url", "tags", "published_date", "crawl_ts", "source_hash"):
            if col not in prev.columns:
                prev[col] = ""
        return prev[["id", "name", "url", "tags", "published_date", "crawl_ts", "source_hash"]].copy()

    def run_stage(self, force: bool = False) -> dict[str, Any]:
        previous_df = self._load_previous_index()

        session = self._build_session()
        rows = self._crawl_posts(session)
        if not rows:
            raise RuntimeError("No map entries found from maps index crawl.")

        crawled_df = self._with_ids(rows)
        crawled_df = self._enrich_with_previous_metadata(crawled_df, previous_df)
        current_df, carried_forward = self._merge_with_history(crawled_df, previous_df)
        if carried_forward > 0:
            self.logger.info(
                f"Carry-forward applied: {carried_forward} historical URLs not present on current maps page.",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )

        delta_df, delta_counts = self._build_delta(current_df, previous_df)

        if not self.dry_run:
            current_df.to_csv(self.config.blog_index_csv, index=False, encoding="utf-8-sig")
            delta_df.to_csv(self.config.blog_delta_csv, index=False, encoding="utf-8-sig")

            state_payload = {
                "stage": self.stage_name,
                "run_id": self.run_id,
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "total_rows": int(len(current_df)),
                "delta": delta_counts,
                "output_files": [str(self.config.blog_index_csv), str(self.config.blog_delta_csv)],
            }
            self._write_json(self.config.s00_state_file, state_payload)

        self.logger.info(
            (
                f"s00_crawl stats: total={len(current_df)} "
                f"new={delta_counts['new']} updated={delta_counts['updated']} unchanged={delta_counts['unchanged']}"
            ),
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return {
            "total_rows": int(len(current_df)),
            "discovered_rows": int(len(crawled_df)),
            "carried_forward": int(carried_forward),
            "new": int(delta_counts["new"]),
            "updated": int(delta_counts["updated"]),
            "unchanged": int(delta_counts["unchanged"]),
        }
