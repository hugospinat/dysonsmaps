from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .stage_base import StageBase


URL_DATE_RE = re.compile(r"/(20\d{2})/(\d{2})/(\d{2})/")


def _slug(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "").strip())
    return value


def _extract_date_from_url(url: str) -> str:
    match = URL_DATE_RE.search(url or "")
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _xml_child_text(node: ET.Element, child_name: str) -> str:
    for child in list(node):
        if _local_name(str(child.tag)) == child_name:
            return _slug(child.text or "")
    return ""


def _xml_children_text(node: ET.Element, child_name: str) -> list[str]:
    out: list[str] = []
    for child in list(node):
        if _local_name(str(child.tag)) != child_name:
            continue
        value = _slug(child.text or "")
        if value:
            out.append(value)
    return out


def _rss_items(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)

    channel: ET.Element | None = None
    if _local_name(str(root.tag)) == "channel":
        channel = root
    else:
        for node in root.iter():
            if _local_name(str(node.tag)) == "channel":
                channel = node
                break

    if channel is None:
        raise RuntimeError("RSS parse error: channel node not found.")

    items: list[dict[str, Any]] = []
    for node in list(channel):
        if _local_name(str(node.tag)) != "item":
            continue
        items.append(
            {
                "title": _xml_child_text(node, "title"),
                "url": _xml_child_text(node, "link"),
                "pub_date_raw": _xml_child_text(node, "pubDate"),
                "categories": _xml_children_text(node, "category"),
            }
        )
    return items


def _published_date_from_rss(pub_date_raw: str, url: str) -> str:
    text = _slug(pub_date_raw)
    if text:
        try:
            parsed = parsedate_to_datetime(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.date().isoformat()
        except (TypeError, ValueError, OverflowError):
            pass
    return _extract_date_from_url(url)


def _rss_page_url(base_url: str, page_number: int) -> str:
    if page_number <= 1:
        return base_url

    parsed = urlsplit(base_url)
    query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_pairs["paged"] = str(page_number)
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query_pairs),
            parsed.fragment,
        )
    )


def _record_hash(url: str, title: str, tags: str, published_date: str) -> str:
    value = "||".join(
        [url.strip(), title.strip(), tags.strip(), published_date.strip()]
    )
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


class CrawlMapsStage(StageBase):
    stage_name = "s00_crawl"
    progress_log_every = 10
    slow_request_seconds = 8.0
    required_category = "Maps"

    def output_paths(self) -> list[str]:
        return [
            str(self.config.blog_index_csv),
            str(self.config.blog_delta_csv),
            str(self.config.s00_state_file),
        ]

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": self.config.user_agent})

        retry = Retry(
            total=3, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504]
        )
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
        seen_page_signatures: set[tuple[str, ...]] = set()
        crawl_started = time.perf_counter()
        total_feed_items = 0
        pages_fetched = 0

        self.logger.info(
            f"Fetching RSS feed: {self.config.maps_rss_url}",
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        page_number = 1
        while True:
            if self.config.max_pages > 0 and page_number > self.config.max_pages:
                self.logger.info(
                    (
                        "RSS pagination stopped due to max_pages limit: "
                        f"{self.config.max_pages}"
                    ),
                    extra={"stage": self.stage_name, "run_id": self.run_id},
                )
                break

            page_url = _rss_page_url(self.config.maps_rss_url, page_number)
            xml_text = self._fetch(session, page_url)
            items = _rss_items(xml_text)
            pages_fetched += 1

            if not items:
                self.logger.info(
                    f"RSS page {page_number} returned 0 items; stopping pagination.",
                    extra={"stage": self.stage_name, "run_id": self.run_id},
                )
                break

            total_feed_items += len(items)
            page_urls = sorted(
                {
                    _slug(str(item.get("url", "")))
                    for item in items
                    if _slug(str(item.get("url", "")))
                }
            )
            page_signature = tuple(page_urls)
            if page_signature in seen_page_signatures:
                self.logger.info(
                    f"RSS page {page_number} duplicated previous page content; stopping pagination.",
                    extra={"stage": self.stage_name, "run_id": self.run_id},
                )
                break
            seen_page_signatures.add(page_signature)

            self.logger.info(
                f"RSS page {page_number}: {len(items)} items before filters.",
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )

            for item in items:
                url = _slug(str(item.get("url", "")))
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                categories = [
                    value for value in item.get("categories", []) if _slug(value)
                ]
                if self.required_category not in categories:
                    continue

                if self.config.max_posts > 0 and len(posts) >= self.config.max_posts:
                    break

                title = _slug(str(item.get("title", ""))) or self._title_from_url(url)
                published = _published_date_from_rss(
                    str(item.get("pub_date_raw", "")),
                    url,
                )
                tags = ", ".join(categories)

                source_hash = _record_hash(
                    url=url,
                    title=title,
                    tags=tags,
                    published_date=published,
                )
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
                            f"RSS progress: kept={len(posts)} posts "
                            f"after {elapsed:.1f}s, latest_date={published or 'unknown'}"
                        ),
                        extra={"stage": self.stage_name, "run_id": self.run_id},
                    )

            if self.config.max_posts > 0 and len(posts) >= self.config.max_posts:
                break

            time.sleep(self.config.request_delay_seconds)
            page_number += 1

        elapsed = time.perf_counter() - crawl_started
        self.logger.info(
            (
                f"RSS crawl finished: pages={pages_fetched}, "
                f"feed_items={total_feed_items}, maps_posts={len(posts)}, "
                f"duration={elapsed:.1f}s"
            ),
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return posts

    def _with_ids(self, rows: list[dict[str, str]]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(
                columns=[
                    "id",
                    "name",
                    "url",
                    "tags",
                    "published_date",
                    "crawl_ts",
                    "source_hash",
                ]
            )

        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["url"], keep="first").copy()
        df = df.sort_values(
            by=["published_date", "url"], ascending=[False, True]
        ).reset_index(drop=True)
        df.insert(0, "id", df.index.astype(int))
        return df

    def _enrich_with_previous_metadata(
        self, crawled_df: pd.DataFrame, previous_df: pd.DataFrame | None
    ) -> pd.DataFrame:
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

            if (
                not str(row.get("name", "")).strip()
                and str(prev_row.get("name", "")).strip()
            ):
                out.at[idx, "name"] = str(prev_row.get("name", ""))

            if (
                not str(row.get("published_date", "")).strip()
                and str(prev_row.get("published_date", "")).strip()
            ):
                out.at[idx, "published_date"] = str(prev_row.get("published_date", ""))

            # Keep richer tags/source hash from previous run when available.
            if str(prev_row.get("tags", "")).strip():
                out.at[idx, "tags"] = str(prev_row.get("tags", ""))
            if str(prev_row.get("source_hash", "")).strip():
                out.at[idx, "source_hash"] = str(prev_row.get("source_hash", ""))

        return out

    def _merge_with_history(
        self, crawled_df: pd.DataFrame, previous_df: pd.DataFrame | None
    ) -> tuple[pd.DataFrame, int]:
        if (
            previous_df is None
            or previous_df.empty
            or not self.config.carry_forward_missing
        ):
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
                carry_df.at[idx, "published_date"] = _extract_date_from_url(
                    str(row.get("url", ""))
                )
            if not str(row.get("source_hash", "")).strip():
                carry_df.at[idx, "source_hash"] = _record_hash(
                    url=str(row.get("url", "")),
                    title=str(row.get("name", "")),
                    tags=str(row.get("tags", "")),
                    published_date=str(carry_df.at[idx, "published_date"]),
                )

        crawled_no_id = crawled_df.drop(columns=["id"], errors="ignore")
        merged = pd.concat(
            [
                crawled_no_id,
                carry_df[
                    ["name", "url", "tags", "published_date", "crawl_ts", "source_hash"]
                ],
            ],
            ignore_index=True,
        )
        merged = merged.drop_duplicates(subset=["url"], keep="first").copy()
        merged = merged.sort_values(
            by=["published_date", "url"], ascending=[False, True]
        ).reset_index(drop=True)
        merged = merged.drop(columns=["id"], errors="ignore")
        merged.insert(0, "id", merged.index.astype(int))
        return merged, int(len(carry_df))

    def _build_delta(
        self, current_df: pd.DataFrame, previous_df: pd.DataFrame | None
    ) -> tuple[pd.DataFrame, dict[str, int]]:
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
                columns=[
                    "change_type",
                    "url",
                    "name",
                    "published_date",
                    "old_source_hash",
                    "new_source_hash",
                ]
            )

        return delta_df, {
            "new": len(new_urls),
            "updated": len(updated_urls),
            "unchanged": unchanged_count,
        }

    def _load_previous_index(self) -> pd.DataFrame | None:
        def normalize_index(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            for col in (
                "id",
                "name",
                "url",
                "tags",
                "published_date",
                "crawl_ts",
                "source_hash",
            ):
                if col not in out.columns:
                    out[col] = ""
            out = out[
                [
                    "id",
                    "name",
                    "url",
                    "tags",
                    "published_date",
                    "crawl_ts",
                    "source_hash",
                ]
            ].copy()
            out["url"] = out["url"].astype(str).str.strip()
            out = out[out["url"] != ""].copy()
            return out

        if self.config.blog_index_csv.exists():
            return normalize_index(
                pd.read_csv(self.config.blog_index_csv, dtype=str).fillna("")
            )

        return None

    def run_stage(self, force: bool = False) -> dict[str, Any]:
        previous_df = self._load_previous_index()

        session = self._build_session()
        rows = self._crawl_posts(session)
        if not rows:
            raise RuntimeError("No map entries found from the RSS feed crawl.")

        crawled_df = self._with_ids(rows)
        crawled_df = self._enrich_with_previous_metadata(crawled_df, previous_df)
        current_df, carried_forward = self._merge_with_history(crawled_df, previous_df)
        if carried_forward > 0:
            self.logger.info(
                (
                    f"Carry-forward applied: {carried_forward} historical URLs "
                    "not present in current RSS feed snapshot."
                ),
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )

        delta_df, delta_counts = self._build_delta(current_df, previous_df)

        if not self.dry_run:
            current_df.to_csv(
                self.config.blog_index_csv, index=False, encoding="utf-8-sig"
            )
            delta_df.to_csv(
                self.config.blog_delta_csv, index=False, encoding="utf-8-sig"
            )

            state_payload = {
                "stage": self.stage_name,
                "run_id": self.run_id,
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "total_rows": int(len(current_df)),
                "delta": delta_counts,
                "output_files": [
                    str(self.config.blog_index_csv),
                    str(self.config.blog_delta_csv),
                ],
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
