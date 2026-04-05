from __future__ import annotations

import json
import re
import shutil
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BLOG_INDEX_CSV = ROOT / "data" / "raw" / "blog_index.csv"
TARGET_HTML_DIR = ROOT / "data" / "html_cache"
UNMATCHED_HTML_DIR = TARGET_HTML_DIR / "unmatched"
LEGACY_HTML_DIR = ROOT / "dyson_html"
MANIFEST_CSV = ROOT / "data" / "raw" / "blog_index_html.csv"
UNMATCHED_REPORT_CSV = ROOT / "data" / "raw" / "blog_html_unmatched_legacy.csv"
STATE_JSON = ROOT / "data" / "state" / "s01_fetch_html_state.json"

CANONICAL_RE = re.compile(r'<link\s+rel="canonical"\s+href="([^"]+)"', re.IGNORECASE)
SCAN_LOG_EVERY = 50
MIGRATE_LOG_EVERY = 50
FALLBACK_LOG_EVERY = 20


def log_info(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}", flush=True)


def normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    path = parsed.path.rstrip("/")
    if not path:
        path = "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def safe_int(value: str) -> int | None:
    try:
        return int(float(value))
    except Exception:
        return None


def slugify(value: str, max_length: int = 120) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    text = text.strip("-")
    return text[:max_length] or "page"


def html_filename(index: int, row: pd.Series) -> str:
    page_id = safe_int(str(row.get("id", "")))
    prefix = f"{page_id:04d}" if page_id is not None else f"{index + 1:04d}"
    slug = slugify(str(row.get("name", "")))
    return f"{prefix}_{slug}.html"


def filename_slug(path: Path) -> str:
    name = path.name
    m = re.match(r"^\d+_(.+)\.html$", name)
    if m:
        return m.group(1).strip().lower()
    return ""


def extract_canonical_url(html_file: Path) -> str:
    try:
        with html_file.open("rb") as handle:
            raw = handle.read(524_288)
        content = raw.decode("utf-8", errors="ignore")
    except Exception:
        return ""
    match = CANONICAL_RE.search(content)
    if not match:
        return ""
    return normalize_url(match.group(1))


def move_with_unique_name(source_path: Path, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    candidate = target_dir / source_path.name
    if not candidate.exists():
        shutil.move(str(source_path), str(candidate))
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    i = 1
    while True:
        alternate = target_dir / f"{stem}_dup{i}{suffix}"
        if not alternate.exists():
            shutil.move(str(source_path), str(alternate))
            return alternate
        i += 1


def main() -> None:
    started = time.perf_counter()
    if not BLOG_INDEX_CSV.exists():
        raise RuntimeError(f"Missing {BLOG_INDEX_CSV}")
    if not LEGACY_HTML_DIR.exists():
        raise RuntimeError(f"Missing {LEGACY_HTML_DIR}")

    TARGET_HTML_DIR.mkdir(parents=True, exist_ok=True)
    UNMATCHED_HTML_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_CSV.parent.mkdir(parents=True, exist_ok=True)
    UNMATCHED_REPORT_CSV.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(BLOG_INDEX_CSV, dtype=str).fillna("")
    if df.empty:
        raise RuntimeError("blog_index.csv is empty")

    for col in ("id", "name", "url", "tags", "published_date", "crawl_ts", "source_hash"):
        if col not in df.columns:
            df[col] = ""

    all_legacy_files = sorted(LEGACY_HTML_DIR.glob("*.html"))
    total_legacy = len(all_legacy_files)
    total_rows = len(df)

    log_info(
        (
            f"Starting HTML cache migration: rows={total_rows} "
            f"legacy_files={total_legacy}"
        )
    )

    by_slug: dict[str, list[Path]] = {}
    no_slug = 0
    duplicate_slug_files = 0

    for scan_idx, html_file in enumerate(all_legacy_files, start=1):
        slug = filename_slug(html_file)
        if slug:
            bucket = by_slug.setdefault(slug, [])
            bucket.append(html_file)
            if len(bucket) == 2:
                duplicate_slug_files += 1
        else:
            no_slug += 1

        if scan_idx % SCAN_LOG_EVERY == 0 or scan_idx == total_legacy:
            elapsed = max(time.perf_counter() - started, 0.001)
            rate = scan_idx / elapsed
            log_info(
                (
                    f"Indexing legacy HTML: {scan_idx}/{total_legacy} "
                    f"slug_keys={len(by_slug)} no_slug={no_slug} dup_slug_keys={duplicate_slug_files} "
                    f"rate={rate:.1f} files/s"
                )
            )

    moved = 0
    already_in_place = 0
    conflicts = 0
    missing_source = 0
    resolved_by_slug = 0
    ambiguous_slug_matches = 0
    moved_by_canonical_fallback = 0
    ambiguous_canonical_matches = 0

    used_sources: set[Path] = set()
    manifest_rows: list[dict[str, str]] = []
    unresolved_rows: list[tuple[int, pd.Series]] = []

    for row_idx, (idx, row) in enumerate(df.iterrows(), start=1):
        url = str(row.get("url", "")).strip()
        norm_url = normalize_url(url)
        target_name = html_filename(idx, row)
        target_path = TARGET_HTML_DIR / target_name

        source_path = None
        origin = "missing"

        if norm_url:
            slug = urlparse(url).path.rstrip("/").split("/")[-1].strip().lower()
            candidates = [p for p in by_slug.get(slug, []) if p not in used_sources]
            if len(candidates) == 1:
                source_path = candidates[0]
                origin = "slug_direct"
                resolved_by_slug += 1
            elif len(candidates) > 1:
                source_path = sorted(candidates)[0]
                origin = "slug_ambiguous"
                resolved_by_slug += 1
                ambiguous_slug_matches += 1

        if source_path is None:
            if target_path.exists():
                html_exists = True
                html_path = str(target_path.resolve())
                origin = "already_target"
                already_in_place += 1
            else:
                html_exists = False
                html_path = ""
                origin = "missing"
                missing_source += 1
                unresolved_rows.append((len(manifest_rows), row))
        else:
            if target_path.exists():
                html_exists = True
                html_path = str(target_path.resolve())
                if source_path.resolve() == target_path.resolve():
                    already_in_place += 1
                    used_sources.add(source_path)
                    origin = "already_target"
                else:
                    conflicts += 1
                    origin = "conflict_target_exists"
            else:
                source_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source_path), str(target_path))
                used_sources.add(source_path)
                moved += 1
                html_exists = True
                html_path = str(target_path.resolve())

        manifest_rows.append(
            {
                "id": str(row.get("id", "")),
                "name": str(row.get("name", "")),
                "url": url,
                "tags": str(row.get("tags", "")),
                "published_date": str(row.get("published_date", "")),
                "crawl_ts": str(row.get("crawl_ts", "")),
                "source_hash": str(row.get("source_hash", "")),
                "html_file": target_name,
                "html_path": html_path,
                "html_exists": "1" if html_exists else "0",
                "change_type": "",
                "source_origin": origin,
            }
        )

        if row_idx % MIGRATE_LOG_EVERY == 0 or row_idx == total_rows:
            elapsed = max(time.perf_counter() - started, 0.001)
            rate = row_idx / elapsed
            log_info(
                (
                    f"Applying migration: {row_idx}/{total_rows} "
                    f"moved={moved} already={already_in_place} "
                    f"missing={missing_source} conflicts={conflicts} ambiguous={ambiguous_slug_matches} "
                    f"rate={rate:.1f} rows/s"
                )
            )

    remaining_before_fallback = sorted(LEGACY_HTML_DIR.glob("*.html"))
    if unresolved_rows and remaining_before_fallback:
        log_info(
            (
                f"Canonical fallback start: unresolved_rows={len(unresolved_rows)} "
                f"remaining_legacy={len(remaining_before_fallback)}"
            )
        )

        by_canonical_url: dict[str, list[Path]] = {}
        for scan_idx, html_file in enumerate(remaining_before_fallback, start=1):
            canonical = extract_canonical_url(html_file)
            if canonical:
                by_canonical_url.setdefault(canonical, []).append(html_file)

            if scan_idx % FALLBACK_LOG_EVERY == 0 or scan_idx == len(remaining_before_fallback):
                elapsed = max(time.perf_counter() - started, 0.001)
                rate = scan_idx / elapsed
                log_info(
                    (
                        f"Canonical fallback index: {scan_idx}/{len(remaining_before_fallback)} "
                        f"keys={len(by_canonical_url)} rate={rate:.1f} files/s"
                    )
                )

        for idx, (manifest_pos, row) in enumerate(unresolved_rows, start=1):
            url = str(row.get("url", "")).strip()
            norm_url = normalize_url(url)
            if not norm_url:
                continue

            candidates = [p for p in by_canonical_url.get(norm_url, []) if p.exists() and p not in used_sources]
            if not candidates:
                continue

            if len(candidates) > 1:
                ambiguous_canonical_matches += 1
            source_path = sorted(candidates)[0]

            target_name = str(manifest_rows[manifest_pos]["html_file"])
            target_path = TARGET_HTML_DIR / target_name

            if target_path.exists():
                conflicts += 1
                manifest_rows[manifest_pos]["source_origin"] = "conflict_target_exists"
            else:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source_path), str(target_path))
                used_sources.add(source_path)

                moved += 1
                moved_by_canonical_fallback += 1
                missing_source = max(missing_source - 1, 0)

                manifest_rows[manifest_pos]["html_path"] = str(target_path.resolve())
                manifest_rows[manifest_pos]["html_exists"] = "1"
                manifest_rows[manifest_pos]["source_origin"] = "canonical_fallback"

            if idx % FALLBACK_LOG_EVERY == 0 or idx == len(unresolved_rows):
                elapsed = max(time.perf_counter() - started, 0.001)
                rate = idx / elapsed
                log_info(
                    (
                        f"Canonical fallback apply: {idx}/{len(unresolved_rows)} "
                        f"moved_by_canonical={moved_by_canonical_fallback} "
                        f"ambiguous={ambiguous_canonical_matches} rate={rate:.1f} rows/s"
                    )
                )

    unmatched_rows: list[dict[str, str]] = []
    remaining_after_fallback = sorted(LEGACY_HTML_DIR.glob("*.html"))
    if remaining_after_fallback:
        log_info(f"Moving unmatched legacy files: count={len(remaining_after_fallback)}")

    for idx, source_path in enumerate(remaining_after_fallback, start=1):
        destination = move_with_unique_name(source_path, UNMATCHED_HTML_DIR)
        unmatched_rows.append(
            {
                "legacy_file": str(source_path.resolve()),
                "new_file": str(destination.resolve()),
                "slug": filename_slug(destination),
            }
        )

        if idx % SCAN_LOG_EVERY == 0 or idx == len(remaining_after_fallback):
            elapsed = max(time.perf_counter() - started, 0.001)
            rate = idx / elapsed
            log_info(
                (
                    f"Unmatched move progress: {idx}/{len(remaining_after_fallback)} "
                    f"rate={rate:.1f} files/s"
                )
            )

    if unmatched_rows:
        pd.DataFrame(unmatched_rows).to_csv(UNMATCHED_REPORT_CSV, index=False, encoding="utf-8-sig")
    elif not UNMATCHED_REPORT_CSV.exists():
        pd.DataFrame(columns=["legacy_file", "new_file", "slug"]).to_csv(
            UNMATCHED_REPORT_CSV, index=False, encoding="utf-8-sig"
        )

    manifest = pd.DataFrame(manifest_rows)
    log_info(f"Writing manifest: {MANIFEST_CSV}")
    manifest.to_csv(MANIFEST_CSV, index=False, encoding="utf-8-sig")

    remaining_legacy = len(list(LEGACY_HTML_DIR.glob("*.html")))

    state_payload = {
        "stage": "s01_fetch_html",
        "run_id": f"migrate_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "total_rows": int(len(df)),
            "legacy_files_seen": int(len(all_legacy_files)),
            "legacy_without_filename_slug": int(no_slug),
            "legacy_duplicate_slug_keys": int(duplicate_slug_files),
            "moved": int(moved),
            "already_in_place": int(already_in_place),
            "resolved_by_slug": int(resolved_by_slug),
            "ambiguous_slug_matches": int(ambiguous_slug_matches),
            "moved_by_canonical_fallback": int(moved_by_canonical_fallback),
            "ambiguous_canonical_matches": int(ambiguous_canonical_matches),
            "conflicts": int(conflicts),
            "missing_source": int(missing_source),
            "moved_to_unmatched": int(len(unmatched_rows)),
            "remaining_legacy_files": int(remaining_legacy),
        },
        "output_files": [str(MANIFEST_CSV), str(UNMATCHED_REPORT_CSV)],
    }
    STATE_JSON.write_text(json.dumps(state_payload, indent=2, ensure_ascii=True), encoding="utf-8")

    total_elapsed = time.perf_counter() - started
    log_info(f"HTML cache migration completed in {total_elapsed:.1f}s")
    print(json.dumps(state_payload["metrics"], indent=2, ensure_ascii=True), flush=True)


if __name__ == "__main__":
    main()
