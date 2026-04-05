from __future__ import annotations

import json
import re
import time
import unicodedata
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import pandas as pd

try:
    from PIL import Image, ImageChops, ImageStat
except Exception:
    Image = None
    ImageChops = None
    ImageStat = None

from .stage_base import StageBase

if TYPE_CHECKING:
    from PIL.Image import Image as PILImage
else:
    PILImage = Any

if Image is not None:
    Image.MAX_IMAGE_PIXELS = None
    warnings.simplefilter("ignore", Image.DecompressionBombWarning)

BW_TAG = "Black & White"

_IRREGULAR_SINGULAR: dict[str, str] = {
    "cities": "city",
    "caverns": "cavern",
    "caves": "cave",
    "crypts": "crypt",
    "tombs": "tomb",
    "towns": "town",
    "maps": "map",
}

_SINGLE_WORD_ACRONYMS: dict[str, str] = {
    "osr": "OSR",
    "ose": "OSE",
    "rpg": "RPG",
    "ttrpg": "TTRPG",
    "dcc": "DCC",
    "5rd": "5RD",
}

_DND_KEYS = {
    "d and d",
    "dnd",
    "dd",
    "dungeons dragons",
    "dungeons and dragons",
}

_BW_KEYS = {"black and white", "black white", "bw"}


def safe_folder_name(source_file: str) -> str:
    stem = Path(str(source_file)).stem.strip()
    if not stem:
        stem = "unknown_page"
    stem = re.sub(r'[<>:"/\\|?*]+', "_", stem)
    return stem[:180]


def split_tags(tags_value: str) -> list[str]:
    if not isinstance(tags_value, str) or not tags_value.strip():
        return []

    parts = [part.strip() for part in tags_value.split("|")]
    return [part for part in parts if part]


def _normalize_tag_key(raw: str) -> str:
    text = unicodedata.normalize("NFKD", str(raw or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[‘’'`\"]", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _singularize_single_word(token: str) -> str:
    value = token.strip().lower()
    if not value:
        return value
    if value in _SINGLE_WORD_ACRONYMS:
        return value
    if value in _IRREGULAR_SINGULAR:
        return _IRREGULAR_SINGULAR[value]
    if len(value) <= 3:
        return value
    if value.endswith("ies") and len(value) > 4:
        return f"{value[:-3]}y"
    if value.endswith("es") and len(value) > 4:
        stem = value[:-2]
        if re.search(r"(s|x|z|ch|sh)$", stem):
            return stem
    if value.endswith("s") and not value.endswith("ss") and not value.endswith("us") and not value.endswith("is"):
        return value[:-1]
    return value


def _single_word_display(token: str) -> str:
    if token in _SINGLE_WORD_ACRONYMS:
        return _SINGLE_WORD_ACRONYMS[token]
    return token[:1].upper() + token[1:]


def normalize_tags_value(tags_value: str) -> str:
    tags = split_tags(tags_value)
    if not tags:
        return ""

    ordered_keys: list[str] = []
    labels_by_key: dict[str, str] = {}

    for raw_tag in tags:
        key = _normalize_tag_key(raw_tag)
        if not key:
            continue

        if " " not in key:
            key = _singularize_single_word(key)

        if key in _DND_KEYS:
            key = "dungeons and dragons"
            label = "Dungeons & Dragons"
        elif key in _BW_KEYS:
            key = "black and white"
            label = BW_TAG
        elif " " not in key:
            label = _single_word_display(key)
        else:
            label = raw_tag.strip()

        if key not in labels_by_key:
            labels_by_key[key] = label
            ordered_keys.append(key)
            continue

        # Prefer richer cased labels over all-lowercase duplicates.
        existing = labels_by_key[key]
        if existing.islower() and any(ch.isupper() for ch in label):
            labels_by_key[key] = label

    return " | ".join(labels_by_key[key] for key in ordered_keys)


def has_bw_tag(tags_value: str) -> bool:
    for tag in split_tags(tags_value):
        lowered = tag.strip().lower()
        if lowered in {"black & white", "black and white", "bw"}:
            return True
    return False


def merge_tag(tags_value: str, tag_to_add: str) -> str:
    tags = split_tags(tags_value)
    lowered = {tag.lower() for tag in tags}
    if tag_to_add.lower() not in lowered:
        tags.append(tag_to_add)
    return " | ".join(tags)


def remove_tag(tags_value: str, tag_to_remove: str) -> str:
    tags = split_tags(tags_value)
    kept = [tag for tag in tags if tag.lower() != tag_to_remove.lower()]
    return " | ".join(kept)


def is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def row_has_bw(row: pd.Series) -> bool:
    is_bw = str(row.get("is_bw", "")).strip()
    score = str(row.get("bw_score", "")).strip()
    return is_bw in {"0", "1"} and score != ""


def build_preview_name(file_stem: str, width: int, ext: str) -> str:
    return f"{file_stem}_w{width}.{ext}"


def preview_local_path(preview_path: str, preview_root: Path) -> Path | None:
    normalized = str(preview_path or "").strip().replace("\\", "/")
    if not normalized:
        return None

    lower = normalized.lower()
    if lower.startswith("http://") or lower.startswith("https://"):
        normalized = urlparse(normalized).path

    normalized = normalized.lstrip("/")
    if not normalized:
        return None

    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return None

    root_name = preview_root.name.lower()
    rel_candidates: list[str] = []

    for marker in (root_name, "preview"):
        for idx, part in enumerate(parts):
            if part.lower() == marker and idx + 1 < len(parts):
                rel_candidates.append("/".join(parts[idx + 1 :]))

    rel_candidates.append(normalized)

    first_candidate: Path | None = None
    seen: set[str] = set()
    for rel in rel_candidates:
        if not rel or rel in seen:
            continue
        seen.add(rel)
        candidate = preview_root / Path(rel)
        if first_candidate is None:
            first_candidate = candidate
        if candidate.exists():
            return candidate

    return first_candidate


def row_has_preview(row: pd.Series, preview_root: Path) -> bool:
    preview_path = str(row.get("preview_path", "")).strip()
    local_path = preview_local_path(preview_path, preview_root)
    if local_path is None:
        return False
    return local_path.exists()


def build_asset_name_index(assets_root: Path) -> dict[str, list[Path]]:
    index: dict[str, list[Path]] = {}
    for p in assets_root.rglob("*"):
        if not p.is_file():
            continue
        key = p.name.lower()
        index.setdefault(key, []).append(p)
    return index


def resolve_source_path(
    assets_root: Path,
    source_file: str,
    file_name: str,
    asset_name_index: dict[str, list[Path]] | None,
) -> tuple[Path, bool, bool]:
    direct = assets_root / safe_folder_name(source_file) / file_name
    if direct.exists():
        return direct, False, False

    if not asset_name_index:
        return direct, False, False

    candidates = asset_name_index.get(file_name.lower(), [])
    if len(candidates) == 1:
        return candidates[0], True, False
    if len(candidates) > 1:
        return direct, False, True

    return direct, False, False


def detect_bw_score_from_rgb(rgb: PILImage, resize_max: int) -> float:
    sample = rgb.copy()
    sample.thumbnail((resize_max, resize_max), Image.Resampling.LANCZOS)

    hsv = sample.convert("HSV")
    sat_channel = hsv.getchannel("S")
    sat_mean = float(ImageStat.Stat(sat_channel).mean[0]) / 255.0

    r, g, b = sample.split()
    rg_diff = ImageStat.Stat(ImageChops.difference(r, g)).mean[0] / 255.0
    rb_diff = ImageStat.Stat(ImageChops.difference(r, b)).mean[0] / 255.0
    gb_diff = ImageStat.Stat(ImageChops.difference(g, b)).mean[0] / 255.0
    chroma_proxy = (rg_diff + rb_diff + gb_diff) / 3.0

    return max(sat_mean, chroma_proxy)


def generate_preview_from_rgb(
    rgb: PILImage,
    dst_path: Path,
    width: int,
    quality: int,
    fmt: str,
    force: bool,
) -> tuple[int, int, float, bool]:
    if dst_path.exists() and not force:
        with Image.open(dst_path) as existing:
            out_w, out_h = existing.size
        size_kb = dst_path.stat().st_size / 1024.0
        return out_w, out_h, size_kb, False

    dst_path.parent.mkdir(parents=True, exist_ok=True)

    ow, oh = rgb.size
    if ow <= width:
        resized = rgb
    else:
        ratio = width / float(ow)
        new_height = max(1, int(round(oh * ratio)))
        resized = rgb.resize((width, new_height), Image.Resampling.LANCZOS)

    save_kwargs = {"optimize": True}
    if fmt in {"jpg", "jpeg", "webp"}:
        save_kwargs["quality"] = quality

    pil_fmt = "JPEG" if fmt in {"jpg", "jpeg"} else fmt.upper()
    resized.save(dst_path, pil_fmt, **save_kwargs)

    with Image.open(dst_path) as generated:
        out_w, out_h = generated.size
    size_kb = dst_path.stat().st_size / 1024.0
    return out_w, out_h, size_kb, True


def to_web_path(path: Path, preview_root: Path, url_prefix: str) -> str:
    rel = path.relative_to(preview_root).as_posix()
    prefix = str(url_prefix or "").strip()
    if not prefix:
        prefix = f"/{preview_root.name}"
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    prefix = prefix.rstrip("/")
    return f"{prefix}/{rel}"


def write_output_checkpoint(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(output_path)


def process_image_job(
    idx: int,
    src_path: Path,
    source_file: str,
    file_stem: str,
    tags_value: str,
    need_bw: bool,
    need_preview: bool,
    threshold: float,
    resize_max: int,
    preview_root: Path,
    preview_width: int,
    preview_quality: int,
    preview_format: str,
    force_preview: bool,
    preview_url_prefix: str,
    dry_run: bool,
) -> dict[str, str | bool | int | float]:
    result: dict[str, str | bool | int | float] = {
        "idx": idx,
        "need_bw": need_bw,
        "need_preview": need_preview,
    }

    with Image.open(src_path) as img:
        rgb = img.convert("RGB")

        if need_bw:
            score = detect_bw_score_from_rgb(rgb, resize_max)
            is_bw = int(score <= threshold)
            updated_tags = merge_tag(tags_value, BW_TAG) if is_bw == 1 else remove_tag(tags_value, BW_TAG)

            result["bw_score"] = f"{score:.4f}"
            result["is_bw"] = str(is_bw)
            result["tags"] = updated_tags
            result["bw_hit"] = is_bw == 1

        if need_preview:
            folder_name = safe_folder_name(source_file)
            dst_folder = preview_root / folder_name
            preview_name = build_preview_name(file_stem, preview_width, preview_format)
            dst_path = dst_folder / preview_name

            if dry_run:
                if dst_path.exists():
                    with Image.open(dst_path) as existing:
                        out_w, out_h = existing.size
                    size_kb = dst_path.stat().st_size / 1024.0
                else:
                    ow, oh = rgb.size
                    if ow <= preview_width:
                        out_w, out_h = ow, oh
                    else:
                        ratio = preview_width / float(ow)
                        out_w = preview_width
                        out_h = max(1, int(round(oh * ratio)))
                    size_kb = 0.0
                generated = False
            else:
                out_w, out_h, size_kb, generated = generate_preview_from_rgb(
                    rgb=rgb,
                    dst_path=dst_path,
                    width=preview_width,
                    quality=preview_quality,
                    fmt=preview_format,
                    force=force_preview,
                )

            result["preview_path"] = to_web_path(dst_path, preview_root, preview_url_prefix)
            result["preview_width"] = str(out_w)
            result["preview_height"] = str(out_h)
            result["preview_size_kb"] = f"{size_kb:.1f}"
            result["preview_generated"] = generated

    return result


class GeneratePreviewsStage(StageBase):
    stage_name = "s04_generate_previews"
    progress_log_every = 100

    def output_paths(self) -> list[str]:
        return [
            str(self.config.s04_output_queue_csv),
            str(self.config.s04_output_queue_json),
            str(self.config.s04_preview_root),
            str(self.config.s04_state_file),
        ]

    def _build_tag_index(self, records: list[dict[str, Any]]) -> dict[str, list[int]]:
        tag_index: dict[str, list[int]] = {}
        for idx, row in enumerate(records):
            if str(row.get("asset_type", "")).strip().lower() != "image":
                continue
            raw_tags = row.get("tags", [])
            if isinstance(raw_tags, list):
                tags = [str(tag).strip() for tag in raw_tags if str(tag).strip()]
            else:
                tags = split_tags(str(raw_tags or ""))

            for tag in tags:
                tag_index.setdefault(tag, []).append(idx)
        return tag_index

    def _write_json_bundle(self, df: pd.DataFrame, output_path: Path) -> None:
        if self.dry_run:
            return

        records: list[dict[str, Any]] = df.to_dict(orient="records")
        for row in records:
            row["tags"] = split_tags(str(row.get("tags", "") or ""))

        tag_index = self._build_tag_index(records)
        tag_counts = {tag: len(indices) for tag, indices in tag_index.items()}

        payload = {
            "version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_rows": int(len(records)),
            "total_images": int(sum(1 for row in records if str(row.get("asset_type", "")).strip().lower() == "image")),
            "items": records,
            "tag_index": tag_index,
            "tag_counts": tag_counts,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(output_path)

    def _resolve_input_csv(self, force: bool) -> Path:
        if self.config.s04_resume and not force and self.config.s04_output_queue_csv.exists():
            return self.config.s04_output_queue_csv
        if self.config.download_queue_csv.exists():
            return self.config.download_queue_csv
        if self.config.s04_input_queue_csv.exists():
            return self.config.s04_input_queue_csv
        raise RuntimeError(
            f"No input queue CSV found. Checked: {self.config.download_queue_csv} and {self.config.s04_input_queue_csv}"
        )

    def _write_progress_state(self, metrics: dict[str, int], processed_rows: int, input_csv: Path, output_csv: Path) -> None:
        if self.dry_run:
            return
        payload = {
            "stage": self.stage_name,
            "run_id": self.run_id,
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "processed_rows": int(processed_rows),
            "input_csv": str(input_csv),
            "output_csv": str(output_csv),
            "metrics": metrics,
            "output_files": [
                str(self.config.s04_output_queue_csv),
                str(self.config.s04_output_queue_json),
                str(self.config.s04_preview_root),
            ],
        }
        self._write_json(self.config.s04_state_file, payload)

    def run_stage(self, force: bool = False) -> dict[str, int]:
        if Image is None:
            raise RuntimeError("Pillow is required for s04. Install with: pip install pillow")

        input_csv = self._resolve_input_csv(force=force)
        output_csv = self.config.s04_output_queue_csv
        output_json = self.config.s04_output_queue_json
        preview_root = self.config.s04_preview_root
        assets_root = self.config.s04_assets_root

        if not input_csv.exists():
            raise RuntimeError(f"Missing input CSV: {input_csv}")

        if not assets_root.exists():
            raise RuntimeError(f"Missing assets root: {assets_root}")

        df = pd.read_csv(input_csv, dtype=str).fillna("")
        if df.empty:
            raise RuntimeError(f"Input CSV is empty: {input_csv}")

        for col in (
            "is_bw",
            "bw_score",
            "preview_path",
            "preview_width",
            "preview_height",
            "preview_size_kb",
            "original_path",
        ):
            if col not in df.columns:
                df[col] = ""

        preview_root.mkdir(parents=True, exist_ok=True)

        jobs: list[tuple[int, Path, str, str, str, bool, bool]] = []

        metrics: dict[str, int] = {
            "total_rows": int(len(df)),
            "image_rows": 0,
            "queued_jobs": 0,
            "skipped_complete": 0,
            "processed_rows": 0,
            "failed_rows": 0,
            "missing_originals": 0,
            "resolved_by_filename": 0,
            "filename_collisions": 0,
            "bw_detected": 0,
            "bw_from_tags": 0,
            "previews_generated": 0,
            "previews_reused": 0,
        }

        asset_name_index: dict[str, list[Path]] | None = None

        for idx, row in df.iterrows():
            if str(row.get("asset_type", "")).strip().lower() != "image":
                continue

            metrics["image_rows"] += 1

            source_file = str(row.get("source_file", "")).strip()
            file_name = str(row.get("file_name", "")).strip()
            if not source_file or not file_name:
                continue

            file_stem = str(row.get("file_stem", "")).strip() or Path(file_name).stem

            if asset_name_index is None and self.config.s04_resume:
                asset_name_index = build_asset_name_index(assets_root)

            src_path, resolved_by_filename, has_collision = resolve_source_path(
                assets_root=assets_root,
                source_file=source_file,
                file_name=file_name,
                asset_name_index=asset_name_index,
            )
            if resolved_by_filename:
                metrics["resolved_by_filename"] += 1
            if has_collision:
                metrics["filename_collisions"] += 1
            if not src_path.exists():
                metrics["missing_originals"] += 1
                continue

            df.at[idx, "original_path"] = to_web_path(src_path, assets_root, self.config.s04_assets_url_prefix)

            tags_value = str(row.get("tags", ""))
            normalized_tags = normalize_tags_value(tags_value)
            if normalized_tags != tags_value:
                df.at[idx, "tags"] = normalized_tags
                tags_value = normalized_tags

            if has_bw_tag(tags_value):
                if not is_truthy(str(df.at[idx, "is_bw"])):
                    df.at[idx, "is_bw"] = "1"
                    metrics["bw_from_tags"] += 1
                if not str(df.at[idx, "bw_score"]).strip():
                    df.at[idx, "bw_score"] = "tag"

            if force:
                need_bw = True
                need_preview = True
            else:
                has_tag_bw = has_bw_tag(str(df.at[idx, "tags"])) and is_truthy(str(df.at[idx, "is_bw"]))
                need_bw = not row_has_bw(df.loc[idx]) and not has_tag_bw
                need_preview = not row_has_preview(df.loc[idx], preview_root)

            if not need_bw and not need_preview:
                metrics["skipped_complete"] += 1
                continue

            jobs.append((idx, src_path, source_file, file_stem, str(df.at[idx, "tags"]), need_bw, need_preview))

        metrics["queued_jobs"] = int(len(jobs))

        if not jobs:
            self.logger.info(
                (
                    f"S04 no queued jobs: image_rows={metrics['image_rows']} "
                    f"missing_originals={metrics['missing_originals']} assets_root={assets_root}"
                ),
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )

        started = time.perf_counter()
        if jobs:
            total_jobs = int(len(jobs))
            max_workers = max(1, min(int(self.config.s04_max_workers), len(jobs)))
            progress_every = max(1, int(getattr(self.config, "s04_progress_log_every", self.progress_log_every)))
            progress_seconds = max(0.0, float(getattr(self.config, "s04_progress_log_seconds", 0.0)))
            last_progress_elapsed = 0.0

            self.logger.info(
                (
                    f"S04 started: queued={total_jobs} workers={max_workers} "
                    f"log_every={progress_every} log_seconds={progress_seconds:.1f}"
                ),
                extra={"stage": self.stage_name, "run_id": self.run_id},
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        process_image_job,
                        idx,
                        src_path,
                        source_file,
                        file_stem,
                        tags_value,
                        need_bw,
                        need_preview,
                        self.config.s04_bw_threshold,
                        self.config.s04_bw_resize_max,
                        preview_root,
                        self.config.s04_preview_width,
                        self.config.s04_preview_quality,
                        self.config.s04_preview_format,
                        force,
                        self.config.s04_preview_url_prefix,
                        self.dry_run,
                    )
                    for idx, src_path, source_file, file_stem, tags_value, need_bw, need_preview in jobs
                ]

                for done, future in enumerate(as_completed(futures), start=1):
                    try:
                        result = future.result()
                    except Exception as exc:
                        metrics["failed_rows"] += 1
                        self.logger.warning(
                            f"s04 job failed: {exc}",
                            extra={"stage": self.stage_name, "run_id": self.run_id},
                        )
                        continue

                    idx = int(result["idx"])
                    if "bw_score" in result:
                        df.at[idx, "bw_score"] = str(result["bw_score"])
                    if "is_bw" in result:
                        df.at[idx, "is_bw"] = str(result["is_bw"])
                    if "tags" in result:
                        df.at[idx, "tags"] = str(result["tags"])
                    if "preview_path" in result:
                        df.at[idx, "preview_path"] = str(result["preview_path"])
                    if "preview_width" in result:
                        df.at[idx, "preview_width"] = str(result["preview_width"])
                    if "preview_height" in result:
                        df.at[idx, "preview_height"] = str(result["preview_height"])
                    if "preview_size_kb" in result:
                        df.at[idx, "preview_size_kb"] = str(result["preview_size_kb"])

                    metrics["processed_rows"] += 1

                    if bool(result.get("bw_hit", False)):
                        metrics["bw_detected"] += 1

                    if bool(result.get("need_preview", False)) and not self.dry_run:
                        if bool(result.get("preview_generated", False)):
                            metrics["previews_generated"] += 1
                        else:
                            metrics["previews_reused"] += 1

                    elapsed = time.perf_counter() - started
                    should_log = done == total_jobs or (done % progress_every == 0)
                    if not should_log and progress_seconds > 0:
                        should_log = (elapsed - last_progress_elapsed) >= progress_seconds

                    if should_log:
                        pct = (done / total_jobs) * 100.0 if total_jobs else 100.0
                        rate = (done / elapsed) if elapsed > 0 else 0.0
                        remaining = max(0, total_jobs - done)
                        eta = (remaining / rate) if rate > 0 else 0.0
                        self.logger.info(
                            (
                                f"S04 progress: {done}/{total_jobs} ({pct:.1f}%) "
                                f"processed={metrics['processed_rows']} failed={metrics['failed_rows']} "
                                f"bw_detected={metrics['bw_detected']} generated={metrics['previews_generated']} "
                                f"reused={metrics['previews_reused']} elapsed={elapsed:.1f}s "
                                f"rate={rate:.2f}/s eta={eta:.1f}s"
                            ),
                            extra={"stage": self.stage_name, "run_id": self.run_id},
                        )
                        last_progress_elapsed = elapsed

                    if (
                        not self.dry_run
                        and metrics["processed_rows"] > 0
                        and metrics["processed_rows"] % int(self.config.s04_save_every) == 0
                    ):
                        write_output_checkpoint(df, output_csv)
                        self._write_progress_state(
                            metrics=metrics,
                            processed_rows=metrics["processed_rows"],
                            input_csv=input_csv,
                            output_csv=output_csv,
                        )

        if not self.dry_run:
            write_output_checkpoint(df, output_csv)
            self._write_json_bundle(df, output_json)
            payload = {
                "stage": self.stage_name,
                "run_id": self.run_id,
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "status": "completed",
                "input_csv": str(input_csv),
                "output_csv": str(output_csv),
                "output_json": str(output_json),
                "metrics": metrics,
                "output_files": [
                    str(self.config.s04_output_queue_csv),
                    str(self.config.s04_output_queue_json),
                    str(self.config.s04_preview_root),
                ],
            }
            self._write_json(self.config.s04_state_file, payload)

        self.logger.info(
            (
                f"s04_generate_previews stats: image_rows={metrics['image_rows']} "
                f"queued={metrics['queued_jobs']} skipped={metrics['skipped_complete']} "
                f"processed={metrics['processed_rows']} failed={metrics['failed_rows']} "
                f"missing_originals={metrics['missing_originals']} resolved_by_filename={metrics['resolved_by_filename']} "
                f"filename_collisions={metrics['filename_collisions']} "
                f"bw_detected={metrics['bw_detected']} "
                f"bw_from_tags={metrics['bw_from_tags']} "
                f"generated={metrics['previews_generated']} "
                f"reused={metrics['previews_reused']}"
            ),
            extra={"stage": self.stage_name, "run_id": self.run_id},
        )

        return metrics
