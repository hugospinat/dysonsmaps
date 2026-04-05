from __future__ import annotations

import argparse
import shutil
from collections import defaultdict
from pathlib import Path

import pandas as pd

try:
    from PIL import Image
except Exception as exc:
    raise SystemExit("Pillow is required. Install it with: pip install pillow") from exc

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "raw" / "download_queue.csv"
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "download_queue_web.csv"
DEFAULT_LEGACY_PREVIEW = ROOT / "data" / "outputs" / "maps_preview_legacy"
DEFAULT_PREVIEW = ROOT / "data" / "outputs" / "maps_preview"
BW_TAG = "Black & White"


def safe_folder_name(source_file: str) -> str:
    stem = Path(str(source_file)).stem.strip()
    if not stem:
        stem = "unknown_page"
    for char in '<>:"/\\|?*':
        stem = stem.replace(char, "_")
    return stem[:180]


def split_tags(tags_value: str) -> list[str]:
    if not isinstance(tags_value, str) or not tags_value.strip():
        return []
    return [part.strip() for part in tags_value.split("|") if part.strip()]


def has_bw_tag(tags_value: str) -> bool:
    for tag in split_tags(tags_value):
        lowered = tag.lower()
        if lowered in {"black & white", "black and white", "bw"}:
            return True
    return False


def is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def build_preview_name(file_stem: str, width: int, ext: str) -> str:
    return f"{file_stem}_w{width}.{ext}"


def to_web_path(path: Path, preview_root: Path, preview_prefix: str) -> str:
    rel = path.relative_to(preview_root).as_posix()
    prefix = str(preview_prefix or "").strip()
    if not prefix:
        prefix = "/preview"
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    return f"{prefix.rstrip('/')}/{rel}"


def preview_local_path(preview_path: str, root: Path) -> Path | None:
    normalized = str(preview_path or "").strip().replace("\\", "/")
    if not normalized:
        return None

    if normalized.lower().startswith("http://") or normalized.lower().startswith("https://"):
        return None

    normalized = normalized.lstrip("/")
    if not normalized:
        return None

    parts = [part for part in normalized.split("/") if part]
    rel_candidates: list[str] = []

    root_name = root.name.lower()
    if normalized.lower().startswith(root_name + "/"):
        rel_candidates.append(normalized[len(root_name) + 1 :])

    if len(parts) >= 2:
        rel_candidates.append("/".join(parts[1:]))

    rel_candidates.append(normalized)

    seen: set[str] = set()
    for rel in rel_candidates:
        if not rel or rel in seen:
            continue
        seen.add(rel)
        candidate = root / Path(rel)
        if candidate.exists():
            return candidate

    return None


def write_checkpoint(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(output_path)


def build_legacy_index(legacy_root: Path) -> dict[str, list[Path]]:
    out: dict[str, list[Path]] = defaultdict(list)
    if not legacy_root.exists():
        return out

    for file_path in legacy_root.rglob("*"):
        if file_path.is_file():
            out[file_path.name.lower()].append(file_path)

    for key in list(out.keys()):
        out[key] = sorted(out[key], key=lambda path: (len(path.parts), str(path).lower()))

    return out


def pop_legacy_candidate(index: dict[str, list[Path]], file_name: str) -> Path | None:
    key = file_name.lower()
    candidates = index.get(key, [])
    while candidates:
        candidate = candidates.pop(0)
        if candidate.exists():
            return candidate
    index[key] = []
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "One-shot migration for legacy previews: move files to preview root and sync BW metadata from tags."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input queue CSV")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output queue CSV")
    parser.add_argument("--legacy-root", type=Path, default=DEFAULT_LEGACY_PREVIEW, help="Legacy preview root")
    parser.add_argument("--preview-root", type=Path, default=DEFAULT_PREVIEW, help="Target preview root")
    parser.add_argument("--preview-prefix", type=str, default="/preview", help="URL prefix for preview_path")
    parser.add_argument("--width", type=int, default=640, help="Preview width suffix used in file naming")
    parser.add_argument("--format", choices=["jpg", "webp"], default="jpg", help="Preview format")
    parser.add_argument("--save-every", type=int, default=250, help="Checkpoint frequency")
    parser.add_argument("--resume", dest="resume", action="store_true", help="Resume from --output if it exists")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Always start from --input")
    parser.set_defaults(resume=True)
    args = parser.parse_args()

    queue_to_load = args.output if (args.resume and args.output.exists()) else args.input
    if not queue_to_load.exists():
        raise SystemExit(f"Input CSV not found: {queue_to_load}")

    if args.width <= 0:
        raise SystemExit("--width must be > 0")

    if args.save_every < 1:
        raise SystemExit("--save-every must be >= 1")

    df = pd.read_csv(queue_to_load, dtype=str).fillna("")
    if df.empty:
        raise SystemExit(f"CSV is empty: {queue_to_load}")

    for col in ("is_bw", "bw_score", "preview_path", "preview_width", "preview_height", "preview_size_kb"):
        if col not in df.columns:
            df[col] = ""

    args.preview_root.mkdir(parents=True, exist_ok=True)

    legacy_index = build_legacy_index(args.legacy_root)

    metrics = {
        "rows_total": int(len(df)),
        "image_rows": 0,
        "bw_from_tags": 0,
        "preview_already_target": 0,
        "preview_moved": 0,
        "preview_missing": 0,
        "rows_updated": 0,
    }

    updates_since_checkpoint = 0

    for idx, row in df.iterrows():
        if str(row.get("asset_type", "")).strip().lower() != "image":
            continue

        metrics["image_rows"] += 1

        source_file = str(row.get("source_file", "")).strip()
        file_name = str(row.get("file_name", "")).strip()
        file_stem = str(row.get("file_stem", "")).strip() or Path(file_name).stem
        tags_value = str(row.get("tags", ""))

        if not source_file or not file_name:
            continue

        changed = False

        if has_bw_tag(tags_value):
            if not is_truthy(str(df.at[idx, "is_bw"])):
                df.at[idx, "is_bw"] = "1"
                metrics["bw_from_tags"] += 1
                changed = True
            if not str(df.at[idx, "bw_score"]).strip():
                df.at[idx, "bw_score"] = "tag"
                changed = True

        folder_name = safe_folder_name(source_file)
        preview_name = build_preview_name(file_stem, args.width, args.format)
        target_path = args.preview_root / folder_name / preview_name

        if target_path.exists():
            metrics["preview_already_target"] += 1
        else:
            moved_from: Path | None = None

            from_row_path = preview_local_path(str(row.get("preview_path", "")), args.legacy_root)
            if from_row_path is not None and from_row_path.is_file():
                moved_from = from_row_path
            else:
                moved_from = pop_legacy_candidate(legacy_index, preview_name)
                if moved_from is None:
                    moved_from = pop_legacy_candidate(legacy_index, file_name)

            if moved_from is None:
                metrics["preview_missing"] += 1
            else:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(moved_from), str(target_path))
                metrics["preview_moved"] += 1

        if target_path.exists():
            with Image.open(target_path) as img:
                out_w, out_h = img.size
            size_kb = target_path.stat().st_size / 1024.0

            df.at[idx, "preview_path"] = to_web_path(target_path, args.preview_root, args.preview_prefix)
            df.at[idx, "preview_width"] = str(out_w)
            df.at[idx, "preview_height"] = str(out_h)
            df.at[idx, "preview_size_kb"] = f"{size_kb:.1f}"
            changed = True

        if changed:
            metrics["rows_updated"] += 1
            updates_since_checkpoint += 1

        if updates_since_checkpoint >= args.save_every:
            write_checkpoint(df, args.output)
            updates_since_checkpoint = 0

    write_checkpoint(df, args.output)

    print(f"Loaded CSV: {queue_to_load.resolve()}")
    print(f"Rows total: {metrics['rows_total']}")
    print(f"Image rows: {metrics['image_rows']}")
    print(f"BW synced from tags: {metrics['bw_from_tags']}")
    print(f"Preview already in target: {metrics['preview_already_target']}")
    print(f"Preview moved from legacy: {metrics['preview_moved']}")
    print(f"Preview missing in legacy: {metrics['preview_missing']}")
    print(f"Rows updated: {metrics['rows_updated']}")
    print(f"Legacy root: {args.legacy_root.resolve()}")
    print(f"Preview root: {args.preview_root.resolve()}")
    print(f"Output CSV: {args.output.resolve()}")


if __name__ == "__main__":
    main()
