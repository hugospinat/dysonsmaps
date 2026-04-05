from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import PipelineConfig, PipelineRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Dyson pipeline runner (Phases 1-4): crawl, html cache, image extraction, download, and web previews."
        )
    )
    parser.add_argument("--config", type=Path, default=ROOT / "conf" / "default.json", help="JSON config path")
    parser.add_argument("--full", action="store_true", help="Force full stage execution")
    parser.add_argument("--incremental", action="store_true", help="Run incremental mode (default)")
    parser.add_argument("--max-pages", type=int, help="Override max crawl pages")
    parser.add_argument("--max-posts", type=int, help="Override max crawled posts (0 = unlimited)")
    parser.add_argument("--request-delay", type=float, help="Override delay between requests")
    parser.add_argument("--no-tags", action="store_true", help="Skip tag extraction from each post")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output files")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--stage",
        choices=["s00", "s01", "s02", "s03", "s04"],
        help=(
            "Run only one stage (s00 crawl, s01 html fetch, s02 image extraction, "
            "s03 image download, or s04 preview generation + B/W tagging)"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig.from_json(workspace_root=ROOT, config_path=args.config)

    if args.max_pages is not None:
        config = replace(config, max_pages=args.max_pages)
    if args.max_posts is not None:
        config = replace(config, max_posts=args.max_posts)
    if args.request_delay is not None:
        config = replace(config, request_delay_seconds=args.request_delay)
    if args.no_tags:
        config = replace(config, include_tags=False)

    runner = PipelineRunner(config=config, verbose=args.verbose, dry_run=args.dry_run)
    stage_filter = None
    if args.stage == "s00":
        stage_filter = ["s00_crawl"]
    elif args.stage == "s01":
        stage_filter = ["s01_fetch_html"]
    elif args.stage == "s02":
        stage_filter = ["s02_extract_images"]
    elif args.stage == "s03":
        stage_filter = ["s03_download_images"]
    elif args.stage == "s04":
        stage_filter = ["s04_generate_previews"]

    summary = runner.run(full=args.full, stage_filter=stage_filter)

    print(json.dumps(summary, indent=2, ensure_ascii=True))

    failed = any(item.get("status") != "succeeded" for item in summary.get("stage_results", []))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
