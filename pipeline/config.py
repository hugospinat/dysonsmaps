from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path


def _as_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off", ""}:
            return False
    return bool(value)


@dataclass(frozen=True)
class PipelineConfig:
    workspace_root: Path
    maps_index_url: str = "https://dysonlogos.blog/maps/"
    raw_dir: Path = Path("data/raw")
    html_cache_dir: Path = Path("data/html_cache")
    state_dir: Path = Path("data/state")
    logs_dir: Path = Path("data/logs")
    user_agent: str = "Mozilla/5.0 (compatible; DysonPipeline/2.0)"
    timeout_seconds: int = 30
    request_delay_seconds: float = 0.6
    max_pages: int = 120
    max_posts: int = 0
    s01_refetch_existing_on_full: bool = False
    s02_max_workers: int = 4
    s03_legacy_downloads_dir: Path = Path("downloads")
    s03_output_downloads_dir: Path = Path("data/outputs/map_assets")
    s03_require_maps_tag: bool = True
    s03_max_assets: int = 0
    s03_delay_seconds: float = 0.5
    s03_timeout_seconds: int = 30
    s04_input_csv: Path = Path("data/raw/download_queue.csv")
    s04_output_csv: Path = Path("data/raw/download_queue_web.csv")
    s04_output_json: Path = Path("data/outputs/download_queue_web.json")
    s04_assets_dir: Path = Path("data/outputs/map_assets")
    s04_preview_dir: Path = Path("data/outputs/maps_preview")
    s04_preview_legacy_dir: Path = Path("data/outputs/maps_preview_legacy")
    s04_preview_url_prefix: str = "/data/maps_preview"
    s04_assets_url_prefix: str = "/data/map_assets"
    s04_preview_width: int = 640
    s04_preview_quality: int = 78
    s04_preview_format: str = "jpg"
    s04_bw_threshold: float = 0.12
    s04_bw_resize_max: int = 600
    s04_max_workers: int = 6
    s04_save_every: int = 200
    s04_progress_log_every: int = 25
    s04_progress_log_seconds: float = 10.0
    s04_resume: bool = True
    include_tags: bool = True
    carry_forward_missing: bool = True

    @property
    def blog_index_csv(self) -> Path:
        return self.workspace_root / self.raw_dir / "blog_index.csv"

    @property
    def blog_delta_csv(self) -> Path:
        return self.workspace_root / self.raw_dir / "blog_index_delta.csv"

    @property
    def blog_index_html_csv(self) -> Path:
        return self.workspace_root / self.raw_dir / "blog_index_html.csv"

    @property
    def html_cache_root(self) -> Path:
        return self.workspace_root / self.html_cache_dir

    @property
    def s00_state_file(self) -> Path:
        return self.workspace_root / self.state_dir / "s00_crawl_state.json"

    @property
    def s01_state_file(self) -> Path:
        return self.workspace_root / self.state_dir / "s01_fetch_html_state.json"

    @property
    def s02_state_file(self) -> Path:
        return self.workspace_root / self.state_dir / "s02_extract_images_state.json"

    @property
    def image_inventory_json(self) -> Path:
        return self.workspace_root / self.raw_dir / "image_inventory.json"

    @property
    def image_inventory_csv(self) -> Path:
        return self.workspace_root / self.raw_dir / "image_inventory.csv"

    @property
    def download_queue_csv(self) -> Path:
        return self.workspace_root / self.raw_dir / "download_queue.csv"

    @property
    def download_summary_csv(self) -> Path:
        return self.workspace_root / self.raw_dir / "download_summary.csv"

    @property
    def downloads_legacy_root(self) -> Path:
        return self.workspace_root / self.s03_legacy_downloads_dir

    @property
    def downloads_output_root(self) -> Path:
        return self.workspace_root / self.s03_output_downloads_dir

    @property
    def s03_state_file(self) -> Path:
        return self.workspace_root / self.state_dir / "s03_download_images_state.json"

    @property
    def s04_input_queue_csv(self) -> Path:
        return self.workspace_root / self.s04_input_csv

    @property
    def s04_output_queue_csv(self) -> Path:
        return self.workspace_root / self.s04_output_csv

    @property
    def s04_output_queue_json(self) -> Path:
        return self.workspace_root / self.s04_output_json

    @property
    def s04_assets_root(self) -> Path:
        return self.workspace_root / self.s04_assets_dir

    @property
    def s04_preview_root(self) -> Path:
        return self.workspace_root / self.s04_preview_dir

    @property
    def s04_preview_legacy_root(self) -> Path:
        return self.workspace_root / self.s04_preview_legacy_dir

    @property
    def s04_state_file(self) -> Path:
        return self.workspace_root / self.state_dir / "s04_generate_previews_state.json"

    @property
    def legacy_seed_csv(self) -> Path:
        return self.workspace_root / "Dyson_Logos_Map_Catalogue - Dyson_Logos_Map_Catalogue.csv"

    def ensure_dirs(self) -> None:
        (self.workspace_root / self.raw_dir).mkdir(parents=True, exist_ok=True)
        (self.workspace_root / self.html_cache_dir).mkdir(parents=True, exist_ok=True)
        (self.workspace_root / self.state_dir).mkdir(parents=True, exist_ok=True)
        (self.workspace_root / self.logs_dir).mkdir(parents=True, exist_ok=True)
        self.downloads_output_root.mkdir(parents=True, exist_ok=True)
        self.s04_preview_root.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_json(cls, workspace_root: Path, config_path: Path | None = None) -> "PipelineConfig":
        base = cls(workspace_root=workspace_root)
        if config_path is None:
            return base
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        payload = json.loads(config_path.read_text(encoding="utf-8"))
        remap = {
            "maps_index_url": payload.get("maps_index_url", base.maps_index_url),
            "raw_dir": Path(payload.get("raw_dir", str(base.raw_dir))),
            "html_cache_dir": Path(payload.get("html_cache_dir", str(base.html_cache_dir))),
            "state_dir": Path(payload.get("state_dir", str(base.state_dir))),
            "logs_dir": Path(payload.get("logs_dir", str(base.logs_dir))),
            "user_agent": payload.get("user_agent", base.user_agent),
            "timeout_seconds": int(payload.get("timeout_seconds", base.timeout_seconds)),
            "request_delay_seconds": float(payload.get("request_delay_seconds", base.request_delay_seconds)),
            "max_pages": int(payload.get("max_pages", base.max_pages)),
            "max_posts": int(payload.get("max_posts", base.max_posts)),
            "s01_refetch_existing_on_full": _as_bool(
                payload.get("s01_refetch_existing_on_full", base.s01_refetch_existing_on_full),
                base.s01_refetch_existing_on_full,
            ),
            "s02_max_workers": int(payload.get("s02_max_workers", base.s02_max_workers)),
            "s03_legacy_downloads_dir": Path(
                payload.get("s03_legacy_downloads_dir", str(base.s03_legacy_downloads_dir))
            ),
            "s03_output_downloads_dir": Path(
                payload.get("s03_output_downloads_dir", str(base.s03_output_downloads_dir))
            ),
            "s03_require_maps_tag": _as_bool(
                payload.get("s03_require_maps_tag", base.s03_require_maps_tag),
                base.s03_require_maps_tag,
            ),
            "s03_max_assets": int(payload.get("s03_max_assets", base.s03_max_assets)),
            "s03_delay_seconds": float(payload.get("s03_delay_seconds", base.s03_delay_seconds)),
            "s03_timeout_seconds": int(payload.get("s03_timeout_seconds", base.s03_timeout_seconds)),
            "s04_input_csv": Path(payload.get("s04_input_csv", str(base.s04_input_csv))),
            "s04_output_csv": Path(payload.get("s04_output_csv", str(base.s04_output_csv))),
            "s04_output_json": Path(payload.get("s04_output_json", str(base.s04_output_json))),
            "s04_assets_dir": Path(payload.get("s04_assets_dir", str(base.s04_assets_dir))),
            "s04_preview_dir": Path(payload.get("s04_preview_dir", str(base.s04_preview_dir))),
            "s04_preview_legacy_dir": Path(
                payload.get("s04_preview_legacy_dir", str(base.s04_preview_legacy_dir))
            ),
            "s04_preview_url_prefix": str(payload.get("s04_preview_url_prefix", base.s04_preview_url_prefix)),
            "s04_assets_url_prefix": str(payload.get("s04_assets_url_prefix", base.s04_assets_url_prefix)),
            "s04_preview_width": int(payload.get("s04_preview_width", base.s04_preview_width)),
            "s04_preview_quality": int(payload.get("s04_preview_quality", base.s04_preview_quality)),
            "s04_preview_format": str(payload.get("s04_preview_format", base.s04_preview_format)),
            "s04_bw_threshold": float(payload.get("s04_bw_threshold", base.s04_bw_threshold)),
            "s04_bw_resize_max": int(payload.get("s04_bw_resize_max", base.s04_bw_resize_max)),
            "s04_max_workers": int(payload.get("s04_max_workers", base.s04_max_workers)),
            "s04_save_every": int(payload.get("s04_save_every", base.s04_save_every)),
            "s04_progress_log_every": int(payload.get("s04_progress_log_every", base.s04_progress_log_every)),
            "s04_progress_log_seconds": float(
                payload.get("s04_progress_log_seconds", base.s04_progress_log_seconds)
            ),
            "s04_resume": _as_bool(payload.get("s04_resume", base.s04_resume), base.s04_resume),
            "include_tags": _as_bool(payload.get("include_tags", base.include_tags), base.include_tags),
            "carry_forward_missing": _as_bool(
                payload.get("carry_forward_missing", base.carry_forward_missing),
                base.carry_forward_missing,
            ),
        }
        return replace(base, **remap)
