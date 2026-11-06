"""Default configuration (DESIGN 3) with optional config.toml overrides. Shared; changes go through the coordinator."""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)


def _home() -> Path:
    return Path(os.environ.get("BUNKRDL_HOME", Path.home() / ".workhub" / "bunkrdl"))


@dataclass
class Config:
    # Endpoints (change often; keep configurable)
    domains: list[str] = field(default_factory=lambda: ["bunkr.cr", "bunkr.si", "bunkr.site", "bunkr.ph", "bunkr.pk", "bunkr.ru"])
    api_url: str = "https://dl.bunkr.cr/api/_001_v2"
    api_referer: str = "https://dl.bunkr.cr/"
    sign_url: str = "https://glb-apisign.cdn.cr/sign"
    user_agent: str = DEFAULT_UA

    link_name_param: bool = True

    # Throttling
    file_delay_min: float = 3.0
    file_delay_max: float = 6.0
    request_interval: float = 1.0
    batch_size: int = 20
    batch_pause: float = 30.0
    max_retries: int = 5          # kept for compatibility; no longer used per file
    quick_retries: int = 2        # fast retries per file before moving on
    quick_backoff_max: float = 60.0
    backoff_base: float = 5.0
    backoff_max: float = 120.0
    consecutive_limit_failures: int = 3
    cooldown: float = 600.0
    timeout: float = 30.0         # legacy value; requests use the two below
    api_timeout: float = 30.0     # page, API and signing requests (metadata and signing)
    read_timeout: float = 30.0    # streamed download reads
    chunk_size: int = 1 << 16

    # Web scheduler (v3); the CLI keeps the serial settings above
    image_concurrency: int = 2    # 1-6
    video_concurrency: int = 1    # 1-3
    image_gap: float = 1.0        # wait after each image, per slot
    video_gap_min: float = 2.0    # wait after each video, per slot
    video_gap_max: float = 4.0
    web_request_interval: float = 0.7  # shared spacing between request starts
    fail_pause_after: int = 3     # consecutive final failures before pausing all
    web_cooldown: float = 120.0   # first step of the IP-wide cooldown after 429/503
    ratelimit_cooldown_max: float = 900.0  # ceiling of the exponential backoff
    ratelimit_pause_after: int = 4  # consecutive rate-limited files before pausing all

    # Paths
    state_db: str = str(_home() / "state.db")
    download_root: str = str(Path.home() / "Downloads" / "bunkr")

    # Web
    web_host: str = "127.0.0.1"
    web_port: int = 8765


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load defaults, then override with keys from a TOML file if given or if <BUNKRDL_HOME>/config.toml exists."""
    cfg = Config()
    p = Path(path) if path else _home() / "config.toml"
    if p.is_file():
        with open(p, "rb") as fh:
            data = tomllib.load(fh)
        known = {f.name for f in fields(Config)}
        for key, value in data.items():
            if key in known:
                setattr(cfg, key, value)
    return cfg
