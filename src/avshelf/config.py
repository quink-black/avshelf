"""Configuration management for AVShelf."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


APP_DIR = Path.home() / ".avshelf"
DEFAULT_DB_PATH = APP_DIR / "avshelf.db"
DEFAULT_CONFIG_PATH = APP_DIR / "config.toml"
TRASH_DIR = APP_DIR / "trash"
LOGS_DIR = APP_DIR / "logs"

# Extension name lists used for initial file filtering.
# Final media type is always determined by ffprobe, not by extension.
DEFAULT_VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".ts",
    ".m2ts", ".mpg", ".mpeg", ".3gp", ".ogv", ".y4m", ".ivf", ".obu",
    ".h264", ".h265", ".hevc", ".vvc", ".av1", ".vp9",
}
DEFAULT_AUDIO_EXTENSIONS = {
    ".mp3", ".aac", ".flac", ".wav", ".ogg", ".opus", ".m4a", ".wma",
    ".aiff", ".ape", ".ac3", ".eac3", ".dts", ".pcm",
}
DEFAULT_SUBTITLE_EXTENSIONS = {
    ".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".sup",
}
DEFAULT_IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp", ".gif",
    ".heic", ".avif", ".svg", ".dpx", ".exr",
}

DEFAULT_EXCLUDE_PATTERNS = [
    ".git", ".svn", ".hg", "__pycache__", "node_modules",
    ".DS_Store", "Thumbs.db", ".avshelf",
]

# Env var prefix for overrides
_ENV_PREFIX = "AVSHELF_"

# Mapping from dotted config keys to env var names
_ENV_OVERRIDES: dict[str, str] = {
    "llm.api_key": "AVSHELF_LLM_API_KEY",
    "llm.provider": "AVSHELF_LLM_PROVIDER",
    "llm.model": "AVSHELF_LLM_MODEL",
    "database.path": "AVSHELF_DB_PATH",
    "scan.ffprobe_path": "AVSHELF_FFPROBE_PATH",
    "scan.ffmpeg_path": "AVSHELF_FFMPEG_PATH",
}


def _deep_get(data: dict, dotted_key: str, default: Any = None) -> Any:
    """Retrieve a nested value from a dict using a dotted key path."""
    keys = dotted_key.split(".")
    current = data
    for k in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(k)
        if current is None:
            return default
    return current


def _deep_set(data: dict, dotted_key: str, value: Any) -> None:
    """Set a nested value in a dict using a dotted key path."""
    keys = dotted_key.split(".")
    current = data
    for k in keys[:-1]:
        if k not in current or not isinstance(current[k], dict):
            current[k] = {}
        current = current[k]
    current[keys[-1]] = value


def _default_config() -> dict:
    """Return the default configuration structure."""
    return {
        "database": {
            "path": str(DEFAULT_DB_PATH),
        },
        "scan": {
            "extensions": {
                "video": sorted(DEFAULT_VIDEO_EXTENSIONS),
                "audio": sorted(DEFAULT_AUDIO_EXTENSIONS),
                "subtitle": sorted(DEFAULT_SUBTITLE_EXTENSIONS),
                "image": sorted(DEFAULT_IMAGE_EXTENSIONS),
            },
            "exclude_patterns": DEFAULT_EXCLUDE_PATTERNS[:],
            "hash_algorithm": "sha256",
            "ffprobe_path": "ffprobe",
            "ffmpeg_path": "ffmpeg",
        },
        "deep_scan": {
            "default_frames": 10,
        },
        "llm": {
            "provider": "",
            "api_key": "",
            "model": "",
        },
    }


def _serialize_toml(data: dict, indent: int = 0) -> str:
    """Minimal TOML serializer for writing config back to disk.

    Only supports the subset of TOML used by our config: tables with
    scalar values and arrays of scalars.
    """
    lines: list[str] = []
    prefix = ""

    def _write_section(d: dict, section_prefix: str) -> None:
        scalars: list[tuple[str, Any]] = []
        tables: list[tuple[str, dict]] = []

        for k, v in d.items():
            if isinstance(v, dict):
                tables.append((k, v))
            else:
                scalars.append((k, v))

        if scalars and section_prefix:
            lines.append(f"[{section_prefix}]")

        for k, v in scalars:
            lines.append(f"{k} = {_format_value(v)}")

        if scalars:
            lines.append("")

        for k, v in tables:
            sub = f"{section_prefix}.{k}" if section_prefix else k
            _write_section(v, sub)

    def _format_value(v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float):
            return str(v)
        if isinstance(v, str):
            return f'"{v}"'
        if isinstance(v, list):
            items = ", ".join(_format_value(i) for i in v)
            return f"[{items}]"
        return f'"{v}"'

    _write_section(data, prefix)
    return "\n".join(lines) + "\n"


class Config:
    """Application configuration backed by a TOML file with env overrides."""

    def __init__(self, config_path: Path | None = None) -> None:
        self._path = config_path or DEFAULT_CONFIG_PATH
        self._data = _default_config()
        self._load()

    def _load(self) -> None:
        """Load config from TOML file if it exists, then apply env overrides."""
        if self._path.exists():
            with open(self._path, "rb") as f:
                file_data = tomllib.load(f)
            self._merge(self._data, file_data)
        self._apply_env_overrides()

    @staticmethod
    def _merge(base: dict, override: dict) -> None:
        """Recursively merge override into base."""
        for k, v in override.items():
            if k in base and isinstance(base[k], dict) and isinstance(v, dict):
                Config._merge(base[k], v)
            else:
                base[k] = v

    def _apply_env_overrides(self) -> None:
        """Override config values from environment variables."""
        for dotted_key, env_var in _ENV_OVERRIDES.items():
            val = os.environ.get(env_var)
            if val is not None:
                _deep_set(self._data, dotted_key, val)

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Get a config value by dotted key (e.g. 'scan.ffprobe_path')."""
        return _deep_get(self._data, dotted_key, default)

    def set(self, dotted_key: str, value: Any) -> None:
        """Set a config value and persist to disk."""
        _deep_set(self._data, dotted_key, value)
        self.save()

    def save(self) -> None:
        """Write current config to the TOML file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(_serialize_toml(self._data), encoding="utf-8")

    def ensure_dirs(self) -> None:
        """Create application directories if they don't exist."""
        APP_DIR.mkdir(parents=True, exist_ok=True)
        TRASH_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return Path(self.get("database.path", str(DEFAULT_DB_PATH)))

    @property
    def ffprobe_path(self) -> str:
        return self.get("scan.ffprobe_path", "ffprobe")

    @property
    def ffmpeg_path(self) -> str:
        return self.get("scan.ffmpeg_path", "ffmpeg")

    @property
    def hash_algorithm(self) -> str:
        return self.get("scan.hash_algorithm", "sha256")

    @property
    def exclude_patterns(self) -> list[str]:
        return self.get("scan.exclude_patterns", DEFAULT_EXCLUDE_PATTERNS)

    @property
    def deep_scan_default_frames(self) -> int:
        return int(self.get("deep_scan.default_frames", 10))

    def all_media_extensions(self) -> set[str]:
        """Return the union of all configured media file extensions."""
        exts: set[str] = set()
        ext_cfg = self.get("scan.extensions", {})
        for category in ("video", "audio", "subtitle", "image"):
            exts.update(ext_cfg.get(category, []))
        return exts

    def extensions_for_type(self, media_type: str) -> set[str]:
        """Return configured extensions for a specific media type."""
        ext_cfg = self.get("scan.extensions", {})
        return set(ext_cfg.get(media_type, []))

    @property
    def data(self) -> dict:
        """Return the full config dict (read-only view)."""
        return self._data
