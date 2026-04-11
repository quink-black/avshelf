"""ffprobe-based metadata extraction engine for AVShelf."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any


def run_ffprobe(file_path: str, ffprobe_path: str = "ffprobe") -> dict | None:
    """Run ffprobe on a file and return the parsed JSON output.

    Returns None if ffprobe fails or the file is not a valid media file.
    """
    cmd = [
        ffprobe_path,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        "-show_chapters",
        str(file_path),
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def compute_file_hash(file_path: str, algorithm: str = "sha256") -> str:
    """Compute the full content hash of a file."""
    h = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute_fast_hash(file_path: str, sample_size: int = 65536) -> str:
    """Compute a fast hash by sampling the head and tail of a file.

    Combines file size + head bytes + tail bytes into a single SHA256.
    Useful for quick duplicate pre-screening on large files.
    """
    path = Path(file_path)
    size = path.stat().st_size
    h = hashlib.sha256()
    h.update(str(size).encode())

    with open(file_path, "rb") as f:
        head = f.read(sample_size)
        h.update(head)
        if size > sample_size * 2:
            f.seek(-sample_size, 2)
            tail = f.read(sample_size)
            h.update(tail)

    return h.hexdigest()


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_frame_rate(rate_str: str | None) -> float | None:
    """Parse ffprobe frame rate string like '30000/1001' into a float."""
    if not rate_str:
        return None
    if "/" in rate_str:
        parts = rate_str.split("/")
        try:
            num, den = float(parts[0]), float(parts[1])
            return num / den if den != 0 else None
        except (ValueError, IndexError):
            return None
    return _safe_float(rate_str)


def _detect_hdr(stream: dict) -> tuple[bool, str | None]:
    """Detect HDR from a video stream's metadata.

    Returns (has_hdr, hdr_format_name).
    """
    color_transfer = stream.get("color_transfer", "")
    color_primaries = stream.get("color_primaries", "")

    hdr_transfers = {"smpte2084", "arib-std-b67"}
    if color_transfer in hdr_transfers:
        if color_transfer == "smpte2084":
            fmt = "HDR10"
        elif color_transfer == "arib-std-b67":
            fmt = "HLG"
        else:
            fmt = "HDR"

        # Refine: check side_data for Dolby Vision
        for sd in stream.get("side_data_list", []):
            sd_type = sd.get("side_data_type", "")
            if "Dolby Vision" in sd_type:
                fmt = "Dolby Vision"
                break
            if "Mastering display" in sd_type or "Content light level" in sd_type:
                if fmt == "HDR":
                    fmt = "HDR10"

        return True, fmt

    return False, None


def _extract_rotation(stream: dict) -> int | None:
    """Extract rotation angle from stream side_data or tags."""
    for sd in stream.get("side_data_list", []):
        rotation = sd.get("rotation")
        if rotation is not None:
            return int(rotation)
    # Fallback: some files store rotation in tags
    tags = stream.get("tags", {})
    rotate = tags.get("rotate")
    if rotate is not None:
        return _safe_int(rotate)
    return None


def extract_metadata(file_path: str, ffprobe_path: str = "ffprobe") -> dict[str, Any]:
    """Extract metadata from a media file using ffprobe.

    Returns a dict suitable for Database.upsert_media().
    On ffprobe failure, returns a minimal dict with has_error=True.
    """
    path = Path(file_path)
    stat = path.stat()

    base: dict[str, Any] = {
        "file_path": str(path.resolve()),
        "file_name": path.name,
        "file_size": stat.st_size,
        "file_mtime": stat.st_mtime,
    }

    raw = run_ffprobe(file_path, ffprobe_path)
    if raw is None:
        base["has_error"] = 1
        base["error_message"] = "ffprobe failed to parse file"
        return base

    base["raw_metadata"] = json.dumps(raw, ensure_ascii=False)

    fmt = raw.get("format", {})
    streams = raw.get("streams", [])
    chapters = raw.get("chapters", [])

    # -- Format level --
    base["format_name"] = fmt.get("format_name")
    base["format_long_name"] = fmt.get("format_long_name")
    base["duration"] = _safe_float(fmt.get("duration"))
    base["bit_rate"] = _safe_int(fmt.get("bit_rate"))
    base["stream_count"] = len(streams)

    # -- Classify streams --
    video_streams = [s for s in streams if s.get("codec_type") == "video"]
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    subtitle_streams = [s for s in streams if s.get("codec_type") == "subtitle"]

    base["video_track_count"] = len(video_streams)
    base["audio_track_count"] = len(audio_streams)
    base["subtitle_track_count"] = len(subtitle_streams)
    base["chapter_count"] = len(chapters)

    # -- Determine media_type from actual stream content --
    if video_streams:
        # Distinguish still images from video: image formats typically have
        # codec_name in a known set and only 1 frame or very short duration.
        image_codecs = {
            "mjpeg", "png", "bmp", "tiff", "webp", "gif",
            "jpegls", "pam", "pbm", "pgm", "ppm",
            "hevc",  # HEIC uses hevc but in image2 container
        }
        first_v = video_streams[0]
        is_image = (
            base["format_name"] in ("image2", "png_pipe", "bmp_pipe",
                                     "tiff_pipe", "webp_pipe", "gif",
                                     "jpeg_pipe", "svg_pipe")
            or (first_v.get("codec_name") in image_codecs
                and base.get("duration") is not None
                and base["duration"] < 0.1
                and len(video_streams) == 1
                and len(audio_streams) == 0)
        )
        base["media_type"] = "image" if is_image else "video"
    elif audio_streams:
        base["media_type"] = "audio"
    else:
        base["media_type"] = "subtitle" if subtitle_streams else "unknown"

    # -- Primary video stream --
    if video_streams:
        vs = video_streams[0]
        base["video_codec"] = vs.get("codec_name")
        base["video_profile"] = vs.get("profile")
        base["video_level"] = str(vs.get("level")) if vs.get("level") is not None else None
        base["width"] = _safe_int(vs.get("width"))
        base["height"] = _safe_int(vs.get("height"))
        base["sar"] = vs.get("sample_aspect_ratio")
        base["dar"] = vs.get("display_aspect_ratio")
        base["frame_rate"] = _parse_frame_rate(vs.get("avg_frame_rate"))
        base["video_bit_rate"] = _safe_int(vs.get("bit_rate"))
        base["pixel_format"] = vs.get("pix_fmt")
        base["bit_depth"] = _safe_int(vs.get("bits_per_raw_sample"))
        base["color_space"] = vs.get("color_space")
        base["color_range"] = vs.get("color_range")
        base["color_transfer"] = vs.get("color_transfer")
        base["color_primaries"] = vs.get("color_primaries")
        base["field_order"] = vs.get("field_order")
        base["rotation"] = _extract_rotation(vs)

        has_hdr, hdr_fmt = _detect_hdr(vs)
        base["has_hdr"] = 1 if has_hdr else 0
        base["hdr_format"] = hdr_fmt

    # -- Primary audio stream --
    if audio_streams:
        aus = audio_streams[0]
        base["audio_codec"] = aus.get("codec_name")
        base["audio_profile"] = aus.get("profile")
        base["audio_sample_rate"] = _safe_int(aus.get("sample_rate"))
        base["audio_channels"] = _safe_int(aus.get("channels"))
        base["audio_channel_layout"] = aus.get("channel_layout")
        base["audio_bit_rate"] = _safe_int(aus.get("bit_rate"))
        base["audio_bit_depth"] = _safe_int(aus.get("bits_per_raw_sample"))

    # -- Format-level tags --
    fmt_tags = fmt.get("tags")
    if fmt_tags:
        base["tags_json"] = json.dumps(fmt_tags, ensure_ascii=False)

    base["has_error"] = 0
    return base
