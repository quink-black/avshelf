"""MCP Server — expose AVShelf search capabilities to AI assistants."""

from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from avshelf.config import Config
from avshelf.database import Database

mcp = FastMCP(
    "avshelf",
    instructions="Media asset management — search, analyze, and query media files indexed by ffprobe.",
)


def _get_db() -> Database:
    cfg = Config()
    cfg.ensure_dirs()
    db = Database(cfg.db_path)
    db.connect()
    return db


def _format_media_summary(record: dict) -> dict:
    """Create a concise summary of a media record for MCP responses."""
    summary: dict[str, Any] = {
        "file_path": record.get("file_path"),
        "file_name": record.get("file_name"),
        "media_type": record.get("media_type"),
        "file_size": record.get("file_size"),
    }
    if record.get("duration"):
        summary["duration"] = record["duration"]
    if record.get("video_codec"):
        summary["video_codec"] = record["video_codec"]
    if record.get("width") and record.get("height"):
        summary["resolution"] = f"{record['width']}x{record['height']}"
    if record.get("pixel_format"):
        summary["pixel_format"] = record["pixel_format"]
    if record.get("has_hdr"):
        summary["hdr"] = record.get("hdr_format", "HDR")
    if record.get("audio_codec"):
        summary["audio_codec"] = record["audio_codec"]
    if record.get("audio_channels"):
        summary["audio_channels"] = record["audio_channels"]
    if record.get("audio_track_count", 0) > 1:
        summary["audio_tracks"] = record["audio_track_count"]
    if record.get("subtitle_track_count", 0) > 0:
        summary["subtitle_tracks"] = record["subtitle_track_count"]
    if record.get("rotation"):
        summary["rotation"] = record["rotation"]
    if record.get("has_error"):
        summary["has_error"] = True
    return summary


@mcp.tool()
def search_media(
    vcodec: str | None = None,
    acodec: str | None = None,
    format_name: str | None = None,
    media_type: str | None = None,
    min_width: int | None = None,
    max_width: int | None = None,
    min_height: int | None = None,
    max_height: int | None = None,
    pixel_format: str | None = None,
    bit_depth: int | None = None,
    profile: str | None = None,
    has_hdr: bool | None = None,
    has_rotation: bool | None = None,
    has_subtitle: bool | None = None,
    has_multi_audio: bool | None = None,
    has_error: bool | None = None,
    interlaced: bool | None = None,
    has_chapters: bool | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    min_duration: float | None = None,
    max_duration: float | None = None,
    tag: str | None = None,
    category: str | None = None,
    limit: int = 50,
) -> str:
    """Search indexed media files by various criteria.

    All parameters are optional. Combine multiple parameters to narrow results.
    Returns up to `limit` matching files (default 50, max 50).

    Examples:
    - Find HEVC videos: search_media(vcodec="hevc")
    - Find HDR content: search_media(has_hdr=True)
    - Find 4K videos: search_media(min_width=3840)
    - Find files with multiple audio tracks: search_media(has_multi_audio=True)
    - Find large files (>1GB): search_media(min_size=1073741824)
    """
    db = _get_db()
    try:
        conditions: list[str] = []
        params: list[Any] = []

        if vcodec:
            conditions.append("video_codec = ?")
            params.append(vcodec)
        if acodec:
            conditions.append("audio_codec = ?")
            params.append(acodec)
        if format_name:
            conditions.append("format_name LIKE ?")
            params.append(f"%{format_name}%")
        if media_type:
            conditions.append("media_type = ?")
            params.append(media_type)
        if min_width is not None:
            conditions.append("width >= ?")
            params.append(min_width)
        if max_width is not None:
            conditions.append("width <= ?")
            params.append(max_width)
        if min_height is not None:
            conditions.append("height >= ?")
            params.append(min_height)
        if max_height is not None:
            conditions.append("height <= ?")
            params.append(max_height)
        if pixel_format:
            conditions.append("pixel_format = ?")
            params.append(pixel_format)
        if bit_depth is not None:
            conditions.append("bit_depth = ?")
            params.append(bit_depth)
        if profile:
            conditions.append("video_profile = ?")
            params.append(profile)
        if has_hdr is not None:
            conditions.append("has_hdr = ?")
            params.append(1 if has_hdr else 0)
        if has_rotation is True:
            conditions.append("rotation IS NOT NULL AND rotation != 0")
        elif has_rotation is False:
            conditions.append("(rotation IS NULL OR rotation = 0)")
        if has_subtitle is True:
            conditions.append("subtitle_track_count > 0")
        elif has_subtitle is False:
            conditions.append("subtitle_track_count = 0")
        if has_multi_audio is True:
            conditions.append("audio_track_count > 1")
        elif has_multi_audio is False:
            conditions.append("audio_track_count <= 1")
        if has_error is not None:
            conditions.append("has_error = ?")
            params.append(1 if has_error else 0)
        if interlaced is True:
            conditions.append("field_order IS NOT NULL AND field_order != 'progressive'")
        elif interlaced is False:
            conditions.append("(field_order IS NULL OR field_order = 'progressive')")
        if has_chapters is True:
            conditions.append("chapter_count > 0")
        elif has_chapters is False:
            conditions.append("chapter_count = 0")
        if min_size is not None:
            conditions.append("file_size >= ?")
            params.append(min_size)
        if max_size is not None:
            conditions.append("file_size <= ?")
            params.append(max_size)
        if min_duration is not None:
            conditions.append("duration >= ?")
            params.append(min_duration)
        if max_duration is not None:
            conditions.append("duration <= ?")
            params.append(max_duration)
        if tag:
            conditions.append(
                "id IN (SELECT mt.media_id FROM media_tags mt "
                "JOIN tags t ON t.id = mt.tag_id WHERE t.name = ?)"
            )
            params.append(tag)
        if category:
            conditions.append(
                "id IN (SELECT mc.media_id FROM media_categories mc "
                "JOIN categories c ON c.id = mc.category_id WHERE c.name = ?)"
            )
            params.append(category)

        actual_limit = min(limit or 50, 50)
        rows = db.query_media(conditions, params, limit=actual_limit)
        results = [_format_media_summary(r) for r in rows]
        return json.dumps({"count": len(results), "results": results}, indent=2, default=str)
    finally:
        db.close()


@mcp.tool()
def get_media_info(file_path: str) -> str:
    """Get complete metadata for a single media file.

    Provide the full file path. Returns all indexed metadata including
    codec details, resolution, HDR info, audio tracks, and raw ffprobe data.

    Example: get_media_info(file_path="/path/to/video.mp4")
    """
    db = _get_db()
    try:
        record = db.get_media_by_path(file_path)
        if not record:
            return json.dumps({"error": f"File not found in database: {file_path}"})

        tags = db.get_tags_for_media(record["id"])
        categories = db.get_categories_for_media(record["id"])
        record["tags"] = tags
        record["categories"] = categories

        # Parse raw_metadata for structured output
        if record.get("raw_metadata"):
            try:
                record["raw_metadata"] = json.loads(record["raw_metadata"])
            except json.JSONDecodeError:
                pass

        return json.dumps(record, indent=2, default=str)
    finally:
        db.close()


@mcp.tool()
def list_categories() -> str:
    """List all tags and categories in the media database.

    Returns the complete list of user-defined tags and categories
    with their usage counts.
    """
    db = _get_db()
    try:
        tags = db.list_all_tags()
        cats = db.list_all_categories()
        return json.dumps({
            "tags": tags,
            "categories": cats,
        }, indent=2, default=str)
    finally:
        db.close()


@mcp.tool()
def get_stats() -> str:
    """Get database statistics: total files, type distribution, codec distribution.

    Useful for understanding what media files are available before searching.
    """
    db = _get_db()
    try:
        total = db.count_media()

        type_rows = db.get_media_type_stats()

        vcodec_rows = db.get_codec_stats(col="video_codec", limit=20)

        acodec_rows = db.get_codec_stats(col="audio_codec", limit=20)

        total_size = db.get_total_size()

        return json.dumps({
            "total_files": total,
            "total_size_bytes": total_size,
            "by_type": type_rows,
            "video_codecs": vcodec_rows,
            "audio_codecs": acodec_rows,
        }, indent=2, default=str)
    finally:
        db.close()


@mcp.tool()
def analyze_space(top_n: int = 20) -> str:
    """Analyze disk space usage of indexed media files.

    Returns the largest files and per-directory breakdown.

    Args:
        top_n: Number of top files to return (default 20).
    """
    from avshelf.analysis import analyze_space as _analyze

    db = _get_db()
    try:
        result = _analyze(db, top_n=top_n)
        return json.dumps(result, indent=2, default=str)
    finally:
        db.close()


@mcp.tool()
def get_deep_scan_results(scan_id: int, media_id: int | None = None) -> str:
    """Get frame-level MD5 results from a deep scan.

    Args:
        scan_id: The deep scan ID to retrieve results for.
        media_id: Optional — filter results to a specific media file.

    Returns per-frame MD5 values for decode verification.
    """
    db = _get_db()
    try:
        results = db.get_deep_scan_results(scan_id, media_id=media_id)
        scans = db.list_deep_scans()
        scan_info = next((s for s in scans if s["id"] == scan_id), None)
        return json.dumps({
            "scan_info": scan_info,
            "results": results,
        }, indent=2, default=str)
    finally:
        db.close()


def run_server() -> None:
    """Start the MCP server using stdio transport."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    run_server()
