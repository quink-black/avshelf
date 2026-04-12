"""Natural language query — translate user questions into structured search."""

from __future__ import annotations

import json
from typing import Any

from avshelf.config import Config
from avshelf.database import Database

# System prompt sent to the LLM to guide structured query generation
_SYSTEM_PROMPT = """You are a media file search assistant. The user will describe what media files they want to find in natural language. Your job is to translate their request into a structured JSON query.

Available search fields:
- video_codec: string (e.g. "h264", "hevc", "vp9", "av1", "vvc")
- audio_codec: string (e.g. "aac", "opus", "flac", "ac3")
- format_name: string (e.g. "mp4", "mkv", "mov", "avi")
- media_type: string ("video", "audio", "image", "subtitle")
- min_width, max_width: integer
- min_height, max_height: integer
- pixel_format: string (e.g. "yuv420p", "yuv420p10le")
- bit_depth: integer (e.g. 8, 10, 12)
- video_profile: string (e.g. "High", "Main 10")
- has_hdr: boolean
- has_rotation: boolean
- has_subtitle: boolean
- has_multi_audio: boolean (audio_track_count > 1)
- has_error: boolean
- interlaced: boolean
- has_chapters: boolean
- min_size_bytes, max_size_bytes: integer (in bytes)
- min_duration, max_duration: float (in seconds)
- tag: string
- category: string
- sort: string ("size", "duration", "name")
- limit: integer

Respond with ONLY a valid JSON object containing the applicable fields. Do not include fields that are not mentioned or implied by the user's query. Example:

User: "find HDR videos with multiple audio tracks"
Response: {"has_hdr": true, "has_multi_audio": true, "media_type": "video"}

User: "large h265 files over 1GB"
Response: {"video_codec": "hevc", "min_size_bytes": 1073741824}
"""


def _build_query_from_json(parsed: dict) -> tuple[list[str], list[Any], str | None, int | None]:
    """Convert a parsed JSON query dict into SQL conditions and params."""
    conditions: list[str] = []
    params: list[Any] = []
    order_by = None
    limit = None

    field_map = {
        "video_codec": ("video_codec = ?", str),
        "audio_codec": ("audio_codec = ?", str),
        "format_name": ("format_name LIKE ?", lambda v: f"%{v}%"),
        "media_type": ("media_type = ?", str),
        "pixel_format": ("pixel_format = ?", str),
        "bit_depth": ("bit_depth = ?", int),
        "video_profile": ("video_profile = ?", str),
        "min_width": ("width >= ?", int),
        "max_width": ("width <= ?", int),
        "min_height": ("height >= ?", int),
        "max_height": ("height <= ?", int),
        "min_size_bytes": ("file_size >= ?", int),
        "max_size_bytes": ("file_size <= ?", int),
        "min_duration": ("duration >= ?", float),
        "max_duration": ("duration <= ?", float),
    }

    for key, (sql_frag, converter) in field_map.items():
        if key in parsed:
            conditions.append(sql_frag)
            params.append(converter(parsed[key]))

    # Boolean fields
    bool_map = {
        "has_hdr": ("has_hdr = ?", lambda v: 1 if v else 0),
        "has_error": ("has_error = ?", lambda v: 1 if v else 0),
    }
    for key, (sql_frag, converter) in bool_map.items():
        if key in parsed:
            conditions.append(sql_frag)
            params.append(converter(parsed[key]))

    if parsed.get("has_rotation") is True:
        conditions.append("rotation IS NOT NULL AND rotation != 0")
    elif parsed.get("has_rotation") is False:
        conditions.append("(rotation IS NULL OR rotation = 0)")

    if parsed.get("has_subtitle") is True:
        conditions.append("subtitle_track_count > 0")
    elif parsed.get("has_subtitle") is False:
        conditions.append("subtitle_track_count = 0")

    if parsed.get("has_multi_audio") is True:
        conditions.append("audio_track_count > 1")
    elif parsed.get("has_multi_audio") is False:
        conditions.append("audio_track_count <= 1")

    if parsed.get("interlaced") is True:
        conditions.append("field_order IS NOT NULL AND field_order != 'progressive'")
    elif parsed.get("interlaced") is False:
        conditions.append("(field_order IS NULL OR field_order = 'progressive')")

    if parsed.get("has_chapters") is True:
        conditions.append("chapter_count > 0")
    elif parsed.get("has_chapters") is False:
        conditions.append("chapter_count = 0")

    if "tag" in parsed:
        conditions.append(
            "id IN (SELECT mt.media_id FROM media_tags mt "
            "JOIN tags t ON t.id = mt.tag_id WHERE t.name = ?)"
        )
        params.append(parsed["tag"])

    if "category" in parsed:
        conditions.append(
            "id IN (SELECT mc.media_id FROM media_categories mc "
            "JOIN categories c ON c.id = mc.category_id WHERE c.name = ?)"
        )
        params.append(parsed["category"])

    sort_val = parsed.get("sort")
    if sort_val:
        sort_map = {"size": "file_size DESC", "duration": "duration DESC", "name": "file_name ASC"}
        order_by = sort_map.get(sort_val)

    limit = parsed.get("limit")

    return conditions, params, order_by, limit


def _call_openai(prompt: str, config: Config) -> str:
    """Call OpenAI-compatible API."""
    import urllib.request

    api_key = config.get("llm.api_key", "")
    model = config.get("llm.model", "gpt-4o-mini")
    base_url = config.get("llm.base_url", "https://api.openai.com/v1")

    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }).encode()

    req = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    return result["choices"][0]["message"]["content"]


def _call_anthropic(prompt: str, config: Config) -> str:
    """Call Anthropic Claude API."""
    import urllib.request

    api_key = config.get("llm.api_key", "")
    model = config.get("llm.model", "claude-sonnet-4-20250514")

    payload = json.dumps({
        "model": model,
        "max_tokens": 1024,
        "system": _SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": prompt},
        ],
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    return result["content"][0]["text"]


def natural_language_search(
    query: str,
    db: Database,
    config: Config,
) -> tuple[dict, list[dict]]:
    """Translate a natural language query into a structured search and execute it.

    Returns (parsed_query_dict, search_results).
    Raises ValueError if LLM is not configured or returns invalid JSON.
    """
    provider = config.get("llm.provider", "")
    api_key = config.get("llm.api_key", "")

    if not provider or not api_key:
        raise ValueError(
            "LLM not configured. Set provider and API key:\n"
            "  avshelf config set llm.provider openai\n"
            "  avshelf config set llm.api_key <your-key>\n"
            "Or use environment variables: AVSHELF_LLM_PROVIDER, AVSHELF_LLM_API_KEY"
        )

    if provider == "openai":
        response = _call_openai(query, config)
    elif provider in ("anthropic", "claude"):
        response = _call_anthropic(query, config)
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")

    # Extract JSON from response (handle markdown code blocks)
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1])
    parsed = json.loads(text)

    conditions, params, order_by, limit = _build_query_from_json(parsed)
    results = db.query_media(conditions, params, order_by=order_by, limit=limit)

    return parsed, results
