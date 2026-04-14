"""AVShelf CLI — command-line interface for media asset management."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.table import Table

from avshelf.config import Config
from avshelf.database import Database

app = typer.Typer(
    name="avshelf",
    help="A media asset management tool powered by ffprobe.",
    no_args_is_help=True,
)
console = Console()

# Sub-command groups
config_app = typer.Typer(help="Manage configuration.")
tag_app = typer.Typer(help="Manage tags on media files.")
classify_app = typer.Typer(help="Manage categories on media files.")
rule_app = typer.Typer(help="Manage directory auto-tagging rules.")
deep_scan_app = typer.Typer(help="Deep scan: frame-level MD5 collection.")
trash_app = typer.Typer(help="Manage the trash (recycle bin).")
stats_app = typer.Typer(help="Database statistics.")

app.add_typer(config_app, name="config")
app.add_typer(tag_app, name="tag")
app.add_typer(classify_app, name="classify")
app.add_typer(rule_app, name="rule")
app.add_typer(deep_scan_app, name="deep-scan")
app.add_typer(trash_app, name="trash")
app.add_typer(stats_app, name="stats")


def _get_config() -> Config:
    cfg = Config()
    cfg.ensure_dirs()
    return cfg


def _get_db(cfg: Config) -> Database:
    db = Database(cfg.db_path)
    db.connect()
    return db


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------

@app.command()
def scan(
    directory: str = typer.Argument(..., help="Directory to scan."),
    full: bool = typer.Option(False, "--full", help="Full re-scan, ignore incremental cache."),
    probe_all: bool = typer.Option(False, "--probe-all", help="Probe all files including unknown extensions."),
) -> None:
    """Scan a directory and index media files into the database."""
    from avshelf.scanner import scan_directory

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        result = scan_directory(directory, db, cfg, full=full, probe_all=probe_all)

        summary = (
            f"\nScan complete!\n"
            f"  Added:   {result.added}\n"
            f"  Updated: {result.updated}\n"
            f"  Skipped: {result.skipped}\n"
            f"  Errors:  {result.errored}"
        )
        if result.by_type:
            summary += "\n  By type:"
            for mtype, count in sorted(result.by_type.items()):
                summary += f"\n    {mtype}: {count}"

        if hasattr(sys.stdout, "isatty") and sys.stdout.isatty():
            console.print()
            console.print("[bold green]Scan complete![/bold green]")
            console.print(f"  Added:   {result.added}")
            console.print(f"  Updated: {result.updated}")
            console.print(f"  Skipped: {result.skipped}")
            console.print(f"  Errors:  {result.errored}")
            if result.by_type:
                console.print("  By type:")
                for mtype, count in sorted(result.by_type.items()):
                    console.print(f"    {mtype}: {count}")
        else:
            print(summary, flush=True)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------

@app.command()
def refresh(
    dir: Optional[str] = typer.Option(None, "--dir", help="Refresh only this directory."),
) -> None:
    """Refresh the database to reflect file system changes."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        from avshelf.scanner import scan_directory

        if dir:
            dirs_to_refresh = [dir]
        else:
            dirs_to_refresh = db.list_distinct_scan_sources()

        total_added = total_updated = total_deleted = 0

        for d in dirs_to_refresh:
            dp = Path(d)
            if not dp.is_dir():
                console.print(f"[yellow]Directory no longer exists: {d}[/yellow]")
                # Soft-delete all files from this directory
                media = db.list_media_in_dir(d)
                for mf in media:
                    db.soft_delete_media(mf["file_path"])
                    total_deleted += 1
                continue

            # Detect deleted files
            media = db.list_media_in_dir(d)
            for mf in media:
                if not Path(mf["file_path"]).exists():
                    db.soft_delete_media(mf["file_path"])
                    total_deleted += 1

            # Re-scan for new/modified files
            result = scan_directory(d, db, cfg)
            total_added += result.added
            total_updated += result.updated

        console.print("[bold green]Refresh complete![/bold green]")
        console.print(f"  Added:   {total_added}")
        console.print(f"  Updated: {total_updated}")
        console.print(f"  Deleted: {total_deleted}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# purge
# ---------------------------------------------------------------------------

@app.command()
def purge() -> None:
    """Permanently remove all soft-deleted records from the database."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        count = db.purge_deleted()
        console.print(f"Purged {count} deleted record(s).")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------

@app.command()
def search(
    vcodec: Optional[str] = typer.Option(None, "--vcodec", help="Video codec (e.g. hevc, h264)."),
    acodec: Optional[str] = typer.Option(None, "--acodec", help="Audio codec (e.g. aac, opus)."),
    format: Optional[str] = typer.Option(None, "--format", help="Container format (e.g. mp4, mkv)."),
    type: Optional[str] = typer.Option(None, "--type", help="Media type: video, audio, image, subtitle."),
    min_width: Optional[int] = typer.Option(None, "--min-width"),
    max_width: Optional[int] = typer.Option(None, "--max-width"),
    min_height: Optional[int] = typer.Option(None, "--min-height"),
    max_height: Optional[int] = typer.Option(None, "--max-height"),
    res: Optional[str] = typer.Option(None, "--res", help="Exact resolution WxH (e.g. 1920x1080)."),
    pixel_format: Optional[str] = typer.Option(None, "--pixel-format", help="Pixel format (e.g. yuv420p)."),
    bit_depth: Optional[int] = typer.Option(None, "--bit-depth"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Codec profile (e.g. High, Main 10)."),
    has_hdr: Optional[bool] = typer.Option(None, "--has-hdr/--no-hdr"),
    has_rotation: Optional[bool] = typer.Option(None, "--has-rotation/--no-rotation"),
    has_subtitle: Optional[bool] = typer.Option(None, "--has-subtitle/--no-subtitle"),
    has_multi_audio: Optional[bool] = typer.Option(None, "--has-multi-audio/--no-multi-audio"),
    audio_tracks: Optional[str] = typer.Option(None, "--audio-tracks", help="Audio track count filter (e.g. >1, =2, >=3)."),
    has_error: Optional[bool] = typer.Option(None, "--has-error/--no-error"),
    interlaced: Optional[bool] = typer.Option(None, "--interlaced/--no-interlaced"),
    has_chapters: Optional[bool] = typer.Option(None, "--has-chapters/--no-chapters"),
    min_size: Optional[str] = typer.Option(None, "--min-size", help="Min file size (e.g. 100MB, 1GB)."),
    max_size: Optional[str] = typer.Option(None, "--max-size", help="Max file size."),
    min_duration: Optional[float] = typer.Option(None, "--min-duration", help="Min duration in seconds."),
    max_duration: Optional[float] = typer.Option(None, "--max-duration", help="Max duration in seconds."),
    tag: Optional[str] = typer.Option(None, "--tag", help="Filter by tag name."),
    category: Optional[str] = typer.Option(None, "--category", help="Filter by category name."),
    sort: Optional[str] = typer.Option(None, "--sort", help="Sort by: size, duration, name."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Max results to return."),
    path_only: bool = typer.Option(False, "--path-only", help="Output file paths only."),
    count: bool = typer.Option(False, "--count", help="Output count only."),
    output: str = typer.Option("table", "--output", help="Output format: table, json, csv."),
    raw_query: Optional[list[str]] = typer.Option(None, "--raw-query",
        help="Query raw_metadata JSON. Format: 'json.path op value'. "
             "Path uses dot notation (e.g. 'streams[0].codec_name=h264', "
             "'format.bit_rate>1000000'). Can be specified multiple times."),
) -> None:
    """Search media files by various criteria."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        conditions: list[str] = []
        params: list = []

        if vcodec:
            conditions.append("video_codec = ?")
            params.append(vcodec)
        if acodec:
            conditions.append("audio_codec = ?")
            params.append(acodec)
        if format:
            conditions.append("format_name LIKE ?")
            params.append(f"%{format}%")
        if type:
            conditions.append("media_type = ?")
            params.append(type)
        if min_width:
            conditions.append("width >= ?")
            params.append(min_width)
        if max_width:
            conditions.append("width <= ?")
            params.append(max_width)
        if min_height:
            conditions.append("height >= ?")
            params.append(min_height)
        if max_height:
            conditions.append("height <= ?")
            params.append(max_height)
        if res:
            parts = res.lower().split("x")
            if len(parts) == 2:
                conditions.append("width = ? AND height = ?")
                params.extend([int(parts[0]), int(parts[1])])
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
        if audio_tracks:
            op, val = _parse_comparison(audio_tracks)
            conditions.append(f"audio_track_count {op} ?")
            params.append(val)
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
        if min_size:
            conditions.append("file_size >= ?")
            params.append(_parse_size(min_size))
        if max_size:
            conditions.append("file_size <= ?")
            params.append(_parse_size(max_size))
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
        if raw_query:
            for rq in raw_query:
                cond, p = _parse_raw_query(rq)
                conditions.append(cond)
                params.extend(p)

        order_by = None
        if sort:
            sort_map = {
                "size": "file_size DESC",
                "duration": "duration DESC",
                "name": "file_name ASC",
            }
            order_by = sort_map.get(sort, "file_name ASC")

        if count:
            n = db.count_media(conditions, params)
            console.print(str(n))
            return

        rows = db.query_media(conditions, params, order_by=order_by, limit=limit)

        if path_only:
            for r in rows:
                console.print(r["file_path"])
            return

        if output == "json":
            import json
            console.print(json.dumps(rows, indent=2, default=str))
            return

        if output == "csv":
            if rows:
                import csv
                import io
                buf = io.StringIO()
                writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
                console.print(buf.getvalue())
            return

        # Default: table
        _print_search_table(rows)
    finally:
        db.close()


def _parse_comparison(expr: str) -> tuple[str, int]:
    """Parse a comparison expression like '>1', '>=2', '=3' into (sql_op, value).

    Supported operators: >, >=, <, <=, =, !=
    If no operator is given, defaults to '='.
    """
    expr = expr.strip()
    for op in (">=" , "<=", "!=", ">", "<", "="):
        if expr.startswith(op):
            return (op, int(expr[len(op):].strip()))
    # No operator prefix – treat as exact match
    return ("=", int(expr))


def _parse_size(size_str: str) -> int:
    """Parse a human-readable size string like '100MB' into bytes."""
    size_str = size_str.strip().upper()
    multipliers = {"KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    for suffix, mult in multipliers.items():
        if size_str.endswith(suffix):
            return int(float(size_str[:-len(suffix)]) * mult)
    return int(size_str)


def _parse_raw_query(expr: str) -> tuple[str, list]:
    """Parse a raw_metadata JSON query expression into SQL condition and params.

    Supports dot-notation paths with optional array indices.
    Examples:
        'streams[0].codec_name=h264'  -> json_extract(raw_metadata, '$.streams[0].codec_name') = ?
        'format.bit_rate>1000000'     -> json_extract(raw_metadata, '$.format.bit_rate') > ?
        'format.tags.title~test'      -> json_extract(raw_metadata, '$.format.tags.title') LIKE ?

    Operators: =, !=, >, >=, <, <=, ~ (LIKE/contains)
    """
    import re
    # Split on the first operator occurrence
    m = re.match(r'^([\w.\[\]]+)\s*(>=|<=|!=|>|<|=|~)\s*(.+)$', expr.strip())
    if not m:
        raise typer.BadParameter(
            f"Invalid --raw-query format: '{expr}'. "
            "Expected: 'json.path op value' (e.g. 'streams[0].codec_name=h264')"
        )
    path_str, op, value = m.group(1), m.group(2), m.group(3).strip()

    # Convert dot notation to SQLite json_extract path ($.foo.bar[0].baz)
    json_path = "$." + path_str

    sql_func = f"json_extract(raw_metadata, ?)"

    if op == "~":
        # LIKE / contains search
        return f"{sql_func} LIKE ?", [json_path, f"%{value}%"]

    # Try to cast value to number for numeric comparisons
    try:
        num_val = int(value)
        return f"{sql_func} {op} ?", [json_path, num_val]
    except ValueError:
        try:
            num_val = float(value)
            return f"{sql_func} {op} ?", [json_path, num_val]
        except ValueError:
            pass

    # String comparison
    return f"{sql_func} {op} ?", [json_path, value]


def _print_search_table(rows: list[dict]) -> None:
    """Print search results as a Rich table."""
    if not rows:
        console.print("[dim]No results found.[/dim]")
        return

    table = Table(show_lines=False)
    table.add_column("Name", style="cyan", no_wrap=True, max_width=40)
    table.add_column("Type", style="green")
    table.add_column("Codec", style="yellow")
    table.add_column("Resolution")
    table.add_column("Duration", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Path", style="dim", max_width=60)

    for r in rows:
        dur = ""
        if r.get("duration"):
            m, s = divmod(int(r["duration"]), 60)
            h, m = divmod(m, 60)
            dur = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

        size = _format_size(r.get("file_size", 0))
        res = ""
        if r.get("width") and r.get("height"):
            res = f"{r['width']}x{r['height']}"

        codec = r.get("video_codec") or r.get("audio_codec") or ""

        table.add_row(
            r.get("file_name", ""),
            r.get("media_type", ""),
            codec,
            res,
            dur,
            size,
            r.get("file_path", ""),
        )

    console.print(table)
    console.print(f"[dim]{len(rows)} result(s)[/dim]")


def _format_size(size_bytes: int) -> str:
    """Format bytes into a human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}" if unit != "B" else f"{size_bytes}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}PB"


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------

@app.command()
def info(
    file: str = typer.Argument(..., help="Path to the media file."),
) -> None:
    """Show detailed metadata for a single media file."""
    import json as json_mod

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        resolved = str(Path(file).resolve())
        record = db.get_media_by_path(resolved)
        if not record:
            console.print(f"[red]File not found in database: {file}[/red]")
            console.print("[dim]Run 'avshelf scan' first to index this file.[/dim]")
            raise typer.Exit(1)

        # Display key fields
        table = Table(title=record["file_name"], show_header=False, show_lines=True)
        table.add_column("Field", style="bold cyan")
        table.add_column("Value")

        skip_fields = {"raw_metadata", "tags_json", "id"}
        for key, val in record.items():
            if key in skip_fields or val is None:
                continue
            table.add_row(key, str(val))

        # Tags and categories
        tags = db.get_tags_for_media(record["id"])
        if tags:
            table.add_row("tags", ", ".join(tags))
        cats = db.get_categories_for_media(record["id"])
        if cats:
            table.add_row("categories", ", ".join(cats))

        console.print(table)

        # Raw metadata (truncated if too large)
        if record.get("raw_metadata"):
            raw = json_mod.loads(record["raw_metadata"])
            formatted = json_mod.dumps(raw, indent=2, ensure_ascii=False)
            console.print("\n[bold]Raw ffprobe metadata:[/bold]")
            if len(formatted) > 5000:
                console.print(formatted[:5000])
                console.print(
                    f"\n[dim]... output truncated ({len(formatted)} chars total, "
                    f"showing first 5000). Use 'avshelf search --raw-query' for "
                    f"targeted metadata queries.[/dim]"
                )
            else:
                console.print(formatted)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# config commands
# ---------------------------------------------------------------------------

@config_app.command("show")
def config_show() -> None:
    """Show current configuration."""
    import json as json_mod
    cfg = _get_config()
    console.print(json_mod.dumps(cfg.data, indent=2, default=str))


_VALID_CONFIG_KEYS = {
    "database.path",
    "scan.extensions.video", "scan.extensions.audio",
    "scan.extensions.subtitle", "scan.extensions.image",
    "scan.exclude_patterns", "scan.hash_algorithm",
    "scan.ffprobe_path", "scan.ffmpeg_path",
    "deep_scan.default_frames",
    "llm.provider", "llm.api_key", "llm.model", "llm.base_url", "llm.timeout",
    "analysis.boring_codecs",
}


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Config key (dotted path, e.g. llm.provider)."),
    value: str = typer.Argument(..., help="Value to set."),
) -> None:
    """Set a configuration value."""
    if key not in _VALID_CONFIG_KEYS:
        console.print(f"[red]Unknown config key: {key}[/red]")
        console.print(f"[dim]Valid keys: {', '.join(sorted(_VALID_CONFIG_KEYS))}[/dim]")
        raise typer.Exit(1)
    cfg = _get_config()
    # Attempt to parse as int/float/bool
    parsed: str | int | float | bool = value
    if value.lower() in ("true", "false"):
        parsed = value.lower() == "true"
    else:
        try:
            parsed = int(value)
        except ValueError:
            try:
                parsed = float(value)
            except ValueError:
                pass
    cfg.set(key, parsed)
    console.print(f"[green]Set {key} = {parsed}[/green]")


# ---------------------------------------------------------------------------
# tag commands
# ---------------------------------------------------------------------------

@tag_app.command("add")
def tag_add(
    file: str = typer.Argument(..., help="File path."),
    tags: list[str] = typer.Argument(..., help="Tags to add."),
    query: Optional[str] = typer.Option(None, "--query", help="Apply to search results instead of a single file."),
) -> None:
    """Add tags to a media file (or to search results via --query)."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        if query:
            conditions, params = _parse_query_string(query)
            if not conditions:
                console.print("[red]No valid filter conditions parsed from --query.[/red]")
                raise typer.Exit(1)
            rows = db.query_media(conditions, params)
            if not rows:
                console.print("[yellow]No files matched the query.[/yellow]")
                return
            count = 0
            for row in rows:
                db.add_tags_to_media(row["id"], tags)
                count += 1
            console.print(f"[green]Added tags {tags} to {count} file(s).[/green]")
            return
        resolved = str(Path(file).resolve())
        record = db.get_media_by_path(resolved)
        if not record:
            console.print(f"[red]File not in database: {file}[/red]")
            raise typer.Exit(1)
        db.add_tags_to_media(record["id"], tags)
        console.print(f"[green]Added tags {tags} to {record['file_name']}[/green]")
    finally:
        db.close()


@tag_app.command("remove")
def tag_remove(
    file: str = typer.Argument(..., help="File path."),
    tags: list[str] = typer.Argument(..., help="Tags to remove."),
) -> None:
    """Remove tags from a media file."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        resolved = str(Path(file).resolve())
        record = db.get_media_by_path(resolved)
        if not record:
            console.print(f"[red]File not in database: {file}[/red]")
            raise typer.Exit(1)
        db.remove_tags_from_media(record["id"], tags)
        console.print(f"[green]Removed tags {tags} from {record['file_name']}[/green]")
    finally:
        db.close()


@tag_app.command("list")
def tag_list() -> None:
    """List all tags and their usage count."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        tags = db.list_all_tags()
        if not tags:
            console.print("[dim]No tags found.[/dim]")
            return
        table = Table()
        table.add_column("Tag", style="cyan")
        table.add_column("Count", justify="right")
        for t in tags:
            table.add_row(t["name"], str(t["count"]))
        console.print(table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# classify commands
# ---------------------------------------------------------------------------

@classify_app.callback(invoke_without_command=True)
def classify_set(
    ctx: typer.Context,
    file: Optional[str] = typer.Argument(None, help="File path."),
    category: Optional[str] = typer.Option(None, "--category", help="Category name."),
) -> None:
    """Assign a category to a media file, or use subcommands (e.g. classify list)."""
    if ctx.invoked_subcommand is not None:
        return
    if not file or not category:
        console.print("[red]Usage: avshelf classify <file> --category <name>[/red]")
        raise typer.Exit(1)
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        resolved = str(Path(file).resolve())
        record = db.get_media_by_path(resolved)
        if not record:
            console.print(f"[red]File not in database: {file}[/red]")
            raise typer.Exit(1)
        db.add_category_to_media(record["id"], category)
        console.print(f"[green]Set category '{category}' on {record['file_name']}[/green]")
    finally:
        db.close()


@classify_app.command("list")
def classify_list() -> None:
    """List all categories and their usage count."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        cats = db.list_all_categories()
        if not cats:
            console.print("[dim]No categories found.[/dim]")
            return
        table = Table()
        table.add_column("Category", style="cyan")
        table.add_column("Count", justify="right")
        for c in cats:
            table.add_row(c["name"], str(c["count"]))
        console.print(table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# stats commands
# ---------------------------------------------------------------------------

@stats_app.callback(invoke_without_command=True)
def stats_overview(ctx: typer.Context) -> None:
    """Show database statistics overview."""
    if ctx.invoked_subcommand is not None:
        return
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        total = db.count_media()
        console.print(f"[bold]Total media files:[/bold] {total}")

        # By type
        rows = db.get_media_type_stats()
        if rows:
            table = Table(title="By Type")
            table.add_column("Type", style="cyan")
            table.add_column("Count", justify="right")
            for r in rows:
                table.add_row(r["media_type"] or "unknown", str(r["cnt"]))
            console.print(table)

        # Top codecs
        for label, col in [("Video Codecs", "video_codec"), ("Audio Codecs", "audio_codec")]:
            rows = db.get_codec_stats(col=col, limit=10)
            if rows:
                table = Table(title=label)
                table.add_column("Codec", style="yellow")
                table.add_column("Count", justify="right")
                for r in rows:
                    table.add_row(r[col], str(r["cnt"]))
                console.print(table)
    finally:
        db.close()


@stats_app.command("tags")
def stats_tags() -> None:
    """Show tag usage statistics."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        tags = db.list_all_tags()
        if not tags:
            console.print("[dim]No tags found.[/dim]")
            return
        table = Table(title="Tag Statistics")
        table.add_column("Tag", style="cyan")
        table.add_column("Files", justify="right")
        for t in tags:
            table.add_row(t["name"], str(t["count"]))
        console.print(table)
    finally:
        db.close()


@stats_app.command("categories")
def stats_categories() -> None:
    """Show category usage statistics."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        cats = db.list_all_categories()
        if not cats:
            console.print("[dim]No categories found.[/dim]")
            return
        table = Table(title="Category Statistics")
        table.add_column("Category", style="cyan")
        table.add_column("Files", justify="right")
        for c in cats:
            table.add_row(c["name"], str(c["count"]))
        console.print(table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# trash commands
# ---------------------------------------------------------------------------

@trash_app.command("list")
def trash_list() -> None:
    """List files in the trash (recycle bin)."""
    import json as json_mod
    from avshelf.config import TRASH_DIR

    meta_file = TRASH_DIR / ".avshelf_trash_meta.json"
    if not meta_file.exists():
        console.print("[dim]Trash is empty.[/dim]")
        return

    meta = json_mod.loads(meta_file.read_text(encoding="utf-8"))
    if not meta:
        console.print("[dim]Trash is empty.[/dim]")
        return

    table = Table(title="Trash")
    table.add_column("Original Path", style="cyan")
    table.add_column("Size", justify="right")
    table.add_column("Trashed At")
    for entry in meta:
        table.add_row(
            entry.get("original_path", ""),
            _format_size(entry.get("file_size", 0)),
            entry.get("trashed_at", ""),
        )
    console.print(table)


@trash_app.command("purge")
def trash_purge(
    force: bool = typer.Option(False, "--force", help="Skip confirmation."),
) -> None:
    """Permanently delete all files in the trash."""
    import json as json_mod
    import shutil
    from avshelf.config import TRASH_DIR

    meta_file = TRASH_DIR / ".avshelf_trash_meta.json"
    if not meta_file.exists():
        console.print("[dim]Trash is empty.[/dim]")
        return

    meta = json_mod.loads(meta_file.read_text(encoding="utf-8"))
    if not meta:
        console.print("[dim]Trash is empty.[/dim]")
        return

    total_size = sum(e.get("file_size", 0) for e in meta)
    console.print(f"Files in trash: {len(meta)}")
    console.print(f"Total size: {_format_size(total_size)}")

    if not force:
        confirm = typer.prompt("Type 'yes' to permanently delete")
        if confirm != "yes":
            console.print("[yellow]Cancelled.[/yellow]")
            return

    # Clean up database records for trashed files
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        for entry in meta:
            original = entry.get("original_path", "")
            if original:
                # Permanently remove the soft-deleted database record
                db.purge_media_by_path(original)
    finally:
        db.close()

    # Remove all files in trash subdirectories
    for item in TRASH_DIR.iterdir():
        if item.name == ".avshelf_trash_meta.json":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        elif item.is_file():
            item.unlink()

    meta_file.write_text("[]", encoding="utf-8")
    console.print(f"[green]Permanently deleted {len(meta)} file(s), freed {_format_size(total_size)}.[/green]")


@trash_app.command("restore")
def trash_restore(
    original_path: str = typer.Argument(..., help="Original file path to restore."),
) -> None:
    """Restore a file from the trash to its original location."""
    import json as json_mod
    import shutil
    from avshelf.config import TRASH_DIR

    meta_file = TRASH_DIR / ".avshelf_trash_meta.json"
    if not meta_file.exists():
        console.print("[red]Trash is empty.[/red]")
        raise typer.Exit(1)

    meta = json_mod.loads(meta_file.read_text(encoding="utf-8"))
    resolved = str(Path(original_path).resolve())

    entry = None
    for e in meta:
        if e.get("original_path") == resolved:
            entry = e
            break

    if not entry:
        console.print(f"[red]File not found in trash: {original_path}[/red]")
        raise typer.Exit(1)

    trash_path = Path(entry["trash_path"])
    dest = Path(entry["original_path"])

    if dest.exists():
        console.print(f"[red]Destination already exists: {dest}[/red]")
        raise typer.Exit(1)

    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(trash_path), str(dest))

    # Restore the database record (clear soft-delete marker)
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        db.restore_media(resolved, entry.get("trashed_at", ""))
    finally:
        db.close()

    meta = [e for e in meta if e.get("original_path") != resolved]
    meta_file.write_text(json_mod.dumps(meta, indent=2), encoding="utf-8")

    console.print(f"[green]Restored: {dest}[/green]")


# ---------------------------------------------------------------------------
# rule commands
# ---------------------------------------------------------------------------

@rule_app.command("add")
def rule_add(
    directory: str = typer.Argument(..., help="Directory path to apply the rule to."),
    tags: Optional[str] = typer.Option(None, "--tags", help="Comma-separated tags to auto-apply."),
    category: Optional[str] = typer.Option(None, "--category", help="Category to auto-apply."),
) -> None:
    """Add an auto-tagging rule for a directory."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        resolved = str(Path(directory).resolve())
        db.add_directory_rule(resolved, auto_tags=tag_list, auto_category=category)
        console.print(f"[green]Rule added for {resolved}[/green]")
        if tag_list:
            console.print(f"  Auto-tags: {tag_list}")
        if category:
            console.print(f"  Auto-category: {category}")
    finally:
        db.close()


@rule_app.command("list")
def rule_list() -> None:
    """List all directory rules."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        rows = db.list_directory_rules()
        if not rows:
            console.print("[dim]No rules defined.[/dim]")
            return
        table = Table()
        table.add_column("Directory", style="cyan")
        table.add_column("Auto Tags")
        table.add_column("Auto Category")
        for r in rows:
            table.add_row(r["dir_path"], ", ".join(r["auto_tags"]) if r["auto_tags"] else "", r["auto_category"] or "")
        console.print(table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# dedup
# ---------------------------------------------------------------------------

@app.command()
def dedup(
    fast: bool = typer.Option(False, "--fast", help="Use fast hash (head+tail sampling) for pre-screening."),
    output: str = typer.Option("table", "--output", help="Output format: table, json."),
    save_plan: Optional[str] = typer.Option(None, "--save-plan", help="Save cleanup plan to a JSON file."),
) -> None:
    """Find duplicate files by content hash."""
    from avshelf.analysis import find_duplicates, generate_cleanup_plan, save_cleanup_plan

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        groups = find_duplicates(db, fast=fast)
        if not groups:
            console.print("[green]No duplicates found.[/green]")
            return

        if output == "json":
            import json
            data = [{"hash": g.hash_value, "files": g.files, "wasted_bytes": g.wasted_bytes}
                    for g in groups]
            console.print(json.dumps(data, indent=2, default=str))
            return

        total_wasted = 0
        for g in groups:
            console.print(f"\n[bold yellow]Hash: {g.hash_value[:16]}...[/bold yellow] ({len(g.files)} copies)")
            for f in g.files:
                console.print(f"  {_format_size(f['file_size']):>10}  {f['file_path']}")
            total_wasted += g.wasted_bytes

        console.print(f"\n[bold]{len(groups)} duplicate group(s), {_format_size(total_wasted)} wasted.[/bold]")

        if fast:
            console.print(
                "\n[yellow]⚠ Fast hash mode uses head+tail sampling. "
                "Results are for pre-screening only. "
                "Run without --fast to confirm with full hash.[/yellow]"
            )

        if save_plan:
            # Collect all duplicate files except the first in each group (keep one copy)
            plan_files = []
            for g in groups:
                for f in g.files[1:]:
                    plan_files.append(f)
            reason = "duplicate (fast hash)" if fast else "duplicate (full hash)"
            plan = generate_cleanup_plan(plan_files, reason=reason)
            save_cleanup_plan(plan, Path(save_plan))
            console.print(f"[green]Cleanup plan saved to {save_plan} ({len(plan)} entries)[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# similar
# ---------------------------------------------------------------------------

@app.command()
def similar(
    output: str = typer.Option("table", "--output", help="Output format: table, json."),
    save_plan: Optional[str] = typer.Option(None, "--save-plan", help="Save cleanup plan to a JSON file."),
) -> None:
    """Find similar files based on metadata features."""
    from avshelf.analysis import find_similar, generate_cleanup_plan, save_cleanup_plan

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        groups = find_similar(db)
        if not groups:
            console.print("[green]No similar files found.[/green]")
            return

        if output == "json":
            import json
            data = [{"key": g.key, "files": g.files} for g in groups]
            console.print(json.dumps(data, indent=2, default=str))
            return

        for g in groups:
            console.print(f"\n[bold yellow]Group: {g.key}[/bold yellow] ({len(g.files)} files)")
            for f in g.files:
                dur = f"{f.get('duration', 0):.1f}s" if f.get("duration") else "?"
                console.print(f"  {_format_size(f['file_size']):>10}  {dur:>8}  {f['file_path']}")

        console.print(f"\n[bold]{len(groups)} similar group(s) found.[/bold]")

        if save_plan:
            # Collect all similar files except the first in each group
            plan_files = []
            for g in groups:
                for f in g.files[1:]:
                    plan_files.append(f)
            plan = generate_cleanup_plan(plan_files, reason="similar")
            save_cleanup_plan(plan, Path(save_plan))
            console.print(f"[green]Cleanup plan saved to {save_plan} ({len(plan)} entries)[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# space
# ---------------------------------------------------------------------------

@app.command()
def space(
    top: int = typer.Option(20, "--top", help="Number of top files to show."),
) -> None:
    """Analyze disk space usage of indexed media files."""
    from avshelf.analysis import analyze_space

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        result = analyze_space(db, top_n=top)

        console.print(f"[bold]Total: {result['total_files']} files, {_format_size(result['total_size'])}[/bold]\n")

        if result["dir_stats"]:
            table = Table(title="By Directory")
            table.add_column("Directory", style="cyan")
            table.add_column("Files", justify="right")
            table.add_column("Size", justify="right")
            for d in result["dir_stats"]:
                table.add_row(d["scan_source_dir"], str(d["cnt"]), _format_size(d["total_size"] or 0))
            console.print(table)

        if result["top_files"]:
            table = Table(title=f"Top {top} Largest Files")
            table.add_column("Name", style="cyan", max_width=40)
            table.add_column("Size", justify="right")
            table.add_column("Codec", style="yellow")
            table.add_column("Path", style="dim", max_width=60)
            for f in result["top_files"]:
                table.add_row(
                    f["file_name"],
                    _format_size(f["file_size"]),
                    f.get("video_codec") or "",
                    f["file_path"],
                )
            console.print(table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# cold
# ---------------------------------------------------------------------------

@app.command()
def cold(
    days: int = typer.Option(180, "--days", help="Threshold in days since last access."),
    by: str = typer.Option("atime", "--by", help="Timestamp to check: 'atime' (last access, default) or 'mtime' (last modification). For media files, atime is usually a better coldness indicator."),
    limit: Optional[int] = typer.Option(None, "--limit"),
    save_plan: Optional[str] = typer.Option(None, "--save-plan", help="Save cleanup plan to a JSON file."),
) -> None:
    """Find cold files not accessed (or modified) in the last N days.

    Media files are rarely modified after creation, so access time (atime) is
    a better indicator of whether a file is truly 'cold'. If your filesystem
    has atime tracking disabled (e.g. mounted with noatime), use --by mtime.
    """
    from avshelf.analysis import find_cold_files, generate_cleanup_plan, save_cleanup_plan

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        files = find_cold_files(db, days=days, by=by)

        # Check for atime warning (inserted as first element by find_cold_files)
        atime_warning = None
        if files and files[0].get("_atime_warning"):
            atime_warning = files.pop(0)["message"]

        if limit:
            files = files[:limit]
        if not files:
            console.print(f"[green]No cold files found (>{days} days by {by}).[/green]")
            return

        if atime_warning:
            console.print(f"[yellow]⚠ {atime_warning}[/yellow]\n")

        import time
        ts_label = "Last Accessed" if by == "atime" else "Last Modified"
        table = Table(title=f"Cold Files (>{days} days by {by})")
        table.add_column("Name", style="cyan", max_width=40)
        table.add_column("Size", justify="right")
        table.add_column(ts_label)
        table.add_column("Path", style="dim", max_width=60)
        for f in files:
            ts = f.get("cold_ts", f["file_mtime"])
            ts_str = time.strftime("%Y-%m-%d", time.localtime(ts))
            row_data = [f["file_name"], _format_size(f["file_size"]), ts_str, f["file_path"]]
            if by == "atime" and f.get("atime_disabled"):
                row_data[2] = ts_str + " ⚠"
            table.add_row(*row_data)
        console.print(table)
        console.print(f"[dim]{len(files)} cold file(s)[/dim]")

        if save_plan:
            plan = generate_cleanup_plan(files, reason=f"cold (>{days} days by {by})")
            save_cleanup_plan(plan, Path(save_plan))
            console.print(f"[green]Cleanup plan saved to {save_plan} ({len(plan)} entries)[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# boring
# ---------------------------------------------------------------------------

@app.command()
def boring(
    save_plan: Optional[str] = typer.Option(None, "--save-plan", help="Save cleanup plan to a JSON file."),
) -> None:
    """Find files with unremarkable metadata (common codec, low res, no special features)."""
    from avshelf.analysis import find_boring_files, generate_cleanup_plan, save_cleanup_plan, DEFAULT_BORING_CODECS

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        # Allow user to override boring codec list via config
        # Config format: analysis.boring_codecs = "h264:aac,hevc:aac,h264:mp3"
        boring_codecs = None
        raw_codecs = cfg.get("analysis.boring_codecs", "")
        if raw_codecs:
            boring_codecs = []
            for entry in str(raw_codecs).split(","):
                entry = entry.strip()
                if ":" in entry:
                    vcodec, acodecs_str = entry.split(":", 1)
                    boring_codecs.append((vcodec.strip(), [a.strip() for a in acodecs_str.split("+")]))
            if not boring_codecs:
                boring_codecs = None

        files = find_boring_files(db, boring_codecs=boring_codecs)
        if not files:
            console.print("[green]No boring files found.[/green]")
            return

        table = Table(title="Boring Files")
        table.add_column("Name", style="cyan", max_width=40)
        table.add_column("Size", justify="right")
        table.add_column("Resolution")
        table.add_column("Path", style="dim", max_width=60)
        for f in files:
            res = f"{f['width']}x{f['height']}" if f.get("width") and f.get("height") else ""
            table.add_row(f["file_name"], _format_size(f["file_size"]), res, f["file_path"])
        console.print(table)
        total_size = sum(f["file_size"] for f in files)
        console.print(f"[dim]{len(files)} boring file(s), {_format_size(total_size)} total[/dim]")

        if save_plan:
            plan = generate_cleanup_plan(files, reason="boring")
            save_cleanup_plan(plan, Path(save_plan))
            console.print(f"[green]Cleanup plan saved to {save_plan} ({len(plan)} entries)[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------

@app.command()
def clean(
    plan: str = typer.Option(..., "--plan", help="Path to a cleanup plan JSON file."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would happen without executing."),
    force: bool = typer.Option(False, "--force", help="Skip confirmation."),
) -> None:
    """Execute a cleanup plan — move files to trash (never deletes directly)."""
    import json as json_mod
    from avshelf.analysis import execute_cleanup

    plan_path = Path(plan)
    if not plan_path.exists():
        console.print(f"[red]Plan file not found: {plan}[/red]")
        raise typer.Exit(1)

    plan_data = json_mod.loads(plan_path.read_text(encoding="utf-8"))
    total_size = sum(e.get("file_size", 0) for e in plan_data)

    console.print(f"Files to clean: {len(plan_data)}")
    console.print(f"Total size: {_format_size(total_size)}")

    if dry_run:
        console.print("[yellow]Dry run — no files will be moved.[/yellow]")

    if not dry_run and not force:
        confirm = typer.prompt("Type 'yes' to move files to trash")
        if confirm != "yes":
            console.print("[yellow]Cancelled.[/yellow]")
            return

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        stats = execute_cleanup(plan_data, db, dry_run=dry_run)
        prefix = "[DRY RUN] " if dry_run else ""
        console.print(f"[green]{prefix}Moved: {stats['moved']}, Skipped: {stats['skipped']}, Errors: {stats['errors']}[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# ask (natural language search)
# ---------------------------------------------------------------------------

@app.command()
def ask(
    query: str = typer.Argument(..., help="Natural language query (e.g. 'find HDR videos with multiple audio tracks')."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation and execute immediately."),
) -> None:
    """Search media files using natural language (requires LLM configuration)."""
    from avshelf.nlq import parse_natural_language, execute_parsed_query

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        import json as json_mod

        # Step 1: Parse natural language into structured query via LLM
        try:
            parsed = parse_natural_language(query, cfg)
        except ValueError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(1)

        # Step 2: Show parsed conditions and ask for confirmation
        console.print("[bold]Parsed query conditions:[/bold]")
        console.print(json_mod.dumps(parsed, indent=2))
        console.print()

        if not yes:
            action = typer.prompt(
                "Execute this query? [y]es / [e]dit / [c]ancel",
                default="y",
            ).strip().lower()

            if action in ("c", "cancel"):
                console.print("[yellow]Cancelled.[/yellow]")
                return
            elif action in ("e", "edit"):
                # Let user edit the JSON query directly
                edited = typer.prompt("Enter corrected JSON query")
                try:
                    parsed = json_mod.loads(edited)
                except json_mod.JSONDecodeError as e:
                    console.print(f"[red]Invalid JSON: {e}[/red]")
                    raise typer.Exit(1)
                console.print(f"[dim]Using edited query: {json_mod.dumps(parsed)}[/dim]\n")

        # Step 3: Execute the (possibly edited) query
        results = execute_parsed_query(parsed, db)
        _print_search_table(results)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# deep-scan commands
# ---------------------------------------------------------------------------

@deep_scan_app.command("run")
def deep_scan_run(
    file: Optional[str] = typer.Argument(None, help="File path to deep scan."),
    query_filter: Optional[str] = typer.Option(None, "--query", help="Search query to select files (e.g. '--vcodec hevc')."),
    frames: int = typer.Option(10, "--frames", help="Number of frames to decode."),
    ffmpeg: Optional[str] = typer.Option(None, "--ffmpeg", help="Path to ffmpeg binary."),
    decode_params: Optional[str] = typer.Option(None, "--decode-params", help="Extra decode parameters."),
    description: Optional[str] = typer.Option(None, "--description", help="Description for this scan run."),
    threads: int = typer.Option(1, "--threads", "-j", help="Number of parallel ffmpeg workers (default 1)."),
) -> None:
    """Run deep scan on a file or a set of files (frame-level MD5 collection)."""

    from avshelf.deep_scan import run_deep_scan

    cfg = _get_config()
    db = _get_db(cfg)
    ffmpeg_path = ffmpeg or cfg.ffmpeg_path

    try:
        if file:
            resolved = str(Path(file).resolve())
            file_paths = [resolved]
        elif query_filter:
            # Parse simple --vcodec style filter from the query string
            file_paths = _resolve_query_to_paths(query_filter, db)
            if not file_paths:
                console.print("[yellow]No files matched the query.[/yellow]")
                return
        else:
            console.print("[red]Provide a file path or --query to select files.[/red]")
            raise typer.Exit(1)

        effective_threads = max(1, threads)
        if effective_threads > 1:
            console.print(f"Deep scanning {len(file_paths)} file(s), {frames} frames each, {effective_threads} threads...")
        else:
            console.print(f"Deep scanning {len(file_paths)} file(s), {frames} frames each...")
        result = run_deep_scan(
            db, file_paths,
            ffmpeg_path=ffmpeg_path,
            frames=frames,
            decode_params=decode_params,
            description=description,
            threads=effective_threads,
        )
        console.print(f"\n[bold green]Deep scan complete![/bold green]")
        console.print(f"  Scan ID:   {result.scan_id}")
        console.print(f"  Processed: {result.files_processed}")
        console.print(f"  Errors:    {result.files_errored}")
        console.print(f"  Frames:    {result.total_frames}")
    finally:
        db.close()


def _parse_query_string(query_str: str) -> tuple[list[str], list[Any]]:
    """Parse a simple query string like '--vcodec hevc' into SQL conditions and params."""
    import shlex
    parts = shlex.split(query_str)
    conditions: list[str] = []
    params: list[Any] = []
    i = 0
    while i < len(parts):
        if parts[i] == "--vcodec" and i + 1 < len(parts):
            conditions.append("video_codec = ?")
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--acodec" and i + 1 < len(parts):
            conditions.append("audio_codec = ?")
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--type" and i + 1 < len(parts):
            conditions.append("media_type = ?")
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--format" and i + 1 < len(parts):
            conditions.append("format_name LIKE ?")
            params.append(f"%{parts[i + 1]}%")
            i += 2
        elif parts[i] == "--has-hdr":
            conditions.append("has_hdr = 1")
            i += 1
        elif parts[i] == "--tag" and i + 1 < len(parts):
            conditions.append(
                "id IN (SELECT mt.media_id FROM media_tags mt "
                "JOIN tags t ON t.id = mt.tag_id WHERE t.name = ?)"
            )
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--category" and i + 1 < len(parts):
            conditions.append(
                "id IN (SELECT mc.media_id FROM media_categories mc "
                "JOIN categories c ON c.id = mc.category_id WHERE c.name = ?)"
            )
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--min-size" and i + 1 < len(parts):
            conditions.append("file_size >= ?")
            params.append(_parse_size(parts[i + 1]))
            i += 2
        elif parts[i] == "--max-size" and i + 1 < len(parts):
            conditions.append("file_size <= ?")
            params.append(_parse_size(parts[i + 1]))
            i += 2
        elif parts[i] == "--audio-tracks" and i + 1 < len(parts):
            op, val = _parse_comparison(parts[i + 1])
            conditions.append(f"audio_track_count {op} ?")
            params.append(val)
            i += 2
        elif parts[i] == "--has-rotation":
            conditions.append("rotation IS NOT NULL AND rotation != 0")
            i += 1
        elif parts[i] == "--has-subtitle":
            conditions.append("subtitle_track_count > 0")
            i += 1
        elif parts[i] == "--has-multi-audio":
            conditions.append("audio_track_count > 1")
            i += 1
        elif parts[i] == "--min-duration" and i + 1 < len(parts):
            conditions.append("duration >= ?")
            params.append(float(parts[i + 1]))
            i += 2
        elif parts[i] == "--max-duration" and i + 1 < len(parts):
            conditions.append("duration <= ?")
            params.append(float(parts[i + 1]))
            i += 2
        elif parts[i] == "--min-width" and i + 1 < len(parts):
            conditions.append("width >= ?")
            params.append(int(parts[i + 1]))
            i += 2
        elif parts[i] == "--max-width" and i + 1 < len(parts):
            conditions.append("width <= ?")
            params.append(int(parts[i + 1]))
            i += 2
        elif parts[i] == "--min-height" and i + 1 < len(parts):
            conditions.append("height >= ?")
            params.append(int(parts[i + 1]))
            i += 2
        elif parts[i] == "--max-height" and i + 1 < len(parts):
            conditions.append("height <= ?")
            params.append(int(parts[i + 1]))
            i += 2
        elif parts[i] == "--res" and i + 1 < len(parts):
            res_parts = parts[i + 1].lower().split("x")
            if len(res_parts) == 2:
                conditions.append("width = ? AND height = ?")
                params.extend([int(res_parts[0]), int(res_parts[1])])
            i += 2
        elif parts[i] == "--pixel-format" and i + 1 < len(parts):
            conditions.append("pixel_format = ?")
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--bit-depth" and i + 1 < len(parts):
            conditions.append("bit_depth = ?")
            params.append(int(parts[i + 1]))
            i += 2
        elif parts[i] == "--profile" and i + 1 < len(parts):
            conditions.append("video_profile = ?")
            params.append(parts[i + 1])
            i += 2
        elif parts[i] == "--has-hdr":
            conditions.append("has_hdr = 1")
            i += 1
        elif parts[i] == "--no-hdr":
            conditions.append("has_hdr = 0")
            i += 1
        elif parts[i] == "--has-error":
            conditions.append("has_error = 1")
            i += 1
        elif parts[i] == "--no-error":
            conditions.append("has_error = 0")
            i += 1
        elif parts[i] == "--interlaced":
            conditions.append("field_order IS NOT NULL AND field_order != 'progressive'")
            i += 1
        elif parts[i] == "--no-interlaced":
            conditions.append("(field_order IS NULL OR field_order = 'progressive')")
            i += 1
        elif parts[i] == "--has-chapters":
            conditions.append("chapter_count > 0")
            i += 1
        elif parts[i] == "--no-chapters":
            conditions.append("chapter_count = 0")
            i += 1
        else:
            i += 1
    return conditions, params


def _resolve_query_to_paths(query_str: str, db: Database) -> list[str]:
    """Parse a simple query string like '--vcodec hevc' into file paths."""
    conditions, params = _parse_query_string(query_str)
    rows = db.query_media(conditions, params)
    return [r["file_path"] for r in rows]


@deep_scan_app.command("list")
def deep_scan_list() -> None:
    """List all deep scan records."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        scans = db.list_deep_scans()
        if not scans:
            console.print("[dim]No deep scans found.[/dim]")
            return
        table = Table(title="Deep Scans")
        table.add_column("ID", justify="right")
        table.add_column("Time")
        table.add_column("FFmpeg Version", max_width=40)
        table.add_column("Files", justify="right")
        table.add_column("Frames", justify="right")
        table.add_column("Description", max_width=30)
        for s in scans:
            table.add_row(
                str(s["id"]),
                s["scan_time"][:19],
                s.get("ffmpeg_version", "")[:40],
                str(s.get("file_count", 0)),
                str(s["frame_count"]),
                s.get("description") or "",
            )
        console.print(table)
    finally:
        db.close()


@deep_scan_app.command("show")
def deep_scan_show(
    file: str = typer.Argument(..., help="File path to show deep scan results for."),
    scan_id: Optional[int] = typer.Option(None, "--scan-id", help="Specific scan ID (latest if not specified)."),
) -> None:
    """Show frame-level MD5 results for a file."""
    cfg = _get_config()
    db = _get_db(cfg)
    try:
        resolved = str(Path(file).resolve())
        media = db.get_media_by_path(resolved)
        if not media:
            console.print(f"[red]File not in database: {file}[/red]")
            raise typer.Exit(1)

        if scan_id is None:
            scans = db.list_deep_scans()
            if not scans:
                console.print("[dim]No deep scans found.[/dim]")
                return
            scan_id = scans[0]["id"]

        results = db.get_deep_scan_results(scan_id, media_id=media["id"])
        if not results:
            console.print(f"[dim]No results for scan {scan_id} / {file}[/dim]")
            return

        table = Table(title=f"Frame MD5 — Scan {scan_id}")
        table.add_column("Frame", justify="right")
        table.add_column("MD5", style="cyan")
        table.add_column("Status")
        for r in results:
            status_style = "green" if r["status"] == "success" else "red"
            table.add_row(
                str(r["frame_index"]),
                r.get("frame_md5") or "",
                f"[{status_style}]{r['status']}[/{status_style}]",
            )
        console.print(table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

@app.command()
def verify(
    baseline: int = typer.Option(..., "--baseline", help="Baseline deep scan ID."),
    query_filter: Optional[str] = typer.Option(None, "--query", help="Search query to select files (overrides baseline file list)."),
    ffmpeg: Optional[str] = typer.Option(None, "--ffmpeg", help="Path to the new ffmpeg binary."),
    frames: int = typer.Option(10, "--frames", help="Number of frames to decode."),
    decode_params: Optional[str] = typer.Option(None, "--decode-params"),
    threads: int = typer.Option(1, "--threads", "-j", help="Number of parallel ffmpeg workers (default 1)."),
) -> None:
    """Verify decode correctness by comparing frame MD5s against a baseline scan."""
    from avshelf.deep_scan import run_deep_scan, verify_against_baseline

    cfg = _get_config()
    db = _get_db(cfg)
    ffmpeg_path = ffmpeg or cfg.ffmpeg_path

    try:
        if query_filter:
            # Use --query to select files for verification
            file_paths = _resolve_query_to_paths(query_filter, db)
            if not file_paths:
                console.print("[yellow]No files matched the query.[/yellow]")
                return
        else:
            # Determine which files to verify from the baseline scan
            baseline_results = db.get_deep_scan_results(baseline)
            if not baseline_results:
                console.print(f"[red]No results found for baseline scan {baseline}.[/red]")
                raise typer.Exit(1)

            media_ids = sorted(set(r["media_id"] for r in baseline_results))
            file_paths = []
            for mid in media_ids:
                media = db.get_media_by_id(mid)
                if media and Path(media["file_path"]).exists():
                    file_paths.append(media["file_path"])

        if not file_paths:
            console.print("[red]No valid files found for verification.[/red]")
            raise typer.Exit(1)

        console.print(f"Verifying {len(file_paths)} file(s) against baseline scan {baseline}...")
        new_result = run_deep_scan(
            db, file_paths,
            ffmpeg_path=ffmpeg_path,
            frames=frames,
            decode_params=decode_params,
            description=f"Verification against baseline {baseline}",
            threads=max(1, threads),
        )

        verify_result = verify_against_baseline(db, baseline, new_result.scan_id)

        console.print(f"\n[bold]Verification Results:[/bold]")
        console.print(f"  Total files: {verify_result.total_files}")
        console.print(f"  [green]Passed: {verify_result.passed_files}[/green]")
        console.print(f"  [red]Failed: {verify_result.failed_files}[/red]")
        console.print(f"  [yellow]Errors: {verify_result.error_files}[/yellow]")

        if verify_result.failures:
            console.print("\n[bold red]Failures:[/bold red]")
            fail_table = Table(show_lines=False)
            fail_table.add_column("File", style="cyan", max_width=60)
            fail_table.add_column("Reason", style="red")
            fail_table.add_column("Frame", justify="right")
            fail_table.add_column("Mismatch/Total", justify="right")
            for f in verify_result.failures:
                reason = f['reason']
                frame_str = str(f.get('first_mismatch_frame', ''))
                mismatch_str = ""
                if 'mismatch_count' in f:
                    mismatch_str = f"{f['mismatch_count']}/{f.get('total_compared', '?')}"
                fail_table.add_row(
                    f['file_path'],
                    reason,
                    frame_str,
                    mismatch_str,
                )
            console.print(fail_table)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

@app.command("diff")
def diff_cmd(
    dir_a: str = typer.Argument(..., help="First directory."),
    dir_b: str = typer.Argument(..., help="Second directory."),
    by: str = typer.Option("name", "--by", help="Compare by: name, hash."),
) -> None:
    """Compare two directories and show differences."""
    from avshelf.sync import diff_directories

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        result = diff_directories(db, dir_a, dir_b, by=by)

        if result["only_a"]:
            console.print(f"\n[bold cyan]Only in {dir_a}:[/bold cyan] ({len(result['only_a'])})")
            for f in result["only_a"]:
                console.print(f"  {f['relative_path']}")

        if result["only_b"]:
            console.print(f"\n[bold cyan]Only in {dir_b}:[/bold cyan] ({len(result['only_b'])})")
            for f in result["only_b"]:
                console.print(f"  {f['relative_path']}")

        if result["different"]:
            console.print(f"\n[bold yellow]Different:[/bold yellow] ({len(result['different'])})")
            for f in result["different"]:
                console.print(f"  {f['relative_path']}")

        if result["same"]:
            console.print(f"\n[bold green]Same:[/bold green] ({len(result['same'])})")

        console.print(f"\nSummary: {len(result['only_a'])} only-A, {len(result['only_b'])} only-B, "
                       f"{len(result['different'])} different, {len(result['same'])} same")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------

@app.command()
def merge(
    source: str = typer.Argument(..., help="Source directory."),
    target: str = typer.Argument(..., help="Target directory."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would happen without copying."),
    on_conflict: str = typer.Option("skip", "--on-conflict", help="Conflict resolution: skip, overwrite, keep-both."),
    force: bool = typer.Option(False, "--force", help="Skip confirmation."),
) -> None:
    """Merge source directory into target (copy missing files)."""
    from avshelf.sync import merge_directories

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        if not dry_run and not force:
            confirm = typer.prompt(f"Merge {source} → {target}? Type 'yes' to proceed")
            if confirm != "yes":
                console.print("[yellow]Cancelled.[/yellow]")
                return

        stats = merge_directories(source, target, db, dry_run=dry_run, on_conflict=on_conflict)
        prefix = "[DRY RUN] " if dry_run else ""
        console.print(f"[green]{prefix}Copied: {stats['copied']}, Skipped: {stats['skipped']}, "
                       f"Conflicts: {stats['conflicts']}, Errors: {stats['errors']}[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# export / import
# ---------------------------------------------------------------------------

@app.command("export")
def export_cmd(
    output: str = typer.Option("avshelf_export.json", "--output", help="Output file path."),
) -> None:
    """Export the media database to a JSON file."""
    from avshelf.sync import export_database

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        count = export_database(db, Path(output))
        console.print(f"[green]Exported {count} record(s) to {output}[/green]")
    finally:
        db.close()


@app.command("import")
def import_cmd(
    file: str = typer.Argument(..., help="JSON export file to import."),
) -> None:
    """Import media records from a JSON export file."""
    from avshelf.sync import import_database

    cfg = _get_config()
    db = _get_db(cfg)
    try:
        stats = import_database(db, Path(file))
        console.print(f"[green]Imported: {stats['imported']}, Merged: {stats['merged']}, "
                       f"Skipped: {stats['skipped']}[/green]")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# mcp
# ---------------------------------------------------------------------------

@app.command()
def mcp() -> None:
    """Start the MCP server for AI assistant integration."""
    import sys
    from avshelf.mcp_server import run_server

    cfg = _get_config()
    if not cfg.db_path.exists():
        print("Database not found. Run 'avshelf scan' first to index media files.", file=sys.stderr)
        raise typer.Exit(1)

    # Do NOT write anything to stdout — MCP stdio transport uses stdout for JSON-RPC.
    run_server()
