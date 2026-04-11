"""AVShelf CLI — command-line interface for media asset management."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

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
            rows = db.conn.execute(
                "SELECT DISTINCT scan_source_dir FROM media_files WHERE deleted_at IS NULL"
            ).fetchall()
            dirs_to_refresh = [r["scan_source_dir"] for r in rows]

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


def _parse_size(size_str: str) -> int:
    """Parse a human-readable size string like '100MB' into bytes."""
    size_str = size_str.strip().upper()
    multipliers = {"KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    for suffix, mult in multipliers.items():
        if size_str.endswith(suffix):
            return int(float(size_str[:-len(suffix)]) * mult)
    return int(size_str)


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

        # Raw metadata (truncated)
        if record.get("raw_metadata"):
            raw = json_mod.loads(record["raw_metadata"])
            console.print("\n[bold]Raw ffprobe metadata:[/bold]")
            console.print(json_mod.dumps(raw, indent=2, ensure_ascii=False)[:5000])
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


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Config key (dotted path, e.g. llm.provider)."),
    value: str = typer.Argument(..., help="Value to set."),
) -> None:
    """Set a configuration value."""
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
            # Batch mode: not yet implemented, placeholder
            console.print("[yellow]Batch tagging via --query not yet implemented.[/yellow]")
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

@classify_app.command("set")
def classify_set(
    file: str = typer.Argument(..., help="File path."),
    category: str = typer.Option(..., "--category", help="Category name."),
) -> None:
    """Assign a category to a media file."""
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
        rows = db.conn.execute(
            "SELECT media_type, COUNT(*) as cnt FROM media_files "
            "WHERE deleted_at IS NULL GROUP BY media_type ORDER BY cnt DESC"
        ).fetchall()
        if rows:
            table = Table(title="By Type")
            table.add_column("Type", style="cyan")
            table.add_column("Count", justify="right")
            for r in rows:
                table.add_row(r["media_type"] or "unknown", str(r["cnt"]))
            console.print(table)

        # Top codecs
        for label, col in [("Video Codecs", "video_codec"), ("Audio Codecs", "audio_codec")]:
            rows = db.conn.execute(
                f"SELECT {col}, COUNT(*) as cnt FROM media_files "
                f"WHERE deleted_at IS NULL AND {col} IS NOT NULL "
                f"GROUP BY {col} ORDER BY cnt DESC LIMIT 10"
            ).fetchall()
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

    meta = [e for e in meta if e.get("original_path") != resolved]
    meta_file.write_text(json_mod.dumps(meta, indent=2), encoding="utf-8")

    console.print(f"[green]Restored: {dest}[/green]")
