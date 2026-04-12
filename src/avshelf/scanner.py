"""Directory scanner — walks a directory tree and indexes media files."""

from __future__ import annotations

import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from avshelf.config import Config
from avshelf.database import Database
from avshelf.probe import compute_file_hash, compute_fast_hash, extract_metadata


@dataclass
class ScanResult:
    """Accumulated statistics from a scan run."""
    added: int = 0
    updated: int = 0
    skipped: int = 0
    errored: int = 0
    by_type: dict[str, int] = field(default_factory=dict)

    @property
    def total_processed(self) -> int:
        return self.added + self.updated + self.errored


def _should_exclude(path: Path, exclude_patterns: list[str]) -> bool:
    """Check if any path component matches an exclude pattern."""
    parts = path.parts
    for pattern in exclude_patterns:
        if pattern in parts:
            return True
    return False


def _collect_candidates(
    directory: Path,
    extensions: set[str],
    exclude_patterns: list[str],
    probe_all: bool = False,
) -> list[Path]:
    """Walk a directory and collect candidate media files.

    When probe_all is False, only files whose extension is in the known
    set are returned.  When True, all files are returned (except excluded).
    """
    candidates: list[Path] = []
    for entry in directory.rglob("*"):
        if not entry.is_file():
            continue
        if _should_exclude(entry, exclude_patterns):
            continue
        if probe_all or entry.suffix.lower() in extensions:
            candidates.append(entry)
    return candidates


def scan_directory(
    directory: str | Path,
    db: Database,
    config: Config,
    *,
    full: bool = False,
    probe_all: bool = False,
) -> ScanResult:
    """Scan a directory, extract metadata, and store in the database.

    Args:
        directory: path to scan.
        db: open Database instance.
        config: application config.
        full: if True, re-scan all files regardless of mtime/size.
        probe_all: if True, probe files with any extension (not just known media).

    Returns:
        ScanResult with statistics.
    """
    dir_path = Path(directory).resolve()
    if not dir_path.is_dir():
        raise FileNotFoundError(f"Directory not found: {dir_path}")

    extensions = config.all_media_extensions()
    exclude = config.exclude_patterns
    candidates = _collect_candidates(dir_path, extensions, exclude, probe_all)

    result = ScanResult()
    interrupted = False

    def _handle_interrupt(signum: int, frame: Any) -> None:
        nonlocal interrupted
        interrupted = True

    prev_handler = signal.signal(signal.SIGINT, _handle_interrupt)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task("Scanning...", total=len(candidates))

            for file_path in candidates:
                if interrupted:
                    progress.console.print(
                        "\n[yellow]Interrupted — saving progress...[/yellow]"
                    )
                    break

                progress.update(task, description=f"[cyan]{file_path.name}")

                try:
                    _process_file(
                        file_path, dir_path, db, config, result, full=full
                    )
                except Exception as exc:
                    result.errored += 1

                progress.advance(task)
    finally:
        signal.signal(signal.SIGINT, prev_handler)

    # Record scan history
    db.record_scan(
        str(dir_path),
        added=result.added,
        updated=result.updated,
        deleted=0,
        errored=result.errored,
    )

    # Apply directory rules
    _apply_directory_rules(dir_path, db)

    return result


def _process_file(
    file_path: Path,
    scan_dir: Path,
    db: Database,
    config: Config,
    result: ScanResult,
    *,
    full: bool = False,
) -> None:
    """Process a single file: check if changed, extract metadata, upsert."""
    str_path = str(file_path.resolve())

    if not full:
        existing = db.get_media_by_path(str_path)
        if existing:
            stat = file_path.stat()
            if (existing["file_mtime"] == stat.st_mtime
                    and existing["file_size"] == stat.st_size):
                result.skipped += 1
                return

    meta = extract_metadata(str_path, config.ffprobe_path)
    meta["scan_source_dir"] = str(scan_dir)

    if meta.get("has_error"):
        result.errored += 1
        db.upsert_media(meta)
        return

    # Compute hashes
    try:
        meta["fast_hash"] = compute_fast_hash(str_path)
        # Full hash for files under 500MB; larger files get it on demand
        if file_path.stat().st_size < 500 * 1024 * 1024:
            meta["file_hash"] = compute_file_hash(str_path, config.hash_algorithm)
    except OSError:
        pass

    existing = db.get_media_by_path(str_path)
    db.upsert_media(meta)

    media_type = meta.get("media_type", "unknown")
    result.by_type[media_type] = result.by_type.get(media_type, 0) + 1

    if existing:
        result.updated += 1
    else:
        result.added += 1


def _apply_directory_rules(scan_dir: Path, db: Database) -> None:
    """Apply auto-tag and auto-category rules for the scanned directory.

    Each rule has a dir_path prefix.  A rule only applies to files whose
    file_path starts with that prefix, so sub-directory rules are respected.
    """
    rules = db.get_rules_for_dir(str(scan_dir))
    if not rules:
        return

    media_files = db.list_media_in_dir(str(scan_dir))
    for mf in media_files:
        file_path = mf.get("file_path", "")
        for rule in rules:
            # Only apply the rule when the file actually resides under
            # the rule's directory (prefix match).
            rule_dir = rule.get("dir_path", "")
            if rule_dir and not file_path.startswith(rule_dir):
                continue
            if rule["auto_tags"]:
                db.add_tags_to_media(mf["id"], rule["auto_tags"])
            if rule.get("auto_category"):
                db.add_category_to_media(mf["id"], rule["auto_category"])
