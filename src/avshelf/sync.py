"""Sync tools — export, import, diff, and merge media directories."""

from __future__ import annotations

import json
import platform
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from avshelf.database import Database


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Export / Import
# ---------------------------------------------------------------------------

def export_database(db: Database, output_path: Path) -> int:
    """Export all media records to a JSON file.

    Returns the number of records exported.
    """
    rows = db.conn.execute(
        "SELECT * FROM media_files WHERE deleted_at IS NULL"
    ).fetchall()
    records = [dict(r) for r in rows]

    # Collect tags and categories for each record
    for rec in records:
        rec["_tags"] = db.get_tags_for_media(rec["id"])
        rec["_categories"] = db.get_categories_for_media(rec["id"])
        # Remove raw_metadata to keep export size manageable
        rec.pop("raw_metadata", None)

    export_data = {
        "avshelf_version": "0.1.0",
        "export_time": _now_iso(),
        "source_device": platform.node(),
        "record_count": len(records),
        "records": records,
    }

    output_path.write_text(
        json.dumps(export_data, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return len(records)


def import_database(db: Database, input_path: Path) -> dict[str, int]:
    """Import media records from a JSON export file.

    Merges by file_hash: if a record with the same hash exists, it's
    updated with any new tags/categories but not duplicated.

    Returns stats: {imported, merged, skipped}.
    """
    data = json.loads(input_path.read_text(encoding="utf-8"))
    records = data.get("records", [])

    stats = {"imported": 0, "merged": 0, "skipped": 0}

    for rec in records:
        tags = rec.pop("_tags", [])
        categories = rec.pop("_categories", [])
        rec.pop("id", None)

        file_hash = rec.get("file_hash")
        existing = None

        # Try to match by hash first, then by path
        if file_hash:
            existing_row = db.conn.execute(
                "SELECT * FROM media_files WHERE file_hash = ? AND deleted_at IS NULL",
                (file_hash,),
            ).fetchone()
            if existing_row:
                existing = dict(existing_row)

        if not existing:
            existing_row = db.conn.execute(
                "SELECT * FROM media_files WHERE file_path = ? AND deleted_at IS NULL",
                (rec.get("file_path", ""),),
            ).fetchone()
            if existing_row:
                existing = dict(existing_row)

        if existing:
            # Merge tags and categories
            media_id = existing["id"]
            if tags:
                db.add_tags_to_media(media_id, tags)
            for cat in categories:
                db.add_category_to_media(media_id, cat)
            stats["merged"] += 1
        else:
            media_id = db.upsert_media(rec)
            if tags:
                db.add_tags_to_media(media_id, tags)
            for cat in categories:
                db.add_category_to_media(media_id, cat)
            stats["imported"] += 1

    return stats


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

def diff_directories(
    db: Database,
    dir_a: str,
    dir_b: str,
    by: str = "name",
) -> dict[str, list[dict]]:
    """Compare two directories and return their differences.

    Args:
        by: 'name' for filename comparison, 'hash' for content hash comparison.

    Returns dict with keys: only_a, only_b, same, different.
    """
    files_a = _collect_dir_files(Path(dir_a))
    files_b = _collect_dir_files(Path(dir_b))

    result: dict[str, list[dict]] = {
        "only_a": [],
        "only_b": [],
        "same": [],
        "different": [],
    }

    if by == "hash":
        _diff_by_hash(files_a, files_b, db, result)
    else:
        _diff_by_name(files_a, files_b, db, result)

    return result


def _collect_dir_files(directory: Path) -> dict[str, Path]:
    """Collect all files in a directory, keyed by relative path."""
    files: dict[str, Path] = {}
    if not directory.is_dir():
        return files
    for entry in directory.rglob("*"):
        if entry.is_file():
            rel = str(entry.relative_to(directory))
            files[rel] = entry
    return files


def _diff_by_name(
    files_a: dict[str, Path],
    files_b: dict[str, Path],
    db: Database,
    result: dict[str, list[dict]],
) -> None:
    """Compare directories by filename."""
    all_keys = set(files_a.keys()) | set(files_b.keys())

    for key in sorted(all_keys):
        in_a = key in files_a
        in_b = key in files_b

        if in_a and not in_b:
            result["only_a"].append({"relative_path": key, "path": str(files_a[key])})
        elif in_b and not in_a:
            result["only_b"].append({"relative_path": key, "path": str(files_b[key])})
        else:
            # Both exist — compare by size
            size_a = files_a[key].stat().st_size
            size_b = files_b[key].stat().st_size
            entry = {
                "relative_path": key,
                "path_a": str(files_a[key]),
                "path_b": str(files_b[key]),
                "size_a": size_a,
                "size_b": size_b,
            }
            if size_a == size_b:
                result["same"].append(entry)
            else:
                result["different"].append(entry)


def _diff_by_hash(
    files_a: dict[str, Path],
    files_b: dict[str, Path],
    db: Database,
    result: dict[str, list[dict]],
) -> None:
    """Compare directories by content hash from the database."""
    from avshelf.probe import compute_fast_hash

    def _get_hash(path: Path) -> str:
        rec = db.get_media_by_path(str(path.resolve()))
        if rec and rec.get("file_hash"):
            return rec["file_hash"]
        return compute_fast_hash(str(path))

    hashes_a: dict[str, str] = {}
    for key, path in files_a.items():
        hashes_a[key] = _get_hash(path)

    hashes_b: dict[str, str] = {}
    for key, path in files_b.items():
        hashes_b[key] = _get_hash(path)

    hash_to_key_a = {h: k for k, h in hashes_a.items()}
    hash_to_key_b = {h: k for k, h in hashes_b.items()}

    all_hashes_a = set(hashes_a.values())
    all_hashes_b = set(hashes_b.values())

    for key, h in hashes_a.items():
        if h not in all_hashes_b:
            result["only_a"].append({"relative_path": key, "path": str(files_a[key]), "hash": h})

    for key, h in hashes_b.items():
        if h not in all_hashes_a:
            result["only_b"].append({"relative_path": key, "path": str(files_b[key]), "hash": h})

    for key in sorted(set(files_a.keys()) & set(files_b.keys())):
        h_a = hashes_a.get(key, "")
        h_b = hashes_b.get(key, "")
        entry = {
            "relative_path": key,
            "path_a": str(files_a[key]),
            "path_b": str(files_b[key]),
        }
        if h_a == h_b:
            result["same"].append(entry)
        else:
            result["different"].append(entry)


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def merge_directories(
    source: str,
    target: str,
    db: Database,
    *,
    dry_run: bool = False,
    on_conflict: str = "skip",
) -> dict[str, int]:
    """Merge source directory into target directory.

    Copies files from source that are missing in target.

    Args:
        on_conflict: 'skip', 'overwrite', or 'keep-both'.
        dry_run: if True, only report what would happen.

    Returns stats: {copied, skipped, conflicts, errors}.
    """
    src_path = Path(source)
    tgt_path = Path(target)

    if not src_path.is_dir():
        raise FileNotFoundError(f"Source directory not found: {source}")
    tgt_path.mkdir(parents=True, exist_ok=True)

    src_files = _collect_dir_files(src_path)
    tgt_files = _collect_dir_files(tgt_path)

    stats = {"copied": 0, "skipped": 0, "conflicts": 0, "errors": 0}

    for rel_path, src_file in sorted(src_files.items()):
        dest_file = tgt_path / rel_path

        if rel_path not in tgt_files:
            if not dry_run:
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.copy2(str(src_file), str(dest_file))
                except OSError:
                    stats["errors"] += 1
                    continue
            stats["copied"] += 1
        else:
            # Conflict: file exists in both
            src_size = src_file.stat().st_size
            tgt_size = tgt_files[rel_path].stat().st_size

            if src_size == tgt_size:
                stats["skipped"] += 1
                continue

            stats["conflicts"] += 1

            if on_conflict == "skip":
                stats["skipped"] += 1
            elif on_conflict == "overwrite":
                if not dry_run:
                    try:
                        shutil.copy2(str(src_file), str(dest_file))
                    except OSError:
                        stats["errors"] += 1
                        continue
                stats["copied"] += 1
            elif on_conflict == "keep-both":
                stem = dest_file.stem
                suffix = dest_file.suffix
                new_name = f"{stem}_from_source{suffix}"
                new_dest = dest_file.parent / new_name
                counter = 1
                while new_dest.exists():
                    new_name = f"{stem}_from_source_{counter}{suffix}"
                    new_dest = dest_file.parent / new_name
                    counter += 1
                if not dry_run:
                    try:
                        shutil.copy2(str(src_file), str(new_dest))
                    except OSError:
                        stats["errors"] += 1
                        continue
                stats["copied"] += 1

    return stats
