"""Analysis tools — dedup, similar, space, cold, boring, clean."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from avshelf.config import LOGS_DIR, TRASH_DIR
from avshelf.database import Database


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_operation(operation: str, entries: list[dict]) -> None:
    """Append an operation record to the daily log file."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = LOGS_DIR / f"{today}.jsonl"
    with open(log_file, "a", encoding="utf-8") as f:
        for entry in entries:
            record = {
                "time": _now_iso(),
                "operation": operation,
                **entry,
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

@dataclass
class DedupGroup:
    hash_value: str
    files: list[dict]

    @property
    def wasted_bytes(self) -> int:
        if len(self.files) <= 1:
            return 0
        return sum(f["file_size"] for f in self.files[1:])


def find_duplicates(db: Database, *, fast: bool = False) -> list[DedupGroup]:
    """Find duplicate files by content hash.

    When fast=True, uses fast_hash (head+tail sampling) for pre-screening.
    When fast=False, uses full file_hash (SHA256).
    """
    hash_col = "fast_hash" if fast else "file_hash"

    rows = db.conn.execute(
        f"SELECT {hash_col} as h, COUNT(*) as cnt FROM media_files "
        f"WHERE deleted_at IS NULL AND {hash_col} IS NOT NULL "
        f"GROUP BY {hash_col} HAVING cnt > 1 "
        f"ORDER BY cnt DESC"
    ).fetchall()

    groups: list[DedupGroup] = []
    for row in rows:
        files = db.conn.execute(
            f"SELECT * FROM media_files WHERE {hash_col} = ? AND deleted_at IS NULL "
            f"ORDER BY file_path",
            (row["h"],),
        ).fetchall()
        groups.append(DedupGroup(
            hash_value=row["h"],
            files=[dict(f) for f in files],
        ))

    return groups


# ---------------------------------------------------------------------------
# Similar files
# ---------------------------------------------------------------------------

@dataclass
class SimilarGroup:
    key: str
    files: list[dict]


def find_similar(db: Database, duration_tolerance: float = 0.05,
                 size_tolerance: float = 0.10) -> list[SimilarGroup]:
    """Find similar files based on metadata features.

    First groups files by (video_codec, width, height), then compares
    duration/size only within each group.  This avoids O(n²) full-scan
    and reduces to O(n) + O(sum of group_size²) which is much faster
    when files have diverse codecs/resolutions.
    """
    from collections import defaultdict

    rows = db.conn.execute(
        "SELECT * FROM media_files WHERE deleted_at IS NULL AND media_type = 'video' "
        "ORDER BY video_codec, width, height, duration"
    ).fetchall()

    # Phase 1: bucket by (codec, width, height) — O(n)
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        d = dict(r)
        key = (d.get("video_codec"), d.get("width"), d.get("height"))
        buckets[key].append(d)

    # Phase 2: within each bucket, cluster by duration/size similarity
    groups: list[SimilarGroup] = []
    for (codec, w, h), bucket_files in buckets.items():
        if len(bucket_files) < 2:
            continue

        used: set[int] = set()
        for i, f1 in enumerate(bucket_files):
            if f1["id"] in used:
                continue
            cluster = [f1]
            for j in range(i + 1, len(bucket_files)):
                f2 = bucket_files[j]
                if f2["id"] in used:
                    continue
                if _is_duration_size_similar(f1, f2, duration_tolerance, size_tolerance):
                    cluster.append(f2)
                    used.add(f2["id"])

            if len(cluster) > 1:
                used.add(f1["id"])
                key_str = f"{codec or '?'}_{w or '?'}x{h or '?'}"
                groups.append(SimilarGroup(key=key_str, files=cluster))

    return groups


def _is_duration_size_similar(a: dict, b: dict, dur_tol: float, size_tol: float) -> bool:
    """Check if two files (already sharing codec+resolution) have similar duration/size."""
    dur_a, dur_b = a.get("duration"), b.get("duration")
    if dur_a and dur_b and dur_a > 0:
        if abs(dur_a - dur_b) / dur_a > dur_tol:
            return False

    size_a, size_b = a.get("file_size", 0), b.get("file_size", 0)
    if size_a > 0:
        if abs(size_a - size_b) / size_a > size_tol:
            return False

    return True


# ---------------------------------------------------------------------------
# Space analysis
# ---------------------------------------------------------------------------

def analyze_space(db: Database, top_n: int = 20) -> dict[str, Any]:
    """Analyze disk space usage.

    Returns top files by size and per-directory breakdown.
    """
    top_files = db.conn.execute(
        "SELECT file_path, file_name, file_size, media_type, video_codec "
        "FROM media_files WHERE deleted_at IS NULL "
        "ORDER BY file_size DESC LIMIT ?",
        (top_n,),
    ).fetchall()

    dir_stats = db.conn.execute(
        "SELECT scan_source_dir, COUNT(*) as cnt, SUM(file_size) as total_size "
        "FROM media_files WHERE deleted_at IS NULL "
        "GROUP BY scan_source_dir ORDER BY total_size DESC"
    ).fetchall()

    total_row = db.conn.execute(
        "SELECT COUNT(*) as cnt, SUM(file_size) as total_size "
        "FROM media_files WHERE deleted_at IS NULL"
    ).fetchone()

    return {
        "top_files": [dict(r) for r in top_files],
        "dir_stats": [dict(r) for r in dir_stats],
        "total_files": total_row["cnt"],
        "total_size": total_row["total_size"] or 0,
    }


# ---------------------------------------------------------------------------
# Cold files
# ---------------------------------------------------------------------------

def find_cold_files(db: Database, days: int = 180, by: str = "atime") -> list[dict]:
    """Find files not accessed (or not modified, if by='mtime') in the last N days.

    For media files, access time (atime) is a better coldness indicator than
    modification time (mtime), because media files are rarely modified after
    creation but may be frequently accessed (played).

    When atime tracking is disabled on the filesystem (e.g. mounted with
    noatime), atime equals mtime for all files. In that case this function
    falls back to mtime and returns a warning flag.

    Args:
        db: Open Database instance.
        days: Number of days threshold.
        by: 'atime' (default) or 'mtime'.

    Returns:
        List of cold file dicts. Each dict includes an extra 'cold_by' key
        indicating which timestamp was actually used, and 'atime_disabled'
        if atime appears to be unavailable.
    """
    import time

    if by not in ("atime", "mtime"):
        raise ValueError(f"Invalid by={by!r}, must be 'atime' or 'mtime'")

    cutoff = time.time() - (days * 86400)

    if by == "atime":
        # Use COALESCE to fall back to mtime when file_atime is NULL
        # (records created before the atime column was added)
        rows = db.conn.execute(
            "SELECT *, COALESCE(file_atime, file_mtime) AS cold_ts "
            "FROM media_files WHERE deleted_at IS NULL "
            "AND COALESCE(file_atime, file_mtime) < ? "
            "ORDER BY cold_ts ASC",
            (cutoff,),
        ).fetchall()
    else:
        rows = db.conn.execute(
            "SELECT *, file_mtime AS cold_ts "
            "FROM media_files WHERE deleted_at IS NULL AND file_mtime < ? "
            "ORDER BY file_mtime ASC",
            (cutoff,),
        ).fetchall()

    results = []
    atime_disabled_count = 0
    for r in rows:
        d = dict(r)
        d["cold_by"] = by
        # Detect atime disabled: atime equals mtime (within 1s tolerance)
        if by == "atime" and d.get("file_atime") is not None:
            if abs(d["file_atime"] - d["file_mtime"]) < 1.0:
                d["atime_disabled"] = True
                atime_disabled_count += 1
        results.append(d)

    # Attach a summary hint for callers
    if by == "atime" and atime_disabled_count > 0 and atime_disabled_count == len(results):
        results.insert(0, {"_atime_warning": True,
                           "message": f"All {atime_disabled_count} files have atime ≈ mtime. "
                                      "Your filesystem may have atime tracking disabled "
                                      "(mounted with noatime/relatime). Consider using --by mtime."})

    return results


# ---------------------------------------------------------------------------
# Boring files
# ---------------------------------------------------------------------------

DEFAULT_BORING_CODECS: list[tuple[str, list[str]]] = [
    # (video_codec, [audio_codecs])
    ("h264", ["aac"]),
    ("hevc", ["aac"]),
    ("h264", ["mp3"]),
]


def find_boring_files(
    db: Database,
    boring_codecs: list[tuple[str, list[str]]] | None = None,
) -> list[dict]:
    """Find files with unremarkable metadata features.

    Criteria: common codec combinations (configurable via *boring_codecs*),
    <=1080p, single audio track, no subtitles, no rotation, no HDR,
    no errors, no user tags.

    *boring_codecs* is a list of ``(video_codec, [audio_codecs])`` tuples.
    A file is "boring" if it matches ANY of these combinations.
    Defaults to ``DEFAULT_BORING_CODECS`` when *None*.
    """
    if boring_codecs is None:
        boring_codecs = DEFAULT_BORING_CODECS

    # Build OR conditions for codec combinations
    codec_clauses: list[str] = []
    params: list[str] = []
    for vcodec, acodecs in boring_codecs:
        acodec_placeholders = ", ".join("?" for _ in acodecs)
        codec_clauses.append(
            f"(mf.video_codec = ? AND (mf.audio_codec IN ({acodec_placeholders}) "
            f"OR mf.audio_codec IS NULL))"
        )
        params.append(vcodec)
        params.extend(acodecs)

    if not codec_clauses:
        return []

    codec_filter = " OR ".join(codec_clauses)

    rows = db.conn.execute(
        "SELECT mf.* FROM media_files mf "
        "WHERE mf.deleted_at IS NULL "
        "AND mf.media_type = 'video' "
        f"AND ({codec_filter}) "
        "AND (mf.height IS NULL OR mf.height <= 1080) "
        "AND mf.audio_track_count <= 1 "
        "AND mf.subtitle_track_count = 0 "
        "AND (mf.rotation IS NULL OR mf.rotation = 0) "
        "AND mf.has_hdr = 0 "
        "AND mf.has_error = 0 "
        "AND mf.id NOT IN (SELECT media_id FROM media_tags) "
        "ORDER BY mf.file_size DESC",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Cleanup plan generation and execution
# ---------------------------------------------------------------------------

def generate_cleanup_plan(files: list[dict], reason: str) -> list[dict]:
    """Generate a cleanup plan from a list of media file records."""
    plan = []
    for f in files:
        plan.append({
            "file_path": f["file_path"],
            "file_name": f.get("file_name", ""),
            "file_size": f.get("file_size", 0),
            "reason": reason,
        })
    return plan


def save_cleanup_plan(plan: list[dict], output_path: Path) -> None:
    """Save a cleanup plan to a JSON file."""
    output_path.write_text(
        json.dumps(plan, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def execute_cleanup(plan: list[dict], db: Database, *,
                    dry_run: bool = False) -> dict[str, int]:
    """Execute a cleanup plan by moving files to the trash.

    Never uses rm or os.remove — always moves to trash directory.
    Returns stats: {moved, skipped, errors}.
    """
    TRASH_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = TRASH_DIR / today
    day_dir.mkdir(parents=True, exist_ok=True)

    meta_file = TRASH_DIR / ".avshelf_trash_meta.json"
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
    else:
        meta = []

    stats = {"moved": 0, "skipped": 0, "errors": 0}
    log_entries: list[dict] = []

    for entry in plan:
        src = Path(entry["file_path"])
        if not src.exists():
            stats["skipped"] += 1
            continue

        if dry_run:
            stats["moved"] += 1
            continue

        # Preserve uniqueness by appending a counter if needed
        dest = day_dir / src.name
        counter = 1
        while dest.exists():
            dest = day_dir / f"{src.stem}_{counter}{src.suffix}"
            counter += 1

        try:
            shutil.move(str(src), str(dest))
            meta.append({
                "original_path": str(src.resolve()),
                "trash_path": str(dest),
                "file_size": entry.get("file_size", 0),
                "reason": entry.get("reason", ""),
                "trashed_at": _now_iso(),
            })
            log_entries.append({
                "file_path": str(src),
                "file_size": entry.get("file_size", 0),
                "trash_path": str(dest),
                "reason": entry.get("reason", ""),
            })
            db.soft_delete_media(str(src.resolve()))
            stats["moved"] += 1
        except OSError:
            stats["errors"] += 1

    if not dry_run:
        meta_file.write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        if log_entries:
            _log_operation("clean", log_entries)

    return stats
