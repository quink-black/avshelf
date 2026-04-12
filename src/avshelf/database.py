"""SQLite database layer for AVShelf."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS media_files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path       TEXT    NOT NULL UNIQUE,
    file_name       TEXT    NOT NULL,
    file_size       INTEGER NOT NULL,
    file_mtime      REAL    NOT NULL,
    file_hash       TEXT,
    fast_hash       TEXT,
    media_type      TEXT,
    format_name     TEXT,
    format_long_name TEXT,
    duration        REAL,
    bit_rate        INTEGER,
    stream_count    INTEGER,
    video_codec     TEXT,
    video_profile   TEXT,
    video_level     TEXT,
    width           INTEGER,
    height          INTEGER,
    sar             TEXT,
    dar             TEXT,
    frame_rate      REAL,
    video_bit_rate  INTEGER,
    pixel_format    TEXT,
    bit_depth       INTEGER,
    color_space     TEXT,
    color_range     TEXT,
    color_transfer  TEXT,
    color_primaries TEXT,
    has_hdr         INTEGER DEFAULT 0,
    hdr_format      TEXT,
    rotation        INTEGER,
    field_order     TEXT,
    audio_codec     TEXT,
    audio_profile   TEXT,
    audio_sample_rate INTEGER,
    audio_channels  INTEGER,
    audio_channel_layout TEXT,
    audio_bit_rate  INTEGER,
    audio_bit_depth INTEGER,
    video_track_count  INTEGER DEFAULT 0,
    audio_track_count  INTEGER DEFAULT 0,
    subtitle_track_count INTEGER DEFAULT 0,
    chapter_count   INTEGER DEFAULT 0,
    has_error       INTEGER DEFAULT 0,
    error_message   TEXT,
    raw_metadata    TEXT,
    tags_json       TEXT,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    deleted_at      TEXT,
    scan_source_dir TEXT
);

CREATE TABLE IF NOT EXISTS tags (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS media_tags (
    media_id INTEGER NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    tag_id   INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (media_id, tag_id)
);

CREATE TABLE IF NOT EXISTS categories (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS media_categories (
    media_id    INTEGER NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    PRIMARY KEY (media_id, category_id)
);

CREATE TABLE IF NOT EXISTS directory_rules (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dir_path      TEXT NOT NULL,
    auto_tags     TEXT,
    auto_category TEXT
);

CREATE TABLE IF NOT EXISTS scan_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dir_path      TEXT NOT NULL,
    scan_time     TEXT NOT NULL,
    files_added   INTEGER DEFAULT 0,
    files_updated INTEGER DEFAULT 0,
    files_deleted INTEGER DEFAULT 0,
    files_errored INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS deep_scans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_time      TEXT    NOT NULL,
    ffmpeg_version TEXT,
    ffmpeg_path    TEXT,
    frame_count    INTEGER NOT NULL,
    file_count     INTEGER DEFAULT 0,
    decode_params  TEXT,
    description    TEXT
);

CREATE TABLE IF NOT EXISTS deep_scan_results (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    deep_scan_id  INTEGER NOT NULL REFERENCES deep_scans(id) ON DELETE CASCADE,
    media_id      INTEGER NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    frame_index   INTEGER NOT NULL,
    frame_md5     TEXT,
    status        TEXT    NOT NULL DEFAULT 'success',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_file_path ON media_files(file_path);
CREATE INDEX IF NOT EXISTS idx_media_file_hash ON media_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_media_media_type ON media_files(media_type);
CREATE INDEX IF NOT EXISTS idx_media_video_codec ON media_files(video_codec);
CREATE INDEX IF NOT EXISTS idx_media_audio_codec ON media_files(audio_codec);
CREATE INDEX IF NOT EXISTS idx_media_format_name ON media_files(format_name);
CREATE INDEX IF NOT EXISTS idx_media_pixel_format ON media_files(pixel_format);
CREATE INDEX IF NOT EXISTS idx_media_width_height ON media_files(width, height);
CREATE INDEX IF NOT EXISTS idx_media_duration ON media_files(duration);
CREATE INDEX IF NOT EXISTS idx_media_file_size ON media_files(file_size);
CREATE INDEX IF NOT EXISTS idx_media_deleted_at ON media_files(deleted_at);
CREATE INDEX IF NOT EXISTS idx_media_scan_source ON media_files(scan_source_dir);
CREATE INDEX IF NOT EXISTS idx_deep_results_scan ON deep_scan_results(deep_scan_id);
CREATE INDEX IF NOT EXISTS idx_deep_results_media ON deep_scan_results(media_id);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    """Thin wrapper around SQLite for AVShelf data operations."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """Open (or create) the database and ensure schema is up to date."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "Database":
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._conn

    # -- Schema management --------------------------------------------------

    def _init_schema(self) -> None:
        cur = self.conn.executescript(_SCHEMA_SQL)
        row = self.conn.execute(
            "SELECT version FROM schema_version LIMIT 1"
        ).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)",
                (SCHEMA_VERSION,),
            )
            self.conn.commit()

    # -- Media files --------------------------------------------------------

    def upsert_media(self, data: dict[str, Any]) -> int:
        """Insert or update a media file record. Returns the row id."""
        now = _now_iso()
        existing = self.conn.execute(
            "SELECT id FROM media_files WHERE file_path = ?",
            (data["file_path"],),
        ).fetchone()

        if existing:
            media_id = existing["id"]
            fields = {k: v for k, v in data.items() if k != "file_path"}
            fields["updated_at"] = now
            set_clause = ", ".join(f"{k} = ?" for k in fields)
            values = list(fields.values()) + [data["file_path"]]
            self.conn.execute(
                f"UPDATE media_files SET {set_clause} WHERE file_path = ?",
                values,
            )
        else:
            data.setdefault("created_at", now)
            data.setdefault("updated_at", now)
            cols = ", ".join(data.keys())
            placeholders = ", ".join("?" for _ in data)
            cur = self.conn.execute(
                f"INSERT INTO media_files ({cols}) VALUES ({placeholders})",
                list(data.values()),
            )
            media_id = cur.lastrowid

        self.conn.commit()
        return media_id

    def get_media_by_path(self, file_path: str) -> dict | None:
        """Fetch a media record by file path, or None if not found."""
        row = self.conn.execute(
            "SELECT * FROM media_files WHERE file_path = ? AND deleted_at IS NULL",
            (file_path,),
        ).fetchone()
        return dict(row) if row else None

    def get_media_by_id(self, media_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM media_files WHERE id = ?", (media_id,)
        ).fetchone()
        return dict(row) if row else None

    def soft_delete_media(self, file_path: str) -> None:
        """Mark a media file as deleted (soft delete)."""
        self.conn.execute(
            "UPDATE media_files SET deleted_at = ? WHERE file_path = ? AND deleted_at IS NULL",
            (_now_iso(), file_path),
        )
        self.conn.commit()

    def purge_deleted(self) -> int:
        """Permanently remove all soft-deleted records. Returns count."""
        cur = self.conn.execute(
            "DELETE FROM media_files WHERE deleted_at IS NOT NULL"
        )
        self.conn.commit()
        return cur.rowcount

    def list_media_in_dir(self, dir_path: str) -> list[dict]:
        """List all non-deleted media files under a scan source directory."""
        rows = self.conn.execute(
            "SELECT * FROM media_files WHERE scan_source_dir = ? AND deleted_at IS NULL",
            (dir_path,),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_media(self, conditions: list[str], params: list[Any],
                    order_by: str | None = None, limit: int | None = None) -> list[dict]:
        """Run a flexible query against media_files.

        Args:
            conditions: list of SQL WHERE clause fragments (ANDed together).
            params: corresponding bind parameters.
            order_by: optional ORDER BY clause (e.g. 'file_size DESC').
            limit: optional LIMIT.
        """
        sql = "SELECT * FROM media_files WHERE deleted_at IS NULL"
        if conditions:
            sql += " AND " + " AND ".join(conditions)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def count_media(self, conditions: list[str] | None = None,
                    params: list[Any] | None = None) -> int:
        sql = "SELECT COUNT(*) FROM media_files WHERE deleted_at IS NULL"
        if conditions:
            sql += " AND " + " AND ".join(conditions)
        row = self.conn.execute(sql, params or []).fetchone()
        return row[0]

    # -- Tags ---------------------------------------------------------------

    def get_or_create_tag(self, name: str) -> int:
        row = self.conn.execute("SELECT id FROM tags WHERE name = ?", (name,)).fetchone()
        if row:
            return row["id"]
        cur = self.conn.execute("INSERT INTO tags (name) VALUES (?)", (name,))
        self.conn.commit()
        return cur.lastrowid

    def add_tags_to_media(self, media_id: int, tag_names: list[str]) -> None:
        for name in tag_names:
            tag_id = self.get_or_create_tag(name)
            self.conn.execute(
                "INSERT OR IGNORE INTO media_tags (media_id, tag_id) VALUES (?, ?)",
                (media_id, tag_id),
            )
        self.conn.commit()

    def remove_tags_from_media(self, media_id: int, tag_names: list[str]) -> None:
        for name in tag_names:
            row = self.conn.execute("SELECT id FROM tags WHERE name = ?", (name,)).fetchone()
            if row:
                self.conn.execute(
                    "DELETE FROM media_tags WHERE media_id = ? AND tag_id = ?",
                    (media_id, row["id"]),
                )
        self.conn.commit()

    def get_tags_for_media(self, media_id: int) -> list[str]:
        rows = self.conn.execute(
            "SELECT t.name FROM tags t JOIN media_tags mt ON t.id = mt.tag_id WHERE mt.media_id = ?",
            (media_id,),
        ).fetchall()
        return [r["name"] for r in rows]

    def list_all_tags(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT t.name, COUNT(mt.media_id) as count "
            "FROM tags t LEFT JOIN media_tags mt ON t.id = mt.tag_id "
            "GROUP BY t.id ORDER BY count DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    # -- Categories ---------------------------------------------------------

    def get_or_create_category(self, name: str) -> int:
        row = self.conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
        if row:
            return row["id"]
        cur = self.conn.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        self.conn.commit()
        return cur.lastrowid

    def add_category_to_media(self, media_id: int, category_name: str) -> None:
        cat_id = self.get_or_create_category(category_name)
        self.conn.execute(
            "INSERT OR IGNORE INTO media_categories (media_id, category_id) VALUES (?, ?)",
            (media_id, cat_id),
        )
        self.conn.commit()

    def get_categories_for_media(self, media_id: int) -> list[str]:
        rows = self.conn.execute(
            "SELECT c.name FROM categories c "
            "JOIN media_categories mc ON c.id = mc.category_id "
            "WHERE mc.media_id = ?",
            (media_id,),
        ).fetchall()
        return [r["name"] for r in rows]

    def list_all_categories(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT c.name, COUNT(mc.media_id) as count "
            "FROM categories c LEFT JOIN media_categories mc ON c.id = mc.category_id "
            "GROUP BY c.id ORDER BY count DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    # -- Directory rules ----------------------------------------------------

    def add_directory_rule(self, dir_path: str, auto_tags: list[str] | None = None,
                          auto_category: str | None = None) -> int:
        cur = self.conn.execute(
            "INSERT INTO directory_rules (dir_path, auto_tags, auto_category) VALUES (?, ?, ?)",
            (dir_path, json.dumps(auto_tags or []), auto_category),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_rules_for_dir(self, dir_path: str) -> list[dict]:
        """Get all rules that apply to a directory (exact match or parent)."""
        rows = self.conn.execute(
            "SELECT * FROM directory_rules WHERE ? LIKE dir_path || '%'",
            (dir_path,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["auto_tags"] = json.loads(d["auto_tags"]) if d["auto_tags"] else []
            result.append(d)
        return result

    # -- Scan history -------------------------------------------------------

    def record_scan(self, dir_path: str, added: int = 0, updated: int = 0,
                    deleted: int = 0, errored: int = 0) -> int:
        cur = self.conn.execute(
            "INSERT INTO scan_history (dir_path, scan_time, files_added, files_updated, "
            "files_deleted, files_errored) VALUES (?, ?, ?, ?, ?, ?)",
            (dir_path, _now_iso(), added, updated, deleted, errored),
        )
        self.conn.commit()
        return cur.lastrowid

    # -- Scan sources -------------------------------------------------------

    def list_distinct_scan_sources(self) -> list[str]:
        """Return all distinct scan source directories with non-deleted files."""
        rows = self.conn.execute(
            "SELECT DISTINCT scan_source_dir FROM media_files WHERE deleted_at IS NULL"
        ).fetchall()
        return [r["scan_source_dir"] for r in rows]

    # -- Statistics ---------------------------------------------------------

    def get_media_type_stats(self) -> list[dict]:
        """Count media files grouped by type."""
        rows = self.conn.execute(
            "SELECT media_type, COUNT(*) as cnt FROM media_files "
            "WHERE deleted_at IS NULL GROUP BY media_type ORDER BY cnt DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_codec_stats(self, col: str = "video_codec", limit: int = 10) -> list[dict]:
        """Count media files grouped by a codec column.

        Args:
            col: Column name to group by (e.g. 'video_codec', 'audio_codec').
            limit: Maximum number of results.
        """
        allowed = {"video_codec", "audio_codec"}
        if col not in allowed:
            raise ValueError(f"Invalid column {col!r}, must be one of {allowed}")
        rows = self.conn.execute(
            f"SELECT {col}, COUNT(*) as cnt FROM media_files "
            f"WHERE deleted_at IS NULL AND {col} IS NOT NULL "
            f"GROUP BY {col} ORDER BY cnt DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_total_size(self) -> int:
        """Return the total file size in bytes of all non-deleted media."""
        row = self.conn.execute(
            "SELECT SUM(file_size) as total_size FROM media_files WHERE deleted_at IS NULL"
        ).fetchone()
        return row["total_size"] or 0

    # -- Trash operations ---------------------------------------------------

    def restore_media(self, file_path: str, trashed_at: str = "") -> bool:
        """Restore a soft-deleted media record. Returns True if a row was updated."""
        cur = self.conn.execute(
            "UPDATE media_files SET deleted_at = NULL, updated_at = ? "
            "WHERE file_path = ? AND deleted_at IS NOT NULL",
            (trashed_at, file_path),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def purge_media_by_path(self, file_path: str) -> bool:
        """Permanently delete a specific soft-deleted media record. Returns True if deleted."""
        cur = self.conn.execute(
            "DELETE FROM media_files WHERE file_path = ? AND deleted_at IS NOT NULL",
            (file_path,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # -- Directory rules listing --------------------------------------------

    def list_directory_rules(self) -> list[dict]:
        """Return all directory rules ordered by dir_path."""
        rows = self.conn.execute(
            "SELECT * FROM directory_rules ORDER BY dir_path"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["auto_tags"] = json.loads(d["auto_tags"]) if d["auto_tags"] else []
            result.append(d)
        return result

    # -- Deep scans ---------------------------------------------------------

    def create_deep_scan(self, ffmpeg_version: str, ffmpeg_path: str,
                         frame_count: int, decode_params: str | None = None,
                         description: str | None = None) -> int:
        cur = self.conn.execute(
            "INSERT INTO deep_scans (scan_time, ffmpeg_version, ffmpeg_path, "
            "frame_count, file_count, decode_params, description) "
            "VALUES (?, ?, ?, ?, 0, ?, ?)",
            (_now_iso(), ffmpeg_version, ffmpeg_path, frame_count,
             decode_params, description),
        )
        self.conn.commit()
        return cur.lastrowid

    def add_deep_scan_result(self, deep_scan_id: int, media_id: int,
                             frame_index: int, frame_md5: str | None = None,
                             status: str = "success",
                             error_message: str | None = None) -> None:
        self.conn.execute(
            "INSERT INTO deep_scan_results "
            "(deep_scan_id, media_id, frame_index, frame_md5, status, error_message) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (deep_scan_id, media_id, frame_index, frame_md5, status, error_message),
        )

    def update_deep_scan_file_count(self, deep_scan_id: int, file_count: int) -> None:
        self.conn.execute(
            "UPDATE deep_scans SET file_count = ? WHERE id = ?",
            (file_count, deep_scan_id),
        )
        self.conn.commit()

    def list_deep_scans(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM deep_scans ORDER BY scan_time DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_deep_scan_results(self, deep_scan_id: int,
                              media_id: int | None = None) -> list[dict]:
        sql = "SELECT * FROM deep_scan_results WHERE deep_scan_id = ?"
        params: list[Any] = [deep_scan_id]
        if media_id is not None:
            sql += " AND media_id = ?"
            params.append(media_id)
        sql += " ORDER BY media_id, frame_index"
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
