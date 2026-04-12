"""Deep scan — frame-level MD5 collection and decode verification."""

from __future__ import annotations

import subprocess
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

from avshelf.database import Database


def get_ffmpeg_version(ffmpeg_path: str = "ffmpeg") -> str:
    """Get the ffmpeg version string."""
    try:
        result = subprocess.run(
            [ffmpeg_path, "-version"],
            capture_output=True, text=True, timeout=10,
        )
        first_line = result.stdout.split("\n")[0]
        return first_line.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return "unknown"


def extract_frame_md5s(
    file_path: str,
    ffmpeg_path: str = "ffmpeg",
    frames: int = 10,
    decode_params: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Decode the first N video frames and compute per-frame MD5.

    Returns a list of dicts: [{frame_index, frame_md5, status, error_message}].
    Uses: ffmpeg -i <input> -vframes N -f framemd5 -
    """
    cmd = [ffmpeg_path, "-i", str(file_path)]
    if decode_params:
        cmd.extend(decode_params)
    cmd.extend(["-vframes", str(frames), "-an", "-f", "framemd5", "-"])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        return [{"frame_index": 0, "frame_md5": None,
                 "status": "error", "error_message": "ffmpeg timed out"}]
    except FileNotFoundError:
        return [{"frame_index": 0, "frame_md5": None,
                 "status": "error", "error_message": f"ffmpeg not found: {ffmpeg_path}"}]

    if result.returncode != 0 and not result.stdout.strip():
        return [{"frame_index": 0, "frame_md5": None,
                 "status": "error", "error_message": result.stderr[:500]}]

    results: list[dict[str, Any]] = []
    frame_idx = 0
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # framemd5 format: stream#, dts, pts, duration, size, hash
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 6:
            try:
                md5 = parts[-1].strip()
                results.append({
                    "frame_index": frame_idx,
                    "frame_md5": md5,
                    "status": "success",
                    "error_message": None,
                })
                frame_idx += 1
            except (ValueError, IndexError):
                continue

    if not results:
        return [{"frame_index": 0, "frame_md5": None,
                 "status": "error",
                 "error_message": "No frames decoded (non-video file or decode failure)"}]

    return results


@dataclass
class DeepScanResult:
    """Accumulated statistics from a deep scan run."""
    scan_id: int = 0
    files_processed: int = 0
    files_errored: int = 0
    total_frames: int = 0


def _process_single_file(
    fp: str,
    ffmpeg_path: str,
    frames: int,
    extra_params: list[str] | None,
) -> tuple[str, list[dict[str, Any]]]:
    """Extract frame MD5s for a single file (thread-safe, no DB access)."""
    return fp, extract_frame_md5s(fp, ffmpeg_path, frames, extra_params)


def run_deep_scan(
    db: Database,
    file_paths: list[str],
    ffmpeg_path: str = "ffmpeg",
    frames: int = 10,
    decode_params: str | None = None,
    description: str | None = None,
    threads: int = 1,
) -> DeepScanResult:
    """Run deep scan on a list of files, storing frame MD5s in the database.

    When *threads* > 1, ffmpeg decode jobs are dispatched to a thread pool
    while database writes remain serialised on the main thread.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    version = get_ffmpeg_version(ffmpeg_path)
    scan_id = db.create_deep_scan(
        ffmpeg_version=version,
        ffmpeg_path=ffmpeg_path,
        frame_count=frames,
        decode_params=decode_params,
        description=description,
    )

    result = DeepScanResult(scan_id=scan_id)
    extra_params = decode_params.split() if decode_params else None

    # Build a mapping from file_path -> media record so we can skip
    # unknown files before submitting work to the pool.
    media_map: dict[str, dict] = {}
    for fp in file_paths:
        media = db.get_media_by_path(fp)
        if media:
            media_map[fp] = media

    paths_to_scan = [fp for fp in file_paths if fp in media_map]
    skipped = len(file_paths) - len(paths_to_scan)
    result.files_errored += skipped

    def _store_results(fp: str, frame_results: list[dict[str, Any]]) -> None:
        """Write frame results to DB (must run on main thread)."""
        media = media_map[fp]
        has_error = False
        for fr in frame_results:
            db.add_deep_scan_result(
                deep_scan_id=scan_id,
                media_id=media["id"],
                frame_index=fr["frame_index"],
                frame_md5=fr["frame_md5"],
                status=fr["status"],
                error_message=fr.get("error_message"),
            )
            if fr["status"] == "error":
                has_error = True
            else:
                result.total_frames += 1
        db.conn.commit()
        if has_error:
            result.files_errored += 1
        result.files_processed += 1

    effective_threads = max(1, threads)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ) as progress:
        ptask = progress.add_task("Deep scanning...", total=len(file_paths))

        # Advance progress for skipped files immediately.
        for _ in range(skipped):
            progress.advance(ptask)

        if effective_threads <= 1:
            # Sequential path — same behaviour as before.
            for fp in paths_to_scan:
                progress.update(ptask, description=f"[cyan]{Path(fp).name}")
                _, frame_results = _process_single_file(
                    fp, ffmpeg_path, frames, extra_params
                )
                _store_results(fp, frame_results)
                progress.advance(ptask)
        else:
            # Parallel path — fan-out ffmpeg, fan-in DB writes.
            with ThreadPoolExecutor(max_workers=effective_threads) as pool:
                futures = {
                    pool.submit(
                        _process_single_file, fp, ffmpeg_path, frames, extra_params
                    ): fp
                    for fp in paths_to_scan
                }
                for future in as_completed(futures):
                    fp, frame_results = future.result()
                    progress.update(ptask, description=f"[cyan]{Path(fp).name}")
                    _store_results(fp, frame_results)
                    progress.advance(ptask)

    db.update_deep_scan_file_count(scan_id, result.files_processed)
    return result


@dataclass
class VerifyResult:
    """Result of comparing two deep scan runs."""
    total_files: int = 0
    passed_files: int = 0
    failed_files: int = 0
    error_files: int = 0
    failures: list[dict] = field(default_factory=list)


def verify_against_baseline(
    db: Database,
    baseline_scan_id: int,
    new_scan_id: int,
) -> VerifyResult:
    """Compare two deep scan results frame-by-frame.

    Returns a VerifyResult with pass/fail details.
    """
    baseline = db.get_deep_scan_results(baseline_scan_id)
    new_results = db.get_deep_scan_results(new_scan_id)

    # Index by (media_id, frame_index)
    baseline_map: dict[tuple[int, int], str | None] = {}
    for r in baseline:
        baseline_map[(r["media_id"], r["frame_index"])] = r["frame_md5"]

    new_map: dict[tuple[int, int], str | None] = {}
    for r in new_results:
        new_map[(r["media_id"], r["frame_index"])] = r["frame_md5"]

    # Collect unique media_ids from baseline
    media_ids = sorted(set(r["media_id"] for r in baseline))

    result = VerifyResult(total_files=len(media_ids))

    for mid in media_ids:
        base_frames = {fi: md5 for (m, fi), md5 in baseline_map.items() if m == mid}
        new_frames = {fi: md5 for (m, fi), md5 in new_map.items() if m == mid}

        if not new_frames:
            result.error_files += 1
            media = db.get_media_by_id(mid)
            result.failures.append({
                "media_id": mid,
                "file_path": media["file_path"] if media else "unknown",
                "reason": "missing from new scan",
            })
            continue

        mismatch_frame = None
        for fi in sorted(base_frames.keys()):
            base_md5 = base_frames.get(fi)
            new_md5 = new_frames.get(fi)
            if base_md5 != new_md5:
                mismatch_frame = fi
                break

        if mismatch_frame is not None:
            result.failed_files += 1
            media = db.get_media_by_id(mid)
            result.failures.append({
                "media_id": mid,
                "file_path": media["file_path"] if media else "unknown",
                "reason": f"mismatch at frame {mismatch_frame}",
                "first_mismatch_frame": mismatch_frame,
                "baseline_md5": base_frames.get(mismatch_frame),
                "new_md5": new_frames.get(mismatch_frame),
            })
        else:
            result.passed_files += 1

    return result
