"""Deep scan — frame-level MD5 collection and decode verification."""

from __future__ import annotations

import os
import subprocess
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

from avshelf.database import Database


def _is_tty() -> bool:
    """Check if stdout is connected to a terminal."""
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _is_wasm_ffmpeg(ffmpeg_path: str) -> tuple[bool, str | None]:
    """Detect if ffmpeg_path is a WASM/JS binary that needs Node.js.

    Returns (is_wasm, node_path).
    A file is considered WASM if:
      - It ends with .js, OR
      - It has no extension AND starts with a JS shebang or contains 'WebAssembly'
    """
    import os
    p = Path(ffmpeg_path)
    if p.suffix == ".js":
        return True, _find_node()

    # Check if it's a JS file without extension (e.g. emscripten-built ffmpeg_g)
    if p.exists() and p.is_file() and not p.suffix:
        try:
            with open(p, "rb") as f:
                header = f.read(512)
            # JS shebang or emscripten marker
            if header.startswith(b"#!/") or b"WebAssembly" in header or b"emscripten" in header.lower():
                return True, _find_node()
        except OSError:
            pass

    return False, None


def _find_node() -> str | None:
    """Find a suitable Node.js binary (>=18) for running WASM ffmpeg."""
    import shutil

    # 1. Respect EMSDK_NODE env var (set by emsdk_env.sh)
    emsdk_node = os.environ.get("EMSDK_NODE")
    if emsdk_node and Path(emsdk_node).is_file():
        return emsdk_node

    # 2. Look for emsdk-bundled node (common path)
    emsdk = os.environ.get("EMSDK")
    if emsdk:
        node_dir = Path(emsdk) / "node"
        if node_dir.is_dir():
            candidates = sorted(node_dir.iterdir(), reverse=True)
            for d in candidates:
                node_bin = d / "bin" / "node"
                if node_bin.is_file():
                    return str(node_bin)

    # 3. Well-known emsdk install location
    for base in [Path.home() / "local" / "emsdk", Path.home() / "emsdk"]:
        node_dir = base / "node"
        if node_dir.is_dir():
            candidates = sorted(node_dir.iterdir(), reverse=True)
            for d in candidates:
                node_bin = d / "bin" / "node"
                if node_bin.is_file():
                    return str(node_bin)

    # 4. System node (may be too old, but try anyway)
    return shutil.which("node") or shutil.which("nodejs")


def get_ffmpeg_version(ffmpeg_path: str = "ffmpeg") -> str:
    """Get the ffmpeg version string."""
    import os
    is_wasm, node_path = _is_wasm_ffmpeg(ffmpeg_path)
    try:
        if is_wasm and node_path:
            cmd = [node_path, ffmpeg_path, "-nostdin", "-version"]
        else:
            cmd = [ffmpeg_path, "-version"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
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

    Supports both native ffmpeg and WASM/emscripten-built ffmpeg (run via Node.js).
    WASM ffmpeg requires -nostdin to avoid ioctl_tcgets crash in NODERAWFS mode.
    """
    is_wasm, node_path = _is_wasm_ffmpeg(ffmpeg_path)

    if is_wasm and node_path:
        # WASM ffmpeg: run via Node.js, must add -nostdin
        cmd = [node_path, ffmpeg_path, "-nostdin", "-i", str(file_path)]
    else:
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
    total = len(file_paths)
    use_rich = _is_tty()

    if use_rich:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        ) as progress:
            ptask = progress.add_task("Deep scanning...", total=total)

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
    else:
        # Non-TTY: plain text progress for redirected output / nohup
        print(f"Deep scanning {total} files ({skipped} skipped) ...", flush=True)
        done = 0

        if effective_threads <= 1:
            for fp in paths_to_scan:
                _, frame_results = _process_single_file(
                    fp, ffmpeg_path, frames, extra_params
                )
                _store_results(fp, frame_results)
                done += 1
                print(
                    f"  [{done}/{total}] {Path(fp).name} "
                    f"processed={result.files_processed} "
                    f"errors={result.files_errored} "
                    f"frames={result.total_frames}",
                    flush=True,
                )
        else:
            with ThreadPoolExecutor(max_workers=effective_threads) as pool:
                futures = {
                    pool.submit(
                        _process_single_file, fp, ffmpeg_path, frames, extra_params
                    ): fp
                    for fp in paths_to_scan
                }
                for future in as_completed(futures):
                    fp, frame_results = future.result()
                    _store_results(fp, frame_results)
                    done += 1
                    print(
                        f"  [{done}/{total}] {Path(fp).name} "
                        f"processed={result.files_processed} "
                        f"errors={result.files_errored} "
                        f"frames={result.total_frames}",
                        flush=True,
                    )

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
    passed_file_list: list[dict] = field(default_factory=list)


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
        media = db.get_media_by_id(mid)
        file_path = media["file_path"] if media else "unknown"

        if not new_frames:
            result.error_files += 1
            result.failures.append({
                "media_id": mid,
                "file_path": file_path,
                "reason": "missing from new scan",
            })
            continue

        # Count matching and mismatching frames
        match_count = 0
        mismatch_count = 0
        first_mismatch = None
        for fi in sorted(base_frames.keys()):
            base_md5 = base_frames.get(fi)
            new_md5 = new_frames.get(fi)
            if base_md5 == new_md5:
                match_count += 1
            else:
                mismatch_count += 1
                if first_mismatch is None:
                    first_mismatch = {
                        "frame_index": fi,
                        "baseline_md5": base_md5,
                        "new_md5": new_md5,
                    }

        total_compared = match_count + mismatch_count
        if mismatch_count > 0:
            result.failed_files += 1
            failure_entry = {
                "media_id": mid,
                "file_path": file_path,
                "reason": f"mismatch at frame {first_mismatch['frame_index']}",
                "first_mismatch_frame": first_mismatch["frame_index"],
                "baseline_md5": first_mismatch["baseline_md5"],
                "new_md5": first_mismatch["new_md5"],
                "mismatch_count": mismatch_count,
                "total_compared": total_compared,
            }
            result.failures.append(failure_entry)
        else:
            result.passed_files += 1
            result.passed_file_list.append({
                "media_id": mid,
                "file_path": file_path,
                "frames_compared": total_compared,
            })

    return result
