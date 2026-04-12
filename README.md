# AVShelf

A CLI-first media asset management tool powered by ffprobe. Scan, index, search, analyze, and manage your media files — all from the terminal.

## Features

- **Scan & Index** — Recursively scan directories, extract metadata via ffprobe, and store in a portable SQLite database. Incremental scanning skips unchanged files automatically.
- **Smart Search** — Query by codec, resolution, pixel format, HDR, rotation, interlacing, chapters, and dozens of other media attributes. Output as table, JSON, or CSV.
- **Natural Language Search** — Use LLM-powered queries (OpenAI / Anthropic) to find files without memorizing CLI flags.
- **Deep Scan & Verification** — Frame-level MD5 collection for decoder correctness verification. Compare two ffmpeg builds to detect decode regressions.
- **Dedup & Cleanup** — Find duplicate files (full hash or fast head+tail sampling), similar files, cold files, and "boring" files. Generate cleanup plans and safely move files to a recoverable trash.
- **Tags & Categories** — Organize files with user-defined tags and categories. Set up directory rules for automatic tagging on scan.
- **Multi-device Sync** — Export/import databases as JSON, diff and merge media directories across devices.
- **MCP Server** — Expose search and analysis capabilities to AI coding assistants via Model Context Protocol (stdio transport).
- **Safe by Design** — Cleanup never deletes files directly; everything goes through a recoverable trash with full audit logging.

## Requirements

- Python 3.10+
- ffprobe / ffmpeg (from [FFmpeg](https://ffmpeg.org/))

## Installation

```bash
# Create a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate

# Install in editable mode
pip install -e .
```

## Quick Start

```bash
# 1. Scan a directory to build the index
avshelf scan /path/to/media

# 2. Search for H.265 videos
avshelf search --vcodec hevc

# 3. Search with natural language (requires LLM config)
avshelf ask "find HDR videos with multiple audio tracks"

# 4. View detailed metadata for a file
avshelf info /path/to/file.mp4

# 5. Find duplicate files
avshelf dedup

# 6. Analyze disk space usage
avshelf space

# 7. Start MCP server for AI assistants
avshelf mcp
```

## Typical Use Cases

### 🔍 Find media files by technical properties

```bash
# Find 4K HEVC videos with HDR
avshelf search --vcodec hevc --min-width 3840 --has-hdr

# Find files larger than 1GB, sorted by size
avshelf search --min-size 1GB --sort size

# Find interlaced content with subtitles
avshelf search --interlaced --has-subtitle

# Find multi-audio-track videos (e.g., for language checking)
avshelf search --audio-tracks ">1"

# Output file paths only (pipe-friendly, useful for scripting)
avshelf search --vcodec av1 --path-only | xargs -I{} ls -lh "{}"

# Count all audio files
avshelf search --type audio --count
```

### 🧹 Clean up your media library

```bash
# Step 1: Find duplicates and save a cleanup plan
avshelf dedup --save-plan dedup_plan.json

# Step 2: Review the plan, then execute (files moved to trash, not deleted)
avshelf clean --plan dedup_plan.json --dry-run   # preview first
avshelf clean --plan dedup_plan.json              # actually move to trash

# Find "boring" files (good candidates for archival)
avshelf boring

# Find cold files not accessed in over a year
avshelf cold --days 365

# Use modification time instead (for noatime filesystems)
avshelf cold --days 365 --by mtime

# If you made a mistake, restore from trash
avshelf trash list
avshelf trash restore /original/path/to/file.mp4
```

### 🏷️ Organize with tags and rules

```bash
# Tag individual files
avshelf tag add movie.mp4 "4K" "HDR" "Dolby Vision"

# Set category
avshelf classify set movie.mp4 --category "Movie"

# Set up auto-tagging rules (applied on every scan)
avshelf rule add /media/anime --tags "anime,japanese" --category Animation
avshelf rule add /media/movies --tags "movie" --category Movie

# Now scanning will auto-tag all files in those directories
avshelf scan /media/anime

# Query by tag or category
avshelf search --tags "anime"
avshelf search --category "Movie"
```

### 🔄 Sync across devices

```bash
# On Device A: export the database
avshelf export --output media_index.json

# Transfer the JSON file to Device B, then import
avshelf import media_index.json

# Compare two directories (local or across mounts)
avshelf diff /nas/media /local/media --by hash

# Merge missing files from source to target
avshelf merge /nas/media /local/media --on-conflict skip
```

### 🔬 Verify ffmpeg decode correctness

```bash
# Step 1: Create a baseline with current ffmpeg
avshelf deep-scan run test_file.mp4 --ffmpeg /usr/bin/ffmpeg --frames 30

# Step 2: Upgrade ffmpeg, then re-scan with the new version
avshelf deep-scan run test_file.mp4 --ffmpeg /usr/local/bin/ffmpeg-new --frames 30

# Step 3: Compare frame-by-frame
avshelf verify --baseline <baseline_scan_id>
# Reports any frame MD5 mismatches → decode regression detected
```

### 🤖 Use with AI Assistants via MCP

AVShelf includes a built-in MCP (Model Context Protocol) server that lets AI assistants like Claude Desktop, Cursor, and Windsurf directly search and analyze your media library.

**Start the MCP server:**

```bash
avshelf mcp
```

**Configure Claude Desktop** — edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "avshelf": {
      "command": "/path/to/avshelf/.venv/bin/python",
      "args": ["-m", "avshelf.mcp_server"],
      "env": {
        "AVSHELF_DB_PATH": "/Users/you/.avshelf/avshelf.db"
      }
    }
  }
}
```

**Configure Cursor / Windsurf** — add to your MCP settings:

```json
{
  "mcpServers": {
    "avshelf": {
      "command": "/path/to/avshelf/.venv/bin/python",
      "args": ["-m", "avshelf.mcp_server"]
    }
  }
}
```

Once configured, you can ask your AI assistant questions like:

| What you say | What the AI does |
|---|---|
| "How many 4K HDR videos do I have?" | Calls `get_stats` and `search_media` |
| "Show me the metadata for movie.mp4" | Calls `get_media_info` |
| "Which files are taking up the most space?" | Calls `analyze_space` |
| "List all my tags and categories" | Calls `list_categories` |
| "Find duplicate video files" | Calls `search_media` with hash grouping |
| "Do I have any decode regression in test.mp4?" | Calls `get_deep_scan_results` |

**Available MCP tools:**

| Tool | Description |
|------|-------------|
| `search_media` | Search by any combination of 25+ filter parameters |
| `get_media_info` | Get complete metadata for a single file |
| `list_categories` | List all tags and categories with counts |
| `get_stats` | Database statistics (type distribution, codec distribution, total size) |
| `analyze_space` | Disk space analysis (top files, per-directory breakdown) |
| `get_deep_scan_results` | Retrieve frame-level MD5 results |

## CLI Reference

### Scanning & Indexing

| Command | Description |
|---------|-------------|
| `avshelf scan <dir>` | Scan a directory and index media files. Use `--full` to force re-scan, `--probe-all` to include unknown extensions. |
| `avshelf refresh` | Re-scan all previously indexed directories. Detects deleted/modified files. Use `--dir` to refresh a specific directory. |
| `avshelf purge` | Permanently remove all soft-deleted records from the database. |

### Search & Query

| Command | Description |
|---------|-------------|
| `avshelf search` | Search by codec, resolution, format, HDR, rotation, size, duration, tags, categories, and more. Supports `--output table/json/csv`, `--path-only`, `--count`. |
| `avshelf info <file>` | Show complete metadata for a single file, including raw ffprobe output. |
| `avshelf ask "<query>"` | Natural language search powered by LLM. Translates your question into structured filters automatically. |

**Search filter reference:**

| Filter | Description |
|--------|-------------|
| `--vcodec`, `--acodec` | Video / audio codec name |
| `--min-width`, `--max-width` | Width range (pixels) |
| `--min-height`, `--max-height` | Height range (pixels) |
| `--res` | Resolution class (4k, 1080p, 720p, sd) |
| `--min-size`, `--max-size` | File size range (supports KB, MB, GB) |
| `--min-duration`, `--max-duration` | Duration range (supports seconds, minutes, hours) |
| `--has-hdr` / `--no-hdr` | HDR presence |
| `--interlaced` / `--no-interlaced` | Interlaced content |
| `--has-subtitle` / `--no-subtitle` | Subtitle presence |
| `--has-chapters` / `--no-chapters` | Chapter markers |
| `--audio-tracks` | Audio track count (supports operators: `>1`, `=2`, `>=3`) |
| `--pixel-format` | Pixel format name |
| `--bit-depth` | Bit depth (8, 10, 12) |
| `--tags`, `--category` | Tag or category filter |
| `--raw-query` | JSONPath expression for advanced queries (e.g., `streams[0].codec_name=h264`) |

### Analysis & Cleanup

| Command | Description |
|---------|-------------|
| `avshelf dedup` | Find duplicate files by content hash. Use `--fast` for head+tail sampling. `--output json` for JSON output, `--save-plan` to save cleanup plan. |
| `avshelf similar` | Find similar files (same codec + resolution + similar duration/size). `--output json` for JSON output. |
| `avshelf space` | Analyze disk space: top largest files and per-directory breakdown. |
| `avshelf cold` | Find cold files not accessed in the last N days (default 180). Uses access time (atime) by default — `--by mtime` to use modification time instead. |
| `avshelf boring` | Find unremarkable files (configurable codec list; default: H.264+AAC, ≤1080p, single audio, no HDR/subtitles/tags). |
| `avshelf clean --plan <file>` | Execute a cleanup plan JSON — moves files to trash (never deletes directly). Supports `--dry-run`. |

### Tags, Categories & Rules

| Command | Description |
|---------|-------------|
| `avshelf tag add <file> <tags...>` | Add tags to a media file. Supports `--query` for batch operations. |
| `avshelf tag remove <file> <tags...>` | Remove tags from a media file. |
| `avshelf tag list` | List all tags with usage counts. |
| `avshelf classify set <file> --category <name>` | Assign a category to a file. |
| `avshelf classify list` | List all categories with usage counts. |
| `avshelf rule add <dir> --tags <t1,t2> --category <c>` | Add auto-tagging rule for a directory. Applied on every scan. |
| `avshelf rule list` | List all directory rules. |

### Deep Scan & Verification

| Command | Description |
|---------|-------------|
| `avshelf deep-scan run <file>` | Decode first N frames and collect per-frame MD5. Use `--frames`, `--ffmpeg`, `--decode-params`. |
| `avshelf deep-scan list` | List all deep scan records. |
| `avshelf deep-scan show <file>` | Show frame-level MD5 results for a file. |
| `avshelf verify --baseline <id>` | Re-decode files and compare frame MD5s against a baseline scan. Detects decode regressions across ffmpeg versions. |

### Multi-device Sync

| Command | Description |
|---------|-------------|
| `avshelf export` | Export the media database to a JSON file. |
| `avshelf import <file>` | Import records from a JSON export. Merges by file hash — no duplicates. |
| `avshelf diff <dir_a> <dir_b>` | Compare two directories. Use `--by name` or `--by hash`. |
| `avshelf merge <source> <target>` | Copy missing files from source to target. Conflict resolution: `--on-conflict skip/overwrite/keep-both`. |

### Trash Management

| Command | Description |
|---------|-------------|
| `avshelf trash list` | List files currently in the trash. |
| `avshelf trash restore <path>` | Restore a file from trash to its original location. |
| `avshelf trash purge` | Permanently delete all files in the trash. |

### Other

| Command | Description |
|---------|-------------|
| `avshelf stats` | Database statistics: file counts by type, top codecs. |
| `avshelf config show` | Show current configuration. |
| `avshelf config set <key> <value>` | Set a configuration value (e.g. `llm.provider openai`). |
| `avshelf mcp` | Start the MCP server (stdio transport) for AI assistant integration. |

## Configuration

Configuration is stored in `~/.avshelf/config.toml`. Manage it via CLI or edit directly.

```bash
# Show all settings
avshelf config show

# Configure LLM for natural language search
avshelf config set llm.provider openai
avshelf config set llm.api_key sk-...
avshelf config set llm.model gpt-4o-mini

# Custom ffprobe/ffmpeg paths
avshelf config set scan.ffprobe_path /usr/local/bin/ffprobe
avshelf config set scan.ffmpeg_path /usr/local/bin/ffmpeg
```

**Environment variable overrides** (take precedence over config file):

| Variable | Config Key |
|----------|------------|
| `AVSHELF_LLM_API_KEY` | `llm.api_key` |
| `AVSHELF_LLM_PROVIDER` | `llm.provider` |
| `AVSHELF_LLM_MODEL` | `llm.model` |
| `AVSHELF_DB_PATH` | `database.path` |
| `AVSHELF_FFPROBE_PATH` | `scan.ffprobe_path` |
| `AVSHELF_FFMPEG_PATH` | `scan.ffmpeg_path` |

## Data Storage

All data is stored in `~/.avshelf/`:

| Path | Description |
|------|-------------|
| `config.toml` | User configuration |
| `avshelf.db` | SQLite database (WAL mode, portable) |
| `trash/` | Recoverable trash (organized by date) |
| `logs/` | JSONL audit logs |

To change the database location: `avshelf config set database.path /custom/path/avshelf.db`

---

For architecture details, module documentation, and contribution guidelines, see [DEVELOPMENT.md](DEVELOPMENT.md).

## License

MIT
