# AVShelf

A media asset management tool powered by ffprobe. Scan, index, search, and manage your media files with ease.

## Features

- **Scan & Index**: Recursively scan directories, extract metadata via ffprobe, and store in a portable SQLite database
- **Smart Search**: Query by codec, resolution, pixel format, HDR, rotation, and dozens of other media attributes
- **Natural Language Search**: Use LLM-powered queries to find files without memorizing CLI flags
- **Deep Scan**: Frame-level MD5 collection for decoder correctness verification
- **Dedup & Cleanup**: Find duplicate files, similar files, cold files, and reclaim disk space
- **Multi-device Sync**: Export/import databases, diff and merge media directories
- **MCP Server**: Expose search capabilities to AI coding assistants via Model Context Protocol
- **CLI-first**: All operations available from the command line, pipe-friendly output

## Requirements

- Python 3.10+
- ffprobe / ffmpeg (from FFmpeg)

## Installation

```bash
# Using uv (recommended)
uv pip install -e .

# Or using pip
pip install -e .
```

## Quick Start

```bash
# Scan a directory
avshelf scan /path/to/media

# Search for H.265 videos
avshelf search --vcodec hevc

# Search with natural language
avshelf ask "find HDR videos with multiple audio tracks"

# View file details
avshelf info /path/to/file.mp4

# Start MCP server for AI assistants
avshelf mcp
```

## Configuration

Configuration is stored in `~/.avshelf/config.toml`. Use the CLI to manage settings:

```bash
avshelf config show
avshelf config set llm.provider openai
```

## License

MIT
