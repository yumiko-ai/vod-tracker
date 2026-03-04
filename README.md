# VOD Tracker

Automated YouTube channel monitor that downloads live streams/VODs, segments them into clips, and logs everything to SQLite.

## Features

- Monitors multiple YouTube channels for new VODs
- Downloads VODs efficiently (H.264, 1080p max)
- Segments videos into 5-minute clips
- Deduplicates by video ID (skips already-processed)
- Deletes originals after processing to save storage
- Portable: just needs Python + ffmpeg

## 🌟 New: Best Bits Finder

Automatically find and extract the most engaging moments from your VODs:

- **Transcription**: Uses Whisper (local or API) for accurate speech-to-text with timestamps
- **AI Analysis**: LLM-powered highlight detection tuned for YouTube drama/commentary content
- **Smart Extraction**: Automatically clips the best moments with context padding
- **Categories**: Identifies funny moments, drama/conflict, interesting discussions, and key highlights
- **Viral Scoring**: Ranks clips by engagement potential

## Setup

### Core Dependencies

```bash
# Install dependencies
pip install yt-dlp

# For the best bits feature, also install:
pip install openai-whisper  # Local transcription (no API key needed)

# Or use Whisper API by setting OPENAI_API_KEY
```

### System Requirements

- Python 3.8+
- ffmpeg (for video processing)
- ffprobe (usually included with ffmpeg)

### Configuration

Add channel IDs to `channels.json`:

```json
{
  "channels": [
    "UC_x5XG1OV2P6uZZ5FSM9Tww",
    "UCLtRE2qqK_5jTztmarF0E5g"
  ]
}
```

## Usage

### Basic VOD Tracking

```bash
# Run manually
python3 tracker.py

# Or run hourly via cron
0 * * * * /path/to/tracker.py
```

### Finding Best Bits

The `clip_finder.py` module finds and extracts highlight clips from your VODs.

#### Process a Single Video

```bash
# Process a video file directly
python3 clip_finder.py /path/to/video.mp4 --video-id abc123 --channel mychannel

# Keep the transcript for later analysis
python3 clip_finder.py /path/to/video.mp4 --keep-transcript
```

#### Process from Database

```bash
# Process unprocessed VODs from the database
python3 clip_finder.py --process-db

# Limit to 3 VODs
python3 clip_finder.py --process-db --limit 3

# Reprocess already-processed VODs
python3 clip_finder.py --process-db --reprocess
```

### LLM Provider Options

Choose your LLM provider for highlight analysis:

```bash
# OpenAI (default)
export OPENAI_API_KEY="sk-..."
python3 clip_finder.py video.mp4 --provider openai --model gpt-4o-mini

# Anthropic Claude
export ANTHROPIC_API_KEY="sk-ant-..."
python3 clip_finder.py video.mp4 --provider anthropic --model claude-3-haiku-20240307

# MiniMax
export MINIMAX_API_KEY="your-api-key"
export MINIMAX_GROUP_ID="your-group-id"
python3 clip_finder.py video.mp4 --provider minimax --model minimax-m2.1
# or use minimax-m2.5

# Local Ollama
ollama serve  # In another terminal
python3 clip_finder.py video.mp4 --provider ollama --model llama3.2
```

### Whisper Model Options

Choose your transcription quality/speed trade-off:

```bash
# Fast and accurate (default)
python3 clip_finder.py video.mp4 --whisper-model turbo

# Faster, less accurate
python3 clip_finder.py video.mp4 --whisper-model small

# Most accurate, slower
python3 clip_finder.py video.mp4 --whisper-model large-v3
```

## Output

### VOD Tracking

```
data/
├── upload/
│   └── [channel_id]/
│       └── clips/
│           ├── video123__00-05.mp4
│           ├── video123__05-10.mp4
│           └── ...
└── tracker.db
```

### Best Bits

```
data/
├── highlights/
│   └── [channel_id]/
│       ├── video123__120-180__Epic_Rant.mp4
│       ├── video123__450-520__Funny_Reaction.mp4
│       └── ...
├── transcripts/
│   └── video123.json
└── tracker.db
```

## Database Schema

### `vods` Table
Tracks downloaded VODs and their processing status:
- `video_id` - YouTube video ID (primary key)
- `channel_id` - YouTube channel ID
- `title` - Video title
- `duration_seconds` - Video length
- `status` - pending/downloaded/processed
- `file_path` - Path to downloaded file
- Timestamps for tracking

### `clips` Table
Segmented clips from VODs:
- `video_id` - Source video reference
- `clip_path` - Path to clip file
- `start_time` / `end_time` - Segment timestamps

### `highlights` Table (New)
Best bits extracted from VODs:
- `video_id` - Source video reference
- `clip_path` - Path to highlight clip
- `start_time` / `end_time` - Exact timestamps
- `title` - Suggested clip title
- `description` - What happens in the clip
- `category` - drama/funny/discussion/highlight
- `viral_score` - Engagement potential (1-10)

## How Best Bits Works

### 1. Transcription
Uses OpenAI's Whisper to transcribe the entire video with word-level timestamps:
- Supports local models (no API key needed)
- Accurate timing for clip extraction
- Handles multiple speakers and background noise

### 2. AI Analysis
Sends transcript to an LLM with a specialized prompt for YouTube drama/commentary:
- Identifies funny moments, drama, interesting discussions
- Provides viral scores for prioritization
- Returns structured JSON with timestamps

### 3. Smart Extraction
Extracts clips with intelligent handling:
- **Context padding**: Adds 5 seconds before/after for natural cuts
- **Overlap detection**: Avoids duplicate clips
- **Score ranking**: Prioritizes highest viral potential
- **Limit**: Extracts top 10 highlights max

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VOD_WORKSPACE` | Script directory | Base directory for data |
| `WHISPER_MODEL` | `turbo` | Whisper model size |
| `LLM_PROVIDER` | `openai` | LLM provider (openai/anthropic/minimax/ollama) |
| `LLM_MODEL` | `gpt-4o-mini` | LLM model name |
| `OPENAI_API_KEY` | - | OpenAI API key |
| `ANTHROPIC_API_KEY` | - | Anthropic API key |
| `MINIMAX_API_KEY` | - | MiniMax API key |
| `MINIMAX_GROUP_ID` | - | MiniMax group ID (required for MiniMax) |
| `LLM_API_URL` | `http://localhost:11434/api/generate` | Ollama API URL |

## Cloud Ready

- Stateless design (portable DB + config)
- Deletes originals after segmentation
- Efficient H.264 encoding with CRF 28
- Whisper turbo model for fast transcription
- Works with local LLMs for zero API costs

## Example Workflow

```bash
# 1. Download and segment VODs
python3 tracker.py

# 2. Find best bits in downloaded VODs
python3 clip_finder.py --process-db

# 3. Review highlights in data/highlights/
```

## Tips

- Use `turbo` Whisper model for best speed/accuracy balance
- `gpt-4o-mini` is cost-effective for analysis
- Keep transcripts with `--keep-transcript` for debugging
- Set `VOD_WORKSPACE` for cloud deployments
- Process during off-peak hours for large backlogs