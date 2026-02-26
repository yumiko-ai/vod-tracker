# VOD Tracker

Automated YouTube channel monitor that downloads live streams/VODs, segments them into clips, and logs everything to SQLite.

## Features

- Monitors multiple YouTube channels for new VODs
- Downloads VODs efficiently (H.264, 1080p max)
- Segments videos into 5-minute clips
- Deduplicates by video ID (skips already-processed)
- Deletes originals after processing to save storage
- Portable: just needs Python + ffmpeg

## Setup

```bash
# Install dependencies
pip install yt-dlp

# Add channel IDs to channels.json
```

## Usage

```bash
# Run manually
python3 tracker.py

# Or run hourly via cron
0 * * * * /path/to/tracker.py
```

## Output

```
upload/
├── [channel_id]/
│   └── clips/
│       ├── video123__00-05.mp4
│       ├── video123__05-10.mp4
│       └── ...
```

## Database

SQLite database tracks:
- `vods` - Downloaded VODs and their status
- `clips` - Created clip segments

## Cloud Ready

- Stateless design (portable DB + config)
- Deletes originals after segmentation
- Efficient H.264 encoding with CRF 28
- No heavy dependencies
