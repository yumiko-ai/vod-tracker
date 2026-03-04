#!/usr/bin/env python3
"""
VOD Tracker - Monitors YouTube channels for live streams/VODs
Downloads, segments into clips, and logs everything to SQLite
"""

import json
import os
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

# Config - Use environment variables with fallbacks
WORKSPACE = Path(os.environ.get("VOD_WORKSPACE", Path(__file__).parent))
DATA_DIR = WORKSPACE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR = DATA_DIR / "upload"
DB_PATH = DATA_DIR / "tracker.db"
CHANNELS_FILE = DATA_DIR / "channels.json"

# YouTube API (using yt-dlp for free, no API key needed)
YT_DLP_OPTS = [
    "yt-dlp",
    "--format", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",  # 1080p max
    "--merge-output-format", "mp4",
]


def init_db():
    """Initialize SQLite database"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vods (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            channel_name TEXT,
            title TEXT,
            duration_seconds INTEGER,
            status TEXT DEFAULT 'pending',
            downloaded_at TEXT,
            processed_at TEXT,
            file_path TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            clip_path TEXT NOT NULL,
            start_time INTEGER,
            end_time INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES vods(video_id)
        )
    """)
    conn.commit()
    return conn


def load_channels():
    """Load channel IDs from config"""
    with open(CHANNELS_FILE) as f:
        data = json.load(f)
    return data.get("channels", [])


def get_latest_vod(channel_id):
    """Get the latest VOD/live for a channel using yt-dlp"""
    cmd = [
        *YT_DLP_OPTS,
        "--flat-playlist",
        "--print", "video_id,title,duration",
        f"https://www.youtube.com/channel/{channel_id}/videos"
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        lines = result.stdout.strip().split("\n")
        
        for line in reversed(lines):
            if "," in line:
                parts = line.split(",")
                if len(parts) >= 3:
                    video_id = parts[0].strip()
                    title = ",".join(parts[1:-1]).strip()
                    try:
                        duration = int(parts[-1].strip())
                        # Skip shorts (under 60 sec) and live streams (duration = 0)
                        if duration >= 60:
                            return {"video_id": video_id, "title": title, "duration": duration}
                    except ValueError:
                        continue
        return None
    except Exception as e:
        print(f"Error fetching VOD for {channel_id}: {e}")
        return None


def download_vod(video_id, channel_id):
    """Download a VOD to temp storage"""
    output_dir = UPLOAD_DIR / channel_id / "temp"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_template = str(output_dir / "%(id)s.%(ext)s")
    
    cmd = [
        *YT_DLP_OPTS,
        "-o", output_template,
        f"https://youtube.com/watch?v={video_id}"
    ]
    
    try:
        subprocess.run(cmd, capture_output=True, timeout=3600)  # 1hr timeout
        # Find the downloaded file
        for f in output_dir.glob(f"{video_id}.*"):
            return str(f)
        return None
    except Exception as e:
        print(f"Error downloading {video_id}: {e}")
        return None


def segment_video(video_path, video_id, channel_id, clip_duration=300):
    """Cut video into N-minute segments"""
    if not video_path or not os.path.exists(video_path):
        return []
    
    output_dir = UPLOAD_DIR / channel_id / "clips"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get duration
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", 
           "-of", "default=noprint_wrappers=1:nokey=1", video_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        total_duration = int(float(result.stdout.strip()))
    except:
        return []
    
    clips = []
    for start in range(0, total_duration, clip_duration):
        end = min(start + clip_duration, total_duration)
        clip_name = f"{video_id}__{start//60:02d}-{end//60:02d}.mp4"
        clip_path = output_dir / clip_name
        
        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-ss", str(start),
            "-t", str(end - start),
            "-c:v", "libx264",
            "-preset", "fast",  # Speed over compression
            "-crf", "28",  # Higher CRF = smaller file
            "-c:a", "aac",
            "-b:a", "128k",
            "-movflags", "+faststart",
            str(clip_path)
        ]
        
        try:
            subprocess.run(cmd, capture_output=True, timeout=300)  # 5min per clip
            if clip_path.exists():
                clips.append(str(clip_path))
        except Exception as e:
            print(f"Error creating clip {clip_name}: {e}")
    
    return clips


def process_channel(conn, channel_id):
    """Check channel for new VODs and process them"""
    print(f"Checking channel: {channel_id}")
    
    # Get latest VOD
    vod = get_latest_vod(channel_id)
    if not vod:
        print(f"  No VOD found for {channel_id}")
        return
    
    video_id = vod["video_id"]
    
    # Check if already processed
    cur = conn.execute("SELECT status FROM vods WHERE video_id = ?", (video_id,))
    row = cur.fetchone()
    
    if row and row[0] == "processed":
        print(f"  Already processed: {video_id}")
        return
    
    if row and row[0] == "downloaded":
        print(f"  Already downloaded, processing: {video_id}")
    else:
        # Log new VOD
        conn.execute("""
            INSERT INTO vods (video_id, channel_id, title, duration_seconds, status, downloaded_at)
            VALUES (?, ?, ?, ?, 'downloaded', ?)
        """, (video_id, channel_id, vod["title"], vod["duration"], datetime.now().isoformat()))
        conn.commit()
    
    # Download
    print(f"  Downloading: {vod['title']}")
    file_path = download_vod(video_id, channel_id)
    
    if not file_path:
        print(f"  Failed to download")
        return
    
    # Update with file path
    conn.execute("UPDATE vods SET file_path = ? WHERE video_id = ?", (file_path, video_id))
    conn.commit()
    
    # Segment
    print(f"  Segmenting into 5-min clips...")
    clips = segment_video(file_path, video_id, channel_id)
    
    # Log clips
    for clip_path in clips:
        clip_name = os.path.basename(clip_path)
        # Extract times from filename
        try:
            times = clip_name.split("__")[1].replace(".mp4", "").split("-")
            start = int(times[0]) * 60
            end = int(times[1]) * 60
        except:
            start, end = 0, 300
        
        conn.execute("INSERT INTO clips (video_id, clip_path, start_time, end_time) VALUES (?, ?, ?, ?)",
                     (video_id, clip_path, start, end))
    
    # Update status
    conn.execute("UPDATE vods SET status = 'processed', processed_at = ? WHERE video_id = ?",
                 (datetime.now().isoformat(), video_id))
    conn.commit()
    
    # Delete original to save space - ONLY after verifying clips are valid
    if clips and len(clips) > 0:
        all_clips_valid = True
        for clip_path in clips:
            # Verify clip with ffprobe
            cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                   "-of", "default=noprint_wrappers=1:nokey=1", clip_path]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                duration = float(result.stdout.strip())
                if duration <= 0:
                    all_clips_valid = False
                    print(f"  Warning: Invalid clip {clip_path} (duration={duration})")
                    break
            except Exception as e:
                all_clips_valid = False
                print(f"  Warning: Could not verify clip {clip_path}: {e}")
                break
        
        if all_clips_valid:
            try:
                os.remove(file_path)
                print(f"  Deleted original to save space (verified {len(clips)} clips)")
            except Exception as e:
                print(f"  Warning: Could not delete original: {e}")
        else:
            print(f"  Keeping original due to invalid clips")
    else:
        print(f"  Keeping original - no clips were created")
    
    print(f"  Done! Created {len(clips)} clips")


def main():
    print(f"=== VOD Tracker started at {datetime.now()} ===")
    
    # Init
    conn = init_db()
    channels = load_channels()
    
    print(f"Tracking {len(channels)} channels")
    
    # Process each channel
    for channel_id in channels:
        try:
            process_channel(conn, channel_id)
        except Exception as e:
            print(f"Error processing {channel_id}: {e}")
    
    print("=== Done ===")
    conn.close()


if __name__ == "__main__":
    main()
