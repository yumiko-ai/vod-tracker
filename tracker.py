#!/usr/bin/env python3
"""
VOD Tracker - Monitors YouTube channels for live streams/VODs
Downloads, segments into clips, and logs everything to SQLite
"""

import json
import logging
import os
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Config - Use environment variables with fallbacks
WORKSPACE = Path(os.environ.get("VOD_WORKSPACE", Path(__file__).parent))
DATA_DIR = WORKSPACE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR = DATA_DIR / "upload"
DB_PATH = DATA_DIR / "tracker.db"
CHANNELS_FILE = WORKSPACE / "channels.json"  # channels.json is at root, not in data/

# YouTube API (using yt-dlp for free, no API key needed)
YT_DLP_OPTS = [
    "yt-dlp",
    "--cookies-from-browser", "chrome",
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
    """Get the latest VOD/live for a channel using yt-dlp.
    
    Returns the most recent video that:
    - Has duration >= 60 seconds (skips shorts)
    - Has duration > 0 (skips live streams, which have duration=0 when live)
    
    Returns dict with video_id, title, duration or None if no valid video found.
    """
    # Use --print with format specifiers to get id, title, and duration
    # Note: --flat-playlist is NOT used because it doesn't return duration
    # Use --playlist-end to limit results (fetching all videos is slow)
    # Use --ignore-errors to continue processing after errors (e.g., age-restricted videos)
    cmd = [
        "yt-dlp",
        "--remote-components", "ejs:github",  # Enable JS challenge solver for YouTube
        "--playlist-end", "10",  # Only fetch first 10 videos (most recent)
        "--ignore-errors",  # Continue processing after errors (age-restricted, etc.)
        "--print", "%(id)s|%(title)s|%(duration)s",
        f"https://www.youtube.com/channel/{channel_id}/streams"
    ]
    
    logger.info(f"Fetching latest videos for channel {channel_id}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        # Log non-zero exit codes as warnings, not errors
        # yt-dlp may still have valid output even with exit code 1 (e.g., age-restricted videos)
        if result.returncode != 0:
            # Check if there's any stderr output to log
            stderr_msg = result.stderr.strip() if result.stderr else "unknown error"
            logger.warning(f"yt-dlp returned exit code {result.returncode} for {channel_id}: {stderr_msg}")
            logger.info(f"Attempting to process output despite non-zero exit code...")

        lines = result.stdout.strip().split("\n")
        
        if not lines or lines == ['']:
            logger.warning(f"No videos found for channel {channel_id}")
            return None
        
        logger.info(f"Found {len(lines)} videos for channel {channel_id}")
        
        # Process videos (yt-dlp returns most recent first)
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Parse format: video_id|title|duration
            # Note: title may contain | characters, so we split from the right
            parts = line.rsplit("|", 2)
            
            if len(parts) != 3:
                logger.debug(f"Skipping malformed line: {line[:100]}...")
                continue
            
            video_id = parts[0].strip()
            title = parts[1].strip()
            duration_str = parts[2].strip()
            
            # Handle "NA" or missing duration
            if not duration_str or duration_str.lower() == "na":
                logger.debug(f"Skipping {video_id}: no duration info (likely live/upcoming)")
                continue
            
            try:
                duration = int(duration_str)
            except ValueError:
                logger.debug(f"Skipping {video_id}: invalid duration '{duration_str}'")
                continue
            
            # Skip live streams (duration = 0 or None when live)
            if duration == 0:
                logger.debug(f"Skipping {video_id}: duration=0 (live stream or upcoming)")
                continue
            
            # Skip shorts (under 60 seconds)
            if duration < 60:
                logger.debug(f"Skipping {video_id}: duration={duration}s (short)")
                continue
            
            # Found a valid video
            logger.info(f"Found valid video: {video_id} - '{title}' ({duration}s)")
            return {
                "video_id": video_id,
                "title": title,
                "duration": duration
            }
        
        logger.warning(f"No valid VOD found for {channel_id} (all videos were shorts, live, or had no duration)")
        return None
        
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout fetching VOD for {channel_id}")
        return None
    except Exception as e:
        logger.error(f"Error fetching VOD for {channel_id}: {e}", exc_info=True)
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
    
    logger.info(f"Downloading {video_id} for channel {channel_id}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1hr timeout
        if result.returncode != 0:
            logger.error(f"yt-dlp download failed: {result.stderr.strip()}")
        # Find the downloaded file
        for f in output_dir.glob(f"{video_id}.*"):
            logger.info(f"Downloaded to: {f}")
            return str(f)
        logger.error(f"Downloaded file not found for {video_id}")
        return None
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout downloading {video_id}")
        return None
    except Exception as e:
        logger.error(f"Error downloading {video_id}: {e}", exc_info=True)
        return None


def segment_video(video_path, video_id, channel_id, clip_duration=300):
    """Cut video into N-minute segments"""
    if not video_path or not os.path.exists(video_path):
        logger.error(f"Video path does not exist: {video_path}")
        return []
    
    output_dir = UPLOAD_DIR / channel_id / "clips"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get duration
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", 
           "-of", "default=noprint_wrappers=1:nokey=1", video_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        total_duration = int(float(result.stdout.strip()))
        logger.info(f"Video duration: {total_duration}s")
    except Exception as e:
        logger.error(f"Failed to get video duration: {e}")
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
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5min per clip
            if clip_path.exists():
                clips.append(str(clip_path))
                logger.debug(f"Created clip: {clip_name}")
            else:
                logger.warning(f"Clip not created: {clip_name}")
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout creating clip {clip_name}")
        except Exception as e:
            logger.error(f"Error creating clip {clip_name}: {e}")
    
    return clips


def process_channel(conn, channel_id):
    """Check channel for new VODs and process them"""
    logger.info(f"Checking channel: {channel_id}")
    
    # Get latest VOD
    vod = get_latest_vod(channel_id)
    if not vod:
        logger.info(f"No valid VOD found for {channel_id}")
        return
    
    video_id = vod["video_id"]
    
    # Check if already processed
    cur = conn.execute("SELECT status FROM vods WHERE video_id = ?", (video_id,))
    row = cur.fetchone()
    
    if row and row[0] == "processed":
        logger.info(f"Already processed: {video_id}")
        return
    
    if row and row[0] == "downloaded":
        logger.info(f"Already downloaded, processing: {video_id}")
    else:
        # Log new VOD
        conn.execute("""
            INSERT INTO vods (video_id, channel_id, title, duration_seconds, status, downloaded_at)
            VALUES (?, ?, ?, ?, 'downloaded', ?)
        """, (video_id, channel_id, vod["title"], vod["duration"], datetime.now().isoformat()))
        conn.commit()
    
    # Download
    logger.info(f"Downloading: {vod['title']}")
    file_path = download_vod(video_id, channel_id)
    
    if not file_path:
        logger.error(f"Failed to download {video_id}")
        return
    
    # Update with file path
    conn.execute("UPDATE vods SET file_path = ? WHERE video_id = ?", (file_path, video_id))
    conn.commit()
    
    # Segment
    logger.info(f"Segmenting into 5-min clips...")
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
                    logger.warning(f"Invalid clip {clip_path} (duration={duration})")
                    break
            except Exception as e:
                all_clips_valid = False
                logger.warning(f"Could not verify clip {clip_path}: {e}")
                break
        
        if all_clips_valid:
            try:
                os.remove(file_path)
                logger.info(f"Deleted original to save space (verified {len(clips)} clips)")
            except Exception as e:
                logger.warning(f"Could not delete original: {e}")
        else:
            logger.warning(f"Keeping original due to invalid clips")
    else:
        logger.warning(f"Keeping original - no clips were created")
    
    logger.info(f"Done! Created {len(clips)} clips")


def main():
    logger.info(f"=== VOD Tracker started ===")
    
    # Init
    conn = init_db()
    channels = load_channels()
    
    logger.info(f"Tracking {len(channels)} channels: {channels}")
    
    # Process each channel
    for channel_id in channels:
        try:
            process_channel(conn, channel_id)
        except Exception as e:
            logger.error(f"Error processing {channel_id}: {e}", exc_info=True)
    
    logger.info("=== Done ===")
    conn.close()


if __name__ == "__main__":
    main()
