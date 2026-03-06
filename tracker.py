#!/usr/bin/env python3
"""
VOD Tracker - Monitors YouTube channels for live streams/VODs
Downloads, transcribes, analyzes with LLM, and extracts highlight clips
"""

import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import html
from datetime import datetime
from pathlib import Path
from typing import Optional

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
HIGHLIGHTS_DIR = DATA_DIR / "highlights"
READY_FOR_UPLOAD_DIR = DATA_DIR / "ready_for_upload"
DB_PATH = DATA_DIR / "tracker.db"
CHANNELS_FILE = WORKSPACE / "channels.json"  # channels.json is at root, not in data/

# Whisper settings
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "turbo")

# LLM settings
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "openai")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

# YouTube download settings - improved quality
# Use better format selection to ensure 1080p when available
YT_DLP_OPTS = [
    "yt-dlp",
    "--format", "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    "--merge-output-format", "mp4",
    "--extractor-args", "youtube:player_client=android",
]

# Video quality settings for encoding
VIDEO_CRF = 20  # Lower = better quality (18-23 is good, default is 23)
VIDEO_PRESET = "medium"  # Better quality than "fast"

# Allowed categories for validation
ALLOWED_CATEGORIES = {"drama", "funny", "discussion", "highlight"}


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
    # Add highlights table for LLM-scored clips
    conn.execute("""
        CREATE TABLE IF NOT EXISTS highlights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            clip_path TEXT NOT NULL,
            start_time INTEGER,
            end_time INTEGER,
            duration_seconds INTEGER,
            description TEXT,
            title TEXT,
            category TEXT,
            viral_score INTEGER,
            transcript_segment TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES vods(video_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_highlights_video_id ON highlights(video_id)
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
        "--playlist-end", "10",  # Only fetch first 10 videos (most recent)
        "--ignore-errors",  # Continue processing after errors (age-restricted, etc.)
        "--print", "%(id)s|%(title)s|%(duration)s",
        "--extractor-args", "youtube:player_client=android",
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


def get_video_duration(video_path: str) -> Optional[int]:
    """Get video duration in seconds using ffprobe"""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_path
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return int(float(result.stdout.strip()))
    except Exception as e:
        logger.error(f"Error getting video duration: {e}")
        return None


def transcribe_video(video_path: str) -> Optional[dict]:
    """
    Transcribe video using Whisper CLI
    
    Returns the parsed JSON transcript with timestamps
    """
    if not os.path.exists(video_path):
        logger.error(f"Video file not found: {video_path}")
        return None
    
    output_dir = tempfile.mkdtemp()
    output_dir = Path(output_dir)
    
    logger.info(f"  Transcribing with Whisper ({WHISPER_MODEL} model)...")
    
    cmd = [
        "whisper",
        video_path,
        "--model", WHISPER_MODEL,
        "--output_format", "json",
        "--output_dir", str(output_dir),
        "--word_timestamps", "True",
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2hr timeout
        if result.returncode != 0:
            logger.error(f"Whisper error: {result.stderr}")
            return None
        
        # Find the output JSON file
        video_name = Path(video_path).stem
        json_file = output_dir / f"{video_name}.json"
        
        if not json_file.exists():
            logger.error(f"Transcript JSON not found: {json_file}")
            return None
        
        with open(json_file) as f:
            transcript = json.load(f)
        
        logger.info(f"  Transcription complete: {len(transcript.get('segments', []))} segments")
        return transcript
        
    except subprocess.TimeoutExpired:
        logger.error("Whisper transcription timed out")
        return None
    except Exception as e:
        logger.error(f"Transcription error: {e}")
        return None


def format_transcript_for_llm(transcript: dict) -> str:
    """Format Whisper transcript into a readable format for the LLM"""
    lines = []
    for segment in transcript.get("segments", []):
        start = segment.get("start", 0)
        text = segment.get("text", "").strip()
        if text:
            mins = int(start // 60)
            secs = int(start % 60)
            lines.append(f"[{mins:02d}:{secs:02d}] {text}")
    
    return "\n".join(lines)


# System prompt for finding highlights
HIGHLIGHT_SYSTEM_PROMPT = """You are an expert at identifying the most engaging moments in video content, specifically for YouTube drama, commentary, and reaction videos.

Analyze the transcript and identify the BEST MOMENTS that would make great short clips. Focus on:

1. **Funny moments** - Jokes, reactions, unexpected humor, comedic timing
2. **Drama/Conflict** - Arguments, callouts, heated discussions, revelations
3. **Interesting discussions** - Hot takes, controversial opinions, deep insights
4. **Key highlights** - Viral-worthy moments, memorable quotes, shocking statements

For each moment, provide:
- Precise start and end timestamps (in seconds)
- A brief description of what happens
- A "viral_score" from 1-10 (how clip-worthy is this?)
- A suggested clip title

Output ONLY valid JSON with this structure:
{
  "highlights": [
    {
      "start_time": 123,
      "end_time": 180,
      "description": "Brief description of the moment",
      "viral_score": 8,
      "title": "Suggested clip title",
      "category": "drama|funny|discussion|highlight"
    }
  ]
}

Rules:
- Timestamps must be EXACT from the transcript
- Clips should be 30 seconds to 3 minutes long
- Prioritize moments with high engagement potential
- Focus on YouTube drama/commentary style content
- Return 5-15 highlights depending on content quality
- If the content is boring or has no good moments, return empty highlights array
"""


def call_llm(transcript_text: str) -> Optional[dict]:
    """Call LLM to analyze transcript and find highlights"""
    logger.info(f"  Analyzing transcript with {LLM_PROVIDER}/{LLM_MODEL}...")
    
    if LLM_PROVIDER == "openai":
        return _call_openai(transcript_text)
    elif LLM_PROVIDER == "anthropic":
        return _call_anthropic(transcript_text)
    elif LLM_PROVIDER == "minimax":
        return _call_minimax(transcript_text)
    elif LLM_PROVIDER == "ollama":
        return _call_ollama(transcript_text)
    else:
        logger.error(f"Unknown LLM provider: {LLM_PROVIDER}")
        return None


def _call_openai(transcript_text: str) -> Optional[dict]:
    """Call OpenAI API"""
    import urllib.request
    import urllib.error
    
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not set")
        return None
    
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": HIGHLIGHT_SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyze this transcript and find the best moments:\n\n{transcript_text}"}
        ],
        "temperature": 0.7,
        "max_tokens": 4000
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers
        )
        with urllib.request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode('utf-8'))
            content = result["choices"][0]["message"]["content"]
            return parse_llm_response(content)
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return None


def _call_anthropic(transcript_text: str) -> Optional[dict]:
    """Call Anthropic API"""
    import urllib.request
    import urllib.error
    
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return None
    
    url = "https://api.anthropic.com/v1/messages"
    payload = {
        "model": LLM_MODEL,
        "max_tokens": 4096,
        "system": HIGHLIGHT_SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": f"Analyze this transcript and find the best moments:\n\n{transcript_text}"}
        ]
    }
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01"
    }
    
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers
        )
        with urllib.request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode('utf-8'))
            content = result["content"][0]["text"]
            return parse_llm_response(content)
    except Exception as e:
        logger.error(f"Anthropic API error: {e}")
        return None


def _call_minimax(transcript_text: str) -> Optional[dict]:
    """Call MiniMax API"""
    import urllib.request
    import urllib.error
    
    api_key = os.environ.get("MINIMAX_API_KEY")
    group_id = os.environ.get("MINIMAX_GROUP_ID")
    
    if not api_key:
        logger.error("MINIMAX_API_KEY not set")
        return None
    
    # Map model names
    model_mapping = {
        "minimax-m2.1": "MiniMax-M2.1",
        "minimax-m2.5": "MiniMax-M2.5",
        "MiniMax-M2.1": "MiniMax-M2.1",
        "MiniMax-M2.5": "MiniMax-M2.5",
    }
    model_id = model_mapping.get(LLM_MODEL.lower(), LLM_MODEL)
    
    url = "https://api.minimax.io/v1/text/chatcompletion_v2"
    payload = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": HIGHLIGHT_SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyze this transcript and find the best moments:\n\n{transcript_text}"}
        ],
        "temperature": 0.7,
        "max_tokens": 4096
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers
        )
        with urllib.request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode('utf-8'))
            content = result["choices"][0]["message"]["content"]
            return parse_llm_response(content)
    except Exception as e:
        logger.error(f"MiniMax API error: {e}")
        return None


def _call_ollama(transcript_text: str) -> Optional[dict]:
    """Call local Ollama API"""
    import urllib.request
    import urllib.error
    
    api_url = os.environ.get("LLM_API_URL", "http://localhost:11434/api/generate")
    payload = {
        "model": LLM_MODEL,
        "prompt": f"{HIGHLIGHT_SYSTEM_PROMPT}\n\nAnalyze this transcript and find the best moments:\n\n{transcript_text}",
        "stream": False
    }
    
    try:
        req = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode('utf-8'),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=300) as response:
            result = json.loads(response.read().decode('utf-8'))
            content = result.get("response", "")
            return parse_llm_response(content)
    except Exception as e:
        logger.error(f"Ollama API error: {e}")
        return None


def parse_llm_response(content: str) -> Optional[dict]:
    """Parse LLM response to extract JSON"""
    content = content.strip()
    
    # Handle markdown code blocks
    if "```json" in content:
        match = re.search(r"```json\s*([\s\S]*?)\s*```", content)
        if match:
            content = match.group(1)
    elif "```" in content:
        match = re.search(r"```\s*([\s\S]*?)\s*```", content)
        if match:
            content = match.group(1)
    
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", content)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        logger.error("Failed to parse LLM response as JSON")
        logger.debug(f"Response: {content[:500]}...")
        return None


def validate_timestamp(value, default=0):
    """Validate and cast timestamp to int"""
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def validate_viral_score(value, default=5):
    """Validate viral_score is in range 1-10"""
    try:
        score = int(value)
        if 1 <= score <= 10:
            return score
        return default
    except (ValueError, TypeError):
        return default


def validate_category(value, default="highlight"):
    """Validate category is in allowed list"""
    if value and isinstance(value, str) and value.lower() in ALLOWED_CATEGORIES:
        return value.lower()
    return default


def sanitize_text(text: str) -> str:
    """Sanitize LLM-sourced text to prevent XSS"""
    if not text or not isinstance(text, str):
        return ""
    return html.escape(text.strip())


def sanitize_channel_id(channel_id: str) -> str:
    """Sanitize channel_id to prevent path traversal attacks"""
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '', channel_id)
    if not sanitized or sanitized in ('.', '..'):
        return "unknown"
    return sanitized


def extract_clip(video_path: str, start_time: int, end_time: int, output_path: str,
                 add_context: int = 5, video_duration: int = None) -> bool:
    """
    Extract a clip from the video using ffmpeg
    
    Args:
        video_path: Source video file
        start_time: Start time in seconds
        end_time: End time in seconds
        output_path: Output file path
        add_context: Add N seconds before/after for context
        video_duration: Video duration in seconds (for bounds checking)
    
    Returns:
        True if clip was created successfully
    """
    # Calculate actual start with context padding
    actual_start = max(0, start_time - add_context)
    actual_end = end_time + add_context
    
    if video_duration:
        actual_end = min(actual_end, video_duration)
    
    duration = actual_end - actual_start
    
    if duration <= 0:
        logger.warning(f"Invalid clip duration: {duration}s")
        return False
    
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(actual_start),
        "-i", video_path,
        "-t", str(duration),
        "-c:v", "libx264",
        "-preset", VIDEO_PRESET,
        "-crf", str(VIDEO_CRF),
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        output_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=300)
        if result.returncode == 0 and os.path.exists(output_path):
            return True
        else:
            logger.error(f"ffmpeg error: {result.stderr.decode()[:500]}")
            return False
    except Exception as e:
        logger.error(f"Error extracting clip: {e}")
        return False


def process_highlights(video_path: str, video_id: str, highlights: list, 
                       channel_id: str = "unknown", conn: sqlite3.Connection = None) -> list:
    """
    Extract highlight clips from video based on LLM analysis
    
    Returns list of created clip paths with metadata
    """
    if not highlights:
        logger.info("  No highlights to process")
        return []
    
    channel_id = sanitize_channel_id(channel_id)
    video_duration = get_video_duration(video_path)
    
    # Create output directory
    output_dir = HIGHLIGHTS_DIR / channel_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    created_clips = []
    
    # Sort by viral score (highest first)
    highlights = sorted(highlights, key=lambda x: validate_viral_score(x.get("viral_score", 0)), reverse=True)
    
    # Filter overlapping segments
    non_overlapping = []
    for h in highlights:
        start = validate_timestamp(h.get("start_time"), default=0)
        end = validate_timestamp(h.get("end_time"), default=0)
        
        if video_duration:
            start = max(0, min(start, max(0, video_duration - 5)))
            end = min(end, video_duration)
        
        if end <= start:
            continue
        
        # Check for overlap with existing
        overlaps = False
        for existing in non_overlapping:
            ex_start = existing.get("_validated_start", 0)
            ex_end = existing.get("_validated_end", 0)
            overlap_start = max(start, ex_start)
            overlap_end = min(end, ex_end)
            if overlap_start < overlap_end:
                overlap_duration = overlap_end - overlap_start
                clip_duration = min(end - start, ex_end - ex_start)
                if clip_duration > 0 and overlap_duration / clip_duration > 0.5:
                    overlaps = True
                    break
        
        if not overlaps:
            h["_validated_start"] = start
            h["_validated_end"] = end
            non_overlapping.append(h)
    
    # Limit to top 10 highlights
    highlights = non_overlapping[:10]
    
    for i, h in enumerate(highlights):
        start = h.get("_validated_start", 0)
        end = h.get("_validated_end", 0)
        
        title = sanitize_text(h.get("title", f"Highlight {i+1}"))
        description = sanitize_text(h.get("description", ""))
        category = validate_category(h.get("category"))
        viral_score = validate_viral_score(h.get("viral_score"))
        
        # Sanitize title for filename
        safe_title = re.sub(r'[^\w\s-]', '', title).strip()[:50]
        safe_title = re.sub(r'[-\s]+', '_', safe_title) or f"highlight_{i+1}"
        
        clip_name = f"{video_id}__{start}-{end}__{safe_title}.mp4"
        clip_path = output_dir / clip_name
        
        logger.info(f"  Extracting: {title} ({start}s-{end}s, score={viral_score})")
        
        if extract_clip(video_path, start, end, str(clip_path), video_duration=video_duration):
            created_clips.append({
                "path": str(clip_path),
                "title": title,
                "description": description,
                "category": category,
                "viral_score": viral_score,
                "start_time": start,
                "end_time": end,
                "duration": end - start
            })
            
            # Log to database
            if conn:
                conn.execute("""
                    INSERT INTO highlights 
                    (video_id, clip_path, start_time, end_time, duration_seconds, 
                     description, title, category, viral_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (video_id, str(clip_path), start, end, end - start,
                      description, title, category, viral_score))
                conn.commit()
    
    return created_clips


def copy_best_clips_to_upload(video_id: str, channel_id: str, clips: list, 
                               min_score: int = 7, max_clips: int = 5) -> list:
    """
    Copy the best scoring clips to the ready_for_upload folder
    
    Args:
        video_id: YouTube video ID
        channel_id: Channel identifier
        clips: List of clip dicts with viral_score
        min_score: Minimum viral_score to consider (default 7)
        max_clips: Maximum clips to copy (default 5)
    
    Returns:
        List of copied clip paths
    """
    if not clips:
        return []
    
    channel_id = sanitize_channel_id(channel_id)
    
    # Create upload folder
    upload_dir = READY_FOR_UPLOAD_DIR / channel_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    # Filter and sort clips:
    # 1. viral_score >= min_score
    # 2. Duration 30s - 3min (optimal for YouTube)
    # 3. Sort by viral_score descending
    good_clips = [
        c for c in clips
        if c.get("viral_score", 0) >= min_score
        and 30 <= c.get("duration", 0) <= 180
    ]
    
    # If no clips meet the criteria, take top clips by score
    if not good_clips:
        good_clips = sorted(clips, key=lambda x: x.get("viral_score", 0), reverse=True)[:max_clips]
    else:
        good_clips = sorted(good_clips, key=lambda x: x.get("viral_score", 0), reverse=True)[:max_clips]
    
    copied = []
    for clip in good_clips:
        src_path = Path(clip["path"])
        if not src_path.exists():
            logger.warning(f"Clip not found: {src_path}")
            continue
        
        # Create descriptive filename for upload
        safe_title = re.sub(r'[^\w\s-]', '', clip["title"]).strip()[:50]
        safe_title = re.sub(r'[-\s]+', '_', safe_title)
        
        dest_name = f"{video_id}__score{clip['viral_score']}__{safe_title}.mp4"
        dest_path = upload_dir / dest_name
        
        try:
            shutil.copy2(str(src_path), str(dest_path))
            copied.append(str(dest_path))
            logger.info(f"  Copied to upload folder: {dest_name}")
        except Exception as e:
            logger.error(f"Failed to copy clip: {e}")
    
    return copied


def run_full_pipeline(video_path: str, video_id: str, channel_id: str, 
                      conn: sqlite3.Connection) -> dict:
    """
    Run the full processing pipeline:
    1. Transcribe with Whisper
    2. Analyze with LLM to find highlights
    3. Extract highlight clips
    4. Copy best clips to upload folder
    
    Returns:
        Dict with results
    """
    logger.info(f"Running full pipeline for {video_id}")
    
    result = {
        "video_id": video_id,
        "transcribed": False,
        "analyzed": False,
        "highlights": [],
        "clips": [],
        "upload_clips": []
    }
    
    # Step 1: Transcribe
    transcript = transcribe_video(video_path)
    if not transcript:
        logger.error("  Failed to transcribe video")
        return result
    
    result["transcribed"] = True
    
    # Step 2: Analyze with LLM
    transcript_text = format_transcript_for_llm(transcript)
    analysis = call_llm(transcript_text)
    
    if not analysis:
        logger.error("  Failed to analyze transcript")
        return result
    
    result["analyzed"] = True
    highlights = analysis.get("highlights", [])
    result["highlights"] = highlights
    
    logger.info(f"  Found {len(highlights)} potential highlights")
    
    # Step 3: Extract clips
    if highlights:
        clips = process_highlights(video_path, video_id, highlights, channel_id, conn)
        result["clips"] = clips
        logger.info(f"  Created {len(clips)} highlight clips")
    
    # Step 4: Copy best clips to upload folder
    if result["clips"]:
        upload_clips = copy_best_clips_to_upload(video_id, channel_id, result["clips"])
        result["upload_clips"] = upload_clips
        logger.info(f"  Copied {len(upload_clips)} clips to upload folder")
    
    return result


def process_channel(conn, channel_id):
    """Check channel for new VODs and process them with full pipeline"""
    logger.info(f"Checking channel: {channel_id}")
    
    # Get latest VOD
    vod = get_latest_vod(channel_id)
    if not vod:
        logger.info(f"No valid VOD found for {channel_id}")
        return
    
    video_id = vod["video_id"]
    
    # Check if already processed
    cur = conn.execute("SELECT status, file_path FROM vods WHERE video_id = ?", (video_id,))
    row = cur.fetchone()
    
    if row and row[0] == "processed":
        logger.info(f"Already processed: {video_id}")
        return
    
    if row and row[0] == "file_missing":
        logger.info(f"Skipping {video_id} - file was missing")
        return
    
    if row and row[0] == "downloaded":
        logger.info(f"Already downloaded, processing: {video_id}")
        file_path = row[1]
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
    
    # Check if file exists (Issue 1: handle missing files gracefully)
    if not file_path or not os.path.exists(file_path):
        logger.warning(f"File not found for {video_id}: {file_path}")
        conn.execute("UPDATE vods SET status = 'file_missing' WHERE video_id = ?", (video_id,))
        conn.commit()
        return
    
    # Run full pipeline: transcribe -> analyze -> extract highlights
    logger.info(f"Processing with full pipeline...")
    result = run_full_pipeline(file_path, video_id, channel_id, conn)
    
    if result["clips"]:
        logger.info(f"Created {len(result['clips'])} highlights, {len(result['upload_clips'])} ready for upload")
    
    # Update status
    conn.execute("UPDATE vods SET status = 'processed', processed_at = ? WHERE video_id = ?",
                 (datetime.now().isoformat(), video_id))
    conn.commit()
    
    # Optionally delete original to save space (keep for re-processing)
    # Uncomment if disk space is critical:
    # if result["clips"]:
    #     try:
    #         os.remove(file_path)
    #         logger.info(f"Deleted original to save space")
    #     except Exception as e:
    #         logger.warning(f"Could not delete original: {e}")
    
    logger.info(f"Done! {len(result['clips'])} highlights created")


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
