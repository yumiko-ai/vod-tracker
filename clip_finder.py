#!/usr/bin/env python3
"""
Clip Finder - Finds the "best bits" in VODs
Transcribes with Whisper, analyzes with LLM, extracts highlight clips
"""

import html
import json
import os
import re
import sqlite3
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Config - Use environment variables with fallbacks
WORKSPACE = Path(os.environ.get("VOD_WORKSPACE", Path(__file__).parent))
DATA_DIR = WORKSPACE / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR = DATA_DIR / "upload"
HIGHLIGHTS_DIR = DATA_DIR / "highlights"
DB_PATH = DATA_DIR / "tracker.db"

# Whisper settings
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "turbo")  # turbo is fast and accurate
WHISPER_OUTPUT_FORMAT = "json"

# LLM settings - supports OpenAI, Anthropic, MiniMax, or local via Ollama
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "openai")  # openai, anthropic, minimax, ollama
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")  # gpt-4o-mini, claude-3-haiku, minimax-m2.1, llama3.2
LLM_API_KEY = os.environ.get("OPENAI_API_KEY", "") or os.environ.get("ANTHROPIC_API_KEY", "") or os.environ.get("MINIMAX_API_KEY", "")
LLM_API_URL = os.environ.get("LLM_API_URL", "http://localhost:11434/api/generate")

# Allowed categories for validation
ALLOWED_CATEGORIES = {"drama", "funny", "discussion", "highlight"}

# Minimum valid video duration (seconds)
MIN_VIDEO_DURATION = 5


def sanitize_channel_id(channel_id: str) -> str:
    """
    Sanitize channel_id to prevent path traversal attacks.
    Only allows alphanumeric, underscore, and hyphen characters.
    """
    # Only allow safe characters
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '', channel_id)
    # Prevent empty or dot-only paths
    if not sanitized or sanitized in ('.', '..'):
        return "unknown"
    return sanitized


def validate_timestamp(value, default=0):
    """
    Validate and cast timestamp to int.
    Handles string, float, int, and None values.
    """
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def validate_viral_score(value, default=5):
    """
    Validate viral_score is in range 1-10.
    Returns default if invalid.
    """
    try:
        score = int(value)
        if 1 <= score <= 10:
            return score
        return default
    except (ValueError, TypeError):
        return default


def validate_category(value, default="highlight"):
    """
    Validate category is in allowed list.
    Returns default if invalid.
    """
    if value and isinstance(value, str) and value.lower() in ALLOWED_CATEGORIES:
        return value.lower()
    return default


def sanitize_text(text: str) -> str:
    """
    Sanitize LLM-sourced text to prevent XSS.
    Escapes HTML characters.
    """
    if not text or not isinstance(text, str):
        return ""
    return html.escape(text.strip())


# System prompt for finding highlights - tuned for YouTube drama/commentary content
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


def init_db():
    """Initialize/extend SQLite database with highlights table"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    
    # Add highlights table
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
    
    # Add index for quick lookups
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_highlights_video_id ON highlights(video_id)
    """)
    
    conn.commit()
    return conn


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
        print(f"Error getting video duration: {e}")
        return None


def transcribe_video(video_path: str, output_dir: Optional[str] = None) -> Optional[dict]:
    """
    Transcribe video using Whisper CLI
    
    Returns the parsed JSON transcript with timestamps
    """
    if not os.path.exists(video_path):
        print(f"Video file not found: {video_path}")
        return None
    
    output_dir = output_dir or tempfile.mkdtemp()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"  Transcribing with Whisper ({WHISPER_MODEL} model)...")
    
    cmd = [
        "whisper",
        video_path,
        "--model", WHISPER_MODEL,
        "--output_format", WHISPER_OUTPUT_FORMAT,
        "--output_dir", str(output_dir),
        "--word_timestamps", "True",  # Get precise word-level timestamps
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2hr timeout
        if result.returncode != 0:
            print(f"Whisper error: {result.stderr}")
            return None
        
        # Find the output JSON file
        video_name = Path(video_path).stem
        json_file = output_dir / f"{video_name}.json"
        
        if not json_file.exists():
            print(f"Transcript JSON not found: {json_file}")
            return None
        
        with open(json_file) as f:
            transcript = json.load(f)
        
        print(f"  Transcription complete: {len(transcript.get('segments', []))} segments")
        return transcript
        
    except subprocess.TimeoutExpired:
        print("Whisper transcription timed out")
        return None
    except Exception as e:
        print(f"Transcription error: {e}")
        return None


def format_transcript_for_llm(transcript: dict) -> str:
    """Format Whisper transcript into a readable format for the LLM"""
    lines = []
    for segment in transcript.get("segments", []):
        start = segment.get("start", 0)
        text = segment.get("text", "").strip()
        if text:
            # Format: [MM:SS] text
            mins = int(start // 60)
            secs = int(start % 60)
            lines.append(f"[{mins:02d}:{secs:02d}] {text}")
    
    return "\n".join(lines)


def call_llm(transcript_text: str, provider: str = None, model: str = None) -> Optional[dict]:
    """
    Call LLM to analyze transcript and find highlights
    
    Supports: openai, anthropic, minimax, ollama (local)
    """
    provider = provider or LLM_PROVIDER
    model = model or LLM_MODEL
    
    print(f"  Analyzing transcript with {provider}/{model}...")
    
    if provider == "openai":
        return _call_openai(transcript_text, model)
    elif provider == "anthropic":
        return _call_anthropic(transcript_text, model)
    elif provider == "minimax":
        return _call_minimax(transcript_text, model)
    elif provider == "ollama":
        return _call_ollama(transcript_text, model)
    else:
        print(f"Unknown LLM provider: {provider}")
        return None


def _call_openai(transcript_text: str, model: str) -> Optional[dict]:
    """Call OpenAI API"""
    import urllib.request
    import urllib.error
    
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY not set")
        return None
    
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": model,
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
        print(f"OpenAI API error: {e}")
        return None


def _call_anthropic(transcript_text: str, model: str) -> Optional[dict]:
    """Call Anthropic API"""
    import urllib.request
    import urllib.error
    
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set")
        return None
    
    url = "https://api.anthropic.com/v1/messages"
    payload = {
        "model": model,
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
        print(f"Anthropic API error: {e}")
        return None


def _call_minimax(transcript_text: str, model: str) -> Optional[dict]:
    """Call MiniMax API"""
    import urllib.request
    import urllib.error
    
    api_key = os.environ.get("MINIMAX_API_KEY")
    group_id = os.environ.get("MINIMAX_GROUP_ID")
    
    if not api_key:
        print("MINIMAX_API_KEY not set")
        return None
    
    if not group_id:
        print("MINIMAX_GROUP_ID not set")
        return None
    
    # Map model names to MiniMax model identifiers
    model_mapping = {
        "minimax-m2.1": "MiniMax-M2.1",
        "minimax-m2.5": "MiniMax-M2.5",
        "MiniMax-M2.1": "MiniMax-M2.1",
        "MiniMax-M2.5": "MiniMax-M2.5",
    }
    model_id = model_mapping.get(model.lower(), model)
    
    url = "https://api.minimax.chat/v1/text/chatcompletion_v2"
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
        "Authorization": f"Bearer {api_key}",
        "X-Group-Id": group_id
    }
    
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers
        )
        with urllib.request.urlopen(req, timeout=120) as response:
            result = json.loads(response.read().decode('utf-8'))
            # MiniMax response format is similar to OpenAI
            content = result["choices"][0]["message"]["content"]
            return parse_llm_response(content)
    except Exception as e:
        print(f"MiniMax API error: {e}")
        return None


def _call_ollama(transcript_text: str, model: str) -> Optional[dict]:
    """Call local Ollama API"""
    import urllib.request
    import urllib.error
    
    payload = {
        "model": model,
        "prompt": f"{HIGHLIGHT_SYSTEM_PROMPT}\n\nAnalyze this transcript and find the best moments:\n\n{transcript_text}",
        "stream": False
    }
    
    try:
        req = urllib.request.Request(
            LLM_API_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=300) as response:
            result = json.loads(response.read().decode('utf-8'))
            content = result.get("response", "")
            return parse_llm_response(content)
    except Exception as e:
        print(f"Ollama API error: {e}")
        return None


def parse_llm_response(content: str) -> Optional[dict]:
    """Parse LLM response to extract JSON"""
    # Try to find JSON in the response
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
        # Try to extract just the JSON object
        match = re.search(r"\{[\s\S]*\}", content)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        print(f"Failed to parse LLM response as JSON")
        print(f"Response: {content[:500]}...")
        return None


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
    
    # Calculate duration including end context padding
    actual_end = end_time + add_context
    
    # Clamp end to video duration if known
    if video_duration:
        actual_end = min(actual_end, video_duration)
    
    duration = actual_end - actual_start
    
    # Skip if duration is invalid
    if duration <= 0:
        print(f"Invalid clip duration: {duration}s (start={actual_start}, end={actual_end})")
        return False
    
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(actual_start),
        "-i", video_path,
        "-t", str(duration),
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",  # Better quality for highlights
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
            print(f"ffmpeg error: {result.stderr.decode()[:500]}")
            return False
    except Exception as e:
        print(f"Error extracting clip: {e}")
        return False


def process_highlights(video_path: str, video_id: str, highlights: list, 
                       channel_id: str = "unknown", conn: sqlite3.Connection = None) -> list:
    """
    Extract highlight clips from video
    
    Returns list of created clip paths
    """
    if not highlights:
        print("  No highlights to process")
        return []
    
    # Sanitize channel_id to prevent path traversal
    channel_id = sanitize_channel_id(channel_id)
    
    # Get video duration for validation
    video_duration = get_video_duration(video_path)
    if not video_duration:
        print("  Warning: Could not get video duration, proceeding anyway")
    
    # Create output directory
    output_dir = HIGHLIGHTS_DIR / channel_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    created_clips = []
    
    # Sort by viral score (highest first) and dedupe overlapping segments
    highlights = sorted(highlights, key=lambda x: validate_viral_score(x.get("viral_score", 0)), reverse=True)
    
    # Filter overlapping segments (keep higher scoring ones)
    non_overlapping = []
    for h in highlights:
        # Validate and cast timestamps
        start = validate_timestamp(h.get("start_time"), default=0)
        end = validate_timestamp(h.get("end_time"), default=0)
        
        # Validate timestamps are within video bounds
        if video_duration:
            # Clamp start to valid range (never negative, at least 10s from end for short videos)
            start = max(0, min(start, max(0, video_duration - MIN_VIDEO_DURATION)))
            end = min(end, video_duration)
        
        # Skip if end <= start after validation
        if end <= start:
            continue
        
        # Check for overlap with existing
        overlaps = False
        for existing in non_overlapping:
            ex_start = existing.get("_validated_start", 0)
            ex_end = existing.get("_validated_end", 0)
            # Check if more than 50% overlap
            overlap_start = max(start, ex_start)
            overlap_end = min(end, ex_end)
            if overlap_start < overlap_end:
                overlap_duration = overlap_end - overlap_start
                clip_duration = min(end - start, ex_end - ex_start)
                if clip_duration > 0 and overlap_duration / clip_duration > 0.5:
                    overlaps = True
                    break
        
        if not overlaps:
            # Store validated timestamps for overlap checking
            h["_validated_start"] = start
            h["_validated_end"] = end
            non_overlapping.append(h)
    
    # Limit to top 10 highlights
    highlights = non_overlapping[:10]
    
    for i, h in enumerate(highlights):
        # Use validated timestamps
        start = h.get("_validated_start", 0)
        end = h.get("_validated_end", 0)
        
        # Sanitize LLM-sourced text fields
        title = sanitize_text(h.get("title", f"Highlight {i+1}"))
        description = sanitize_text(h.get("description", ""))
        
        # Validate structured fields
        category = validate_category(h.get("category"))
        viral_score = validate_viral_score(h.get("viral_score"))
        
        # Sanitize title for filename (additional safety for filesystem)
        safe_title = re.sub(r'[^\w\s-]', '', title).strip()[:50]
        safe_title = re.sub(r'[-\s]+', '_', safe_title) or f"highlight_{i+1}"
        
        clip_name = f"{video_id}__{start}-{end}__{safe_title}.mp4"
        clip_path = output_dir / clip_name
        
        print(f"  Extracting: {title} ({start}s-{end}s, score={viral_score})")
        
        if extract_clip(video_path, start, end, str(clip_path), video_duration=video_duration):
            created_clips.append(str(clip_path))
            
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


def find_best_bits(video_path: str, video_id: str, channel_id: str = "unknown",
                   conn: sqlite3.Connection = None, keep_transcript: bool = False) -> dict:
    """
    Main pipeline: Transcribe -> Analyze -> Extract highlights
    
    Args:
        video_path: Path to video file
        video_id: YouTube video ID
        channel_id: Channel identifier for organization
        conn: Database connection (optional)
        keep_transcript: Keep transcript JSON file
    
    Returns:
        Dict with highlights and created clips
    """
    print(f"\n=== Finding Best Bits for {video_id} ===")
    
    result = {
        "video_id": video_id,
        "transcript": None,
        "highlights": [],
        "clips": []
    }
    
    # Step 1: Transcribe
    transcript = transcribe_video(video_path)
    if not transcript:
        print("  Failed to transcribe video")
        return result
    
    result["transcript"] = transcript
    
    # Save transcript if requested
    if keep_transcript:
        transcript_dir = DATA_DIR / "transcripts"
        transcript_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = transcript_dir / f"{video_id}.json"
        with open(transcript_path, 'w') as f:
            json.dump(transcript, f, indent=2, ensure_ascii=False)
        print(f"  Saved transcript to {transcript_path}")
    
    # Step 2: Analyze with LLM
    transcript_text = format_transcript_for_llm(transcript)
    analysis = call_llm(transcript_text)
    
    if not analysis:
        print("  Failed to analyze transcript")
        return result
    
    highlights = analysis.get("highlights", [])
    result["highlights"] = highlights
    
    print(f"  Found {len(highlights)} potential highlights")
    
    # Step 3: Extract clips
    if highlights:
        clips = process_highlights(video_path, video_id, highlights, channel_id, conn)
        result["clips"] = clips
        print(f"  Created {len(clips)} highlight clips")
    
    print("=== Done ===\n")
    return result


def process_from_db(conn: sqlite3.Connection, limit: int = 5, reprocess: bool = False):
    """
    Process VODs from database that haven't been analyzed yet
    
    Args:
        conn: Database connection
        limit: Max number of VODs to process
        reprocess: Reprocess even if highlights exist
    """
    # Find VODs with files but no highlights
    if reprocess:
        query = """
            SELECT v.video_id, v.file_path, v.channel_id 
            FROM vods v
            WHERE v.file_path IS NOT NULL AND v.status = 'processed'
            ORDER BY v.created_at DESC
            LIMIT ?
        """
    else:
        query = """
            SELECT v.video_id, v.file_path, v.channel_id 
            FROM vods v
            LEFT JOIN highlights h ON v.video_id = h.video_id
            WHERE v.file_path IS NOT NULL 
              AND v.status = 'processed'
              AND h.id IS NULL
            ORDER BY v.created_at DESC
            LIMIT ?
        """
    
    cur = conn.execute(query, (limit,))
    vods = cur.fetchall()
    
    print(f"Found {len(vods)} VODs to process")
    
    for video_id, file_path, channel_id in vods:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        
        find_best_bits(file_path, video_id, channel_id, conn)


# CLI interface
def main():
    import argparse
    
    global LLM_MODEL, LLM_PROVIDER, WHISPER_MODEL
    
    parser = argparse.ArgumentParser(description="Find the best bits in VODs")
    parser.add_argument("video", nargs="?", help="Video file to process")
    parser.add_argument("--video-id", help="YouTube video ID (for database)")
    parser.add_argument("--channel", default="unknown", help="Channel ID for organization")
    parser.add_argument("--process-db", action="store_true", help="Process unprocessed VODs from database")
    parser.add_argument("--limit", type=int, default=5, help="Max VODs to process from DB")
    parser.add_argument("--keep-transcript", action="store_true", help="Save transcript JSON")
    parser.add_argument("--reprocess", action="store_true", help="Reprocess already processed VODs")
    parser.add_argument("--model", default=LLM_MODEL, help="LLM model to use")
    parser.add_argument("--provider", default=LLM_PROVIDER, help="LLM provider (openai, anthropic, minimax, ollama)")
    parser.add_argument("--whisper-model", default=WHISPER_MODEL, help="Whisper model size")
    
    args = parser.parse_args()
    
    # Update global settings from args
    LLM_MODEL = args.model
    LLM_PROVIDER = args.provider
    WHISPER_MODEL = args.whisper_model
    
    # Initialize database
    conn = init_db()
    
    if args.process_db:
        process_from_db(conn, args.limit, args.reprocess)
    elif args.video:
        video_id = args.video_id or Path(args.video).stem
        find_best_bits(args.video, video_id, args.channel, conn, args.keep_transcript)
    else:
        parser.print_help()
    
    conn.close()


if __name__ == "__main__":
    main()