"""
Shared pytest fixtures for VOD Tracker tests
"""

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture
def temp_workspace(tmp_path):
    """Create a temporary workspace directory with data folder"""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data_dir = workspace / "data"
    data_dir.mkdir()
    upload_dir = data_dir / "upload"
    upload_dir.mkdir()
    
    # Set environment variable for workspace
    old_env = os.environ.get("VOD_WORKSPACE")
    os.environ["VOD_WORKSPACE"] = str(workspace)
    
    yield workspace
    
    # Cleanup
    if old_env:
        os.environ["VOD_WORKSPACE"] = old_env
    elif "VOD_WORKSPACE" in os.environ:
        del os.environ["VOD_WORKSPACE"]


@pytest.fixture
def temp_channels_file(temp_workspace):
    """Create a test channels.json file"""
    channels_file = temp_workspace / "data" / "channels.json"
    sample_channels = {
        "channels": [
            "UC_testChannel001",
            "UC_testChannel002"
        ],
        "check_interval_minutes": 60
    }
    channels_file.write_text(json.dumps(sample_channels))
    return channels_file


@pytest.fixture
def empty_channels_file(temp_workspace):
    """Create an empty channels.json file"""
    channels_file = temp_workspace / "data" / "channels.json"
    channels_file.write_text(json.dumps({"channels": []}))
    return channels_file


@pytest.fixture
def invalid_channels_file(temp_workspace):
    """Create an invalid channels.json file"""
    channels_file = temp_workspace / "data" / "channels.json"
    channels_file.write_text("not valid json")
    return channels_file


@pytest.fixture
def temp_db(temp_workspace):
    """Create a temporary database connection"""
    db_path = temp_workspace / "data" / "tracker.db"
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def initialized_db(temp_db):
    """Return an initialized database with schema"""
    temp_db.execute("""
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
    temp_db.execute("""
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
    temp_db.commit()
    return temp_db


@pytest.fixture
def sample_vod():
    """Sample VOD data"""
    return {
        "video_id": "testVideo123",
        "title": "Test Stream Title",
        "duration": 3600  # 1 hour
    }


@pytest.fixture
def sample_vods():
    """Multiple sample VODs"""
    return [
        {"video_id": "video001", "title": "First Stream", "duration": 3600},
        {"video_id": "video002", "title": "Second Stream", "duration": 7200},
        {"video_id": "video003", "title": "Third Stream", "duration": 1800},
    ]


@pytest.fixture
def mock_subprocess():
    """Mock subprocess.run for testing without actual yt-dlp/ffmpeg calls"""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            stdout="testVideo123,Test Stream Title,3600",
            stderr="",
            returncode=0
        )
        yield mock_run


@pytest.fixture
def mock_yt_dlp_success():
    """Mock successful yt-dlp output"""
    return MagicMock(
        stdout="testVideo123,Test Stream Title,3600\notherVideo456,Other Title,7200",
        stderr="",
        returncode=0
    )


@pytest.fixture
def mock_yt_dlp_shorts():
    """Mock yt-dlp output with shorts (under 60s) that should be filtered"""
    return MagicMock(
        stdout="short001,Short Video,30\nvalidVideo,Valid Video,300\nshort002,Another Short,45",
        stderr="",
        returncode=0
    )


@pytest.fixture
def mock_yt_dlp_empty():
    """Mock empty yt-dlp output"""
    return MagicMock(
        stdout="",
        stderr="",
        returncode=0
    )


@pytest.fixture
def mock_ffprobe_duration():
    """Mock ffprobe output with video duration"""
    return MagicMock(
        stdout="3600.5",
        stderr="",
        returncode=0
    )


@pytest.fixture
def temp_video_file(temp_workspace):
    """Create a temporary video file for testing segmentation"""
    channel_dir = temp_workspace / "data" / "upload" / "UC_testChannel001" / "temp"
    channel_dir.mkdir(parents=True, exist_ok=True)
    video_file = channel_dir / "testVideo123.mp4"
    video_file.write_bytes(b"fake video content")
    return video_file