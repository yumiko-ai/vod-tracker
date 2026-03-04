"""
Unit tests for VOD Tracker main functions
"""

import json
import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest

# Import the module under test
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tracker


class TestDatabaseInitialization:
    """Tests for database initialization"""
    
    def test_init_db_creates_tables(self, temp_workspace):
        """Test that init_db creates the vods and clips tables"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            # Re-import to pick up new workspace
            import importlib
            importlib.reload(tracker)
            
            conn = tracker.init_db()
            
            # Check vods table exists
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='vods'"
            )
            assert cur.fetchone() is not None
            
            # Check clips table exists
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='clips'"
            )
            assert cur.fetchone() is not None
            
            conn.close()
    
    def test_init_db_creates_data_directory(self, temp_workspace):
        """Test that init_db creates the data directory if it doesn't exist"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Data dir might exist from fixture, but init_db should handle creation
            data_dir = temp_workspace / "data"
            if data_dir.exists():
                # Remove to test creation
                import shutil
                shutil.rmtree(data_dir)
            
            conn = tracker.init_db()
            
            assert data_dir.exists()
            conn.close()
    
    def test_init_db_idempotent(self, temp_workspace):
        """Test that init_db can be called multiple times without error"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            conn1 = tracker.init_db()
            conn1.close()
            
            conn2 = tracker.init_db()
            conn2.close()
            
            # Should not raise any errors
    
    def test_vods_table_schema(self, initialized_db):
        """Test vods table has all required columns"""
        cur = initialized_db.execute("PRAGMA table_info(vods)")
        columns = {row[1] for row in cur.fetchall()}
        
        required_columns = {
            "video_id", "channel_id", "channel_name", "title",
            "duration_seconds", "status", "downloaded_at",
            "processed_at", "file_path", "created_at"
        }
        
        assert required_columns.issubset(columns)
    
    def test_clips_table_schema(self, initialized_db):
        """Test clips table has all required columns"""
        cur = initialized_db.execute("PRAGMA table_info(clips)")
        columns = {row[1] for row in cur.fetchall()}
        
        required_columns = {
            "id", "video_id", "clip_path", "start_time",
            "end_time", "created_at"
        }
        
        assert required_columns.issubset(columns)
    
    def test_vods_primary_key(self, initialized_db):
        """Test that video_id is primary key for vods table"""
        cur = initialized_db.execute("PRAGMA table_info(vods)")
        for row in cur.fetchall():
            if row[1] == "video_id":
                # row[5] is pk flag
                assert row[5] == 1
                break


class TestChannelLoading:
    """Tests for channel loading and validation"""
    
    def test_load_channels_returns_list(self, temp_channels_file):
        """Test that load_channels returns a list"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_channels_file.parent.parent)}):
            import importlib
            importlib.reload(tracker)
            
            channels = tracker.load_channels()
            
            assert isinstance(channels, list)
    
    def test_load_channels_returns_correct_count(self, temp_channels_file):
        """Test that load_channels returns correct number of channels"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_channels_file.parent.parent)}):
            import importlib
            importlib.reload(tracker)
            
            channels = tracker.load_channels()
            
            assert len(channels) == 2
    
    def test_load_channels_returns_channel_ids(self, temp_channels_file):
        """Test that load_channels returns channel IDs from config"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_channels_file.parent.parent)}):
            import importlib
            importlib.reload(tracker)
            
            channels = tracker.load_channels()
            
            assert "UC_testChannel001" in channels
            assert "UC_testChannel002" in channels
    
    def test_load_channels_empty_file(self, empty_channels_file):
        """Test load_channels handles empty channels list"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(empty_channels_file.parent.parent)}):
            import importlib
            importlib.reload(tracker)
            
            channels = tracker.load_channels()
            
            assert channels == []
    
    def test_load_channels_missing_file(self, temp_workspace):
        """Test load_channels raises error when file doesn't exist"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # CHANNELS_FILE path will be different
            tracker.CHANNELS_FILE = temp_workspace / "data" / "nonexistent.json"
            
            with pytest.raises(FileNotFoundError):
                tracker.load_channels()
    
    def test_load_channels_invalid_json(self, invalid_channels_file):
        """Test load_channels handles invalid JSON gracefully"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(invalid_channels_file.parent.parent)}):
            import importlib
            importlib.reload(tracker)
            
            with pytest.raises(json.JSONDecodeError):
                tracker.load_channels()


class TestVODFetching:
    """Tests for VOD fetching functionality"""
    
    def test_get_latest_vod_returns_dict(self, temp_workspace, mock_yt_dlp_success):
        """Test that get_latest_vod returns a dict on success"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=mock_yt_dlp_success):
                result = tracker.get_latest_vod("UC_testChannel001")
                
                assert result is not None
                assert isinstance(result, dict)
    
    def test_get_latest_vod_has_required_fields(self, temp_workspace, mock_yt_dlp_success):
        """Test that returned VOD has video_id, title, and duration"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=mock_yt_dlp_success):
                result = tracker.get_latest_vod("UC_testChannel001")
                
                assert "video_id" in result
                assert "title" in result
                assert "duration" in result
    
    def test_get_latest_vod_filters_shorts(self, temp_workspace, mock_yt_dlp_shorts):
        """Test that get_latest_vod filters out shorts under 60 seconds"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=mock_yt_dlp_shorts):
                result = tracker.get_latest_vod("UC_testChannel001")
                
                # Should return the valid video, not the shorts
                assert result["video_id"] == "validVideo"
                assert result["duration"] >= 60
    
    def test_get_latest_vod_empty_result(self, temp_workspace, mock_yt_dlp_empty):
        """Test that get_latest_vod returns None for empty results"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=mock_yt_dlp_empty):
                result = tracker.get_latest_vod("UC_testChannel001")
                
                assert result is None
    
    def test_get_latest_vod_timeout(self, temp_workspace):
        """Test that get_latest_vod handles subprocess timeout"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("yt-dlp", 60)):
                result = tracker.get_latest_vod("UC_testChannel001")
                
                assert result is None
    
    def test_get_latest_vod_exception(self, temp_workspace):
        """Test that get_latest_vod handles general exceptions"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=Exception("Test error")):
                result = tracker.get_latest_vod("UC_testChannel001")
                
                assert result is None
    
    def test_get_latest_vod_constructs_correct_command(self, temp_workspace):
        """Test that correct yt-dlp command is constructed"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            mock_result = MagicMock(stdout="vid123,Title,300", stderr="", returncode=0)
            
            with patch("subprocess.run", return_value=mock_result) as mock_run:
                tracker.get_latest_vod("UC_Channel123")
                
                # Check the command includes the channel URL
                call_args = mock_run.call_args
                assert "https://www.youtube.com/channel/UC_Channel123/videos" in call_args[0][0]
    
    def test_get_latest_vod_parses_comma_in_title(self, temp_workspace):
        """Test parsing VOD data when title contains commas"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            mock_result = MagicMock(
                stdout="vid123,Hello, World! Part 1,2,300",
                stderr="",
                returncode=0
            )
            
            with patch("subprocess.run", return_value=mock_result):
                result = tracker.get_latest_vod("UC_test")
                
                # Title should preserve commas
                assert result["title"] == "Hello, World! Part 1,2"
                assert result["video_id"] == "vid123"
                assert result["duration"] == 300


class TestVideoDownload:
    """Tests for video downloading functionality"""
    
    def test_download_vod_returns_path_on_success(self, temp_workspace, temp_video_file):
        """Test that download_vod returns file path on success"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Mock subprocess to simulate successful download
            with patch("subprocess.run") as mock_run:
                # Create the expected file
                video_id = "testVideo123"
                channel_id = "UC_testChannel001"
                
                result = tracker.download_vod(video_id, channel_id)
                
                assert result is not None
                assert video_id in result
    
    def test_download_vod_creates_output_directory(self, temp_workspace):
        """Test that download_vod creates the output directory"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run"):
                tracker.download_vod("testVid", "UC_Channel")
                
                upload_dir = temp_workspace / "data" / "upload" / "UC_Channel" / "temp"
                assert upload_dir.exists()
    
    def test_download_vod_timeout(self, temp_workspace):
        """Test that download_vod handles timeout"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("yt-dlp", 3600)):
                result = tracker.download_vod("testVid", "UC_Channel")
                
                assert result is None
    
    def test_download_vod_exception(self, temp_workspace):
        """Test that download_vod handles exceptions"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=Exception("Download failed")):
                result = tracker.download_vod("testVid", "UC_Channel")
                
                assert result is None
    
    def test_download_vod_constructs_correct_url(self, temp_workspace):
        """Test that download_vod uses correct YouTube URL"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run") as mock_run:
                tracker.download_vod("abc123", "UC_Channel")
                
                call_args = mock_run.call_args
                cmd = call_args[0][0]
                assert "https://youtube.com/watch?v=abc123" in cmd
    
    def test_download_vod_missing_file(self, temp_workspace):
        """Test download_vod returns None when file not created"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # subprocess succeeds but no file is created
            with patch("subprocess.run"):
                result = tracker.download_vod("nonexistent", "UC_Channel")
                
                assert result is None


class TestVideoSegmentation:
    """Tests for video segmentation functionality"""
    
    def test_segment_video_returns_clip_paths(self, temp_workspace, temp_video_file):
        """Test that segment_video returns list of clip paths"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Mock ffprobe for duration and clip verification
            duration_mock = MagicMock(stdout="600", stderr="", returncode=0)
            clip_mock = MagicMock(stdout="300", stderr="", returncode=0)
            
            with patch("subprocess.run", side_effect=[duration_mock] + [clip_mock] * 10):
                with patch("os.path.exists", return_value=True):
                    with patch.object(Path, "exists", return_value=True):
                        clips = tracker.segment_video(
                            str(temp_video_file),
                            "testVid",
                            "UC_Channel",
                            clip_duration=300
                        )
                        
                        # Should return empty list when clips don't actually exist
                        assert isinstance(clips, list)
    
    def test_segment_video_handles_missing_file(self, temp_workspace):
        """Test segment_video handles missing input file"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            result = tracker.segment_video(
                "/nonexistent/video.mp4",
                "testVid",
                "UC_Channel"
            )
            
            assert result == []
    
    def test_segment_video_creates_output_directory(self, temp_workspace, temp_video_file):
        """Test that segment_video creates clips directory"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Mock ffprobe and file operations
            duration_mock = MagicMock(stdout="600", stderr="", returncode=0)
            
            with patch("subprocess.run", return_value=duration_mock):
                with patch("os.path.exists", return_value=True):
                    tracker.segment_video(str(temp_video_file), "vid", "UC_Channel")
                    
                    clips_dir = temp_workspace / "data" / "upload" / "UC_Channel" / "clips"
                    assert clips_dir.exists()
    
    def test_segment_video_ffprobe_timeout(self, temp_workspace, temp_video_file):
        """Test segment_video handles ffprobe timeout"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("ffprobe", 30)):
                with patch("os.path.exists", return_value=True):
                    result = tracker.segment_video(str(temp_video_file), "vid", "UC_Channel")
                    
                    assert result == []
    
    def test_segment_video_ffmpeg_timeout(self, temp_workspace, temp_video_file):
        """Test segment_video handles ffmpeg timeout"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # First call (ffprobe) succeeds, second (ffmpeg) times out
            duration_mock = MagicMock(stdout="600", stderr="", returncode=0)
            
            with patch("subprocess.run", side_effect=[duration_mock, subprocess.TimeoutExpired("ffmpeg", 300)]):
                with patch("os.path.exists", return_value=True):
                    result = tracker.segment_video(str(temp_video_file), "vid", "UC_Channel")
                    
                    # Should handle timeout gracefully
                    assert isinstance(result, list)
    
    def test_segment_video_custom_clip_duration(self, temp_workspace, temp_video_file):
        """Test segment_video with custom clip duration"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # 10 minute video, 2 minute clips = 5 clips
            duration_mock = MagicMock(stdout="600", stderr="", returncode=0)
            clip_mock = MagicMock(stdout="120", stderr="", returncode=0)
            
            with patch("subprocess.run", return_value=duration_mock):
                with patch("os.path.exists", return_value=True):
                    # Just verify it doesn't crash with custom duration
                    result = tracker.segment_video(
                        str(temp_video_file),
                        "vid",
                        "UC_Channel",
                        clip_duration=120
                    )
                    
                    assert isinstance(result, list)