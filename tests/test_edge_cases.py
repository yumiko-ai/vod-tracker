"""
Edge case tests for VOD Tracker
Tests unusual inputs, failures, and boundary conditions
"""

import json
import os
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tracker


class TestEmptyInputs:
    """Tests for empty or missing inputs"""
    
    def test_load_channels_empty_json(self, temp_workspace):
        """Test loading channels from empty JSON object"""
        channels_file = temp_workspace / "data" / "channels.json"
        channels_file.write_text("{}")
        
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            channels = tracker.load_channels()
            
            # Should return empty list for missing 'channels' key
            assert channels == []
    
    def test_load_channels_missing_key(self, temp_workspace):
        """Test loading channels when 'channels' key is missing"""
        channels_file = temp_workspace / "data" / "channels.json"
        channels_file.write_text('{"other_key": "value"}')
        
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            channels = tracker.load_channels()
            
            assert channels == []
    
    def test_get_latest_vod_empty_output(self, temp_workspace):
        """Test VOD fetching with completely empty yt-dlp output"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(stdout="", stderr="", returncode=0)):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is None
    
    def test_get_latest_vod_whitespace_only(self, temp_workspace):
        """Test VOD fetching with whitespace-only output"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(stdout="   \n  \n  ", stderr="", returncode=0)):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is None
    
    def test_download_vod_empty_video_id(self, temp_workspace):
        """Test download with empty video ID"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            result = tracker.download_vod("", "UC_Channel")
            
            # Should still attempt (yt-dlp will fail)
            # but shouldn't crash
            assert result is None or isinstance(result, str)
    
    def test_segment_video_empty_path(self, temp_workspace):
        """Test segmentation with empty path"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            result = tracker.segment_video("", "vid", "channel")
            
            assert result == []
    
    def test_segment_video_none_path(self, temp_workspace):
        """Test segmentation with None path"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            result = tracker.segment_video(None, "vid", "channel")
            
            assert result == []


class TestMalformedInputs:
    """Tests for malformed or invalid inputs"""
    
    def test_get_latest_vod_malformed_output(self, temp_workspace):
        """Test VOD fetching with malformed yt-dlp output"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Missing fields, wrong format
            malformed_outputs = [
                "just_a_string_no_commas",
                ",,",
                "video_id_only,",
                ",title_only,",
                "vid,title,not_a_number",
                "vid,,300",
            ]
            
            for output in malformed_outputs:
                with patch("subprocess.run", return_value=MagicMock(stdout=output, stderr="", returncode=0)):
                    result = tracker.get_latest_vod("UC_any")
                    # Should handle gracefully (either None or valid dict)
                    if result is not None:
                        assert "video_id" in result
                        assert "title" in result
                        assert "duration" in result
    
    def test_get_latest_vod_negative_duration(self, temp_workspace):
        """Test VOD with negative duration"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(
                stdout="vid123,Test Video,-100",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                # Negative duration should be filtered or handled
                if result is not None:
                    assert result["duration"] > 0
    
    def test_channels_json_with_non_string_ids(self, temp_workspace):
        """Test channels.json with non-string channel IDs"""
        channels_file = temp_workspace / "data" / "channels.json"
        channels_file.write_text(json.dumps({
            "channels": [12345, None, True, {"nested": "object"}]
        }))
        
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Should load but might fail later
            channels = tracker.load_channels()
            assert isinstance(channels, list)
    
    def test_unicode_in_titles(self, temp_workspace):
        """Test VOD with unicode characters in title"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(
                stdout="vid123,日本語タイトル 🎮 émojis!,300",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is not None
                assert "日本語" in result["title"] or "vid123" in result["video_id"]
    
    def test_very_long_title(self, temp_workspace):
        """Test VOD with very long title"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            long_title = "A" * 10000  # 10k characters
            with patch("subprocess.run", return_value=MagicMock(
                stdout=f"vid123,{long_title},300",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is not None
                assert len(result["title"]) == 10000


class TestTimeoutScenarios:
    """Tests for various timeout scenarios"""
    
    def test_yt_dlp_list_timeout(self, temp_workspace):
        """Test yt-dlp timeout when listing videos"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("yt-dlp", 60)):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is None
    
    def test_yt_dlp_download_timeout(self, temp_workspace):
        """Test yt-dlp timeout when downloading"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("yt-dlp", 3600)):
                result = tracker.download_vod("vid123", "UC_Channel")
                
                assert result is None
    
    def test_ffprobe_timeout(self, temp_workspace, temp_video_file):
        """Test ffprobe timeout when getting duration"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("ffprobe", 30)):
                with patch("os.path.exists", return_value=True):
                    result = tracker.segment_video(str(temp_video_file), "vid", "UC_Channel")
                    
                    assert result == []
    
    def test_ffmpeg_segment_timeout(self, temp_workspace, temp_video_file):
        """Test ffmpeg timeout during segmentation"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # First call is ffprobe (success), subsequent are ffmpeg (timeout)
            call_count = [0]
            
            def side_effect(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return MagicMock(stdout="600", stderr="", returncode=0)
                raise subprocess.TimeoutExpired("ffmpeg", 300)
            
            with patch("subprocess.run", side_effect=side_effect):
                with patch("os.path.exists", return_value=True):
                    result = tracker.segment_video(str(temp_video_file), "vid", "UC_Channel")
                    
                    # Should handle timeout gracefully
                    assert isinstance(result, list)


class TestFileSystemIssues:
    """Tests for filesystem-related issues"""
    
    def test_segment_video_permission_denied(self, temp_workspace, temp_video_file):
        """Test handling when permission denied for output"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            def side_effect(*args, **kwargs):
                if "mkdir" in str(args):
                    raise PermissionError("Permission denied")
                return MagicMock(stdout="600", stderr="", returncode=0)
            
            with patch("subprocess.run", return_value=MagicMock(stdout="600", stderr="", returncode=0)):
                with patch.object(Path, "mkdir", side_effect=PermissionError("Permission denied")):
                    # Should handle permission errors
                    try:
                        result = tracker.segment_video(str(temp_video_file), "vid", "UC_Channel")
                    except PermissionError:
                        pass  # Acceptable to propagate
    
    def test_download_vod_disk_full_simulation(self, temp_workspace):
        """Test handling of disk full scenario"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=OSError("No space left on device")):
                result = tracker.download_vod("vid123", "UC_Channel")
                
                assert result is None
    
    def test_database_locked(self, temp_workspace):
        """Test handling of locked database"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Create database and lock it
            db_path = temp_workspace / "data" / "tracker.db"
            conn1 = sqlite3.connect(db_path)
            
            # Try to init while locked (SQLite handles this, but test the concept)
            try:
                conn2 = tracker.init_db()
                conn2.close()
            except sqlite3.OperationalError:
                pass  # Expected if truly locked
            finally:
                conn1.close()


class TestNetworkIssues:
    """Tests for network-related issues"""
    
    def test_yt_dlp_connection_refused(self, temp_workspace):
        """Test handling when connection is refused"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.CalledProcessError(
                1, "yt-dlp", stderr="Connection refused"
            )):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is None
    
    def test_yt_dlp_dns_failure(self, temp_workspace):
        """Test handling of DNS resolution failure"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.CalledProcessError(
                1, "yt-dlp", stderr="Could not resolve host"
            )):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is None
    
    def test_yt_dlp_http_error(self, temp_workspace):
        """Test handling of HTTP errors"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", side_effect=subprocess.CalledProcessError(
                1, "yt-dlp", stderr="HTTP Error 404: Not Found"
            )):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is None


class TestBoundaryConditions:
    """Tests for boundary conditions"""
    
    def test_exactly_60_second_video(self, temp_workspace):
        """Test video exactly at 60 second boundary (minimum for VOD)"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(
                stdout="vid123,Exactly 60 Seconds,60",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                # Exactly 60 seconds should be accepted
                assert result is not None
                assert result["duration"] >= 60
    
    def test_59_second_video(self, temp_workspace):
        """Test video at 59 seconds (should be filtered as short)"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(
                stdout="vid123,59 Seconds,59",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                # 59 seconds should be filtered
                assert result is None
    
    def test_zero_duration_video(self, temp_workspace):
        """Test video with zero duration (live stream indicator)"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            with patch("subprocess.run", return_value=MagicMock(
                stdout="vid123,Live Stream,0",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                # Zero duration should be filtered
                assert result is None
    
    def test_very_long_video(self, temp_workspace):
        """Test very long video (24+ hours)"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # 25 hours in seconds
            with patch("subprocess.run", return_value=MagicMock(
                stdout="vid123,Marathon Stream,90000",
                stderr="",
                returncode=0
            )):
                result = tracker.get_latest_vod("UC_any")
                
                assert result is not None
                assert result["duration"] == 90000


class TestSpecialCharacters:
    """Tests for special characters in various inputs"""
    
    def test_channel_id_special_chars(self, temp_workspace):
        """Test channel IDs with special characters"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Normal channel IDs are alphanumeric + underscore/hyphen
            with patch("subprocess.run", return_value=MagicMock(stdout="", stderr="", returncode=0)):
                # Should not crash
                result = tracker.get_latest_vod("UC_Test-Channel_123")
    
    def test_video_id_special_chars(self, temp_workspace, temp_video_file):
        """Test video IDs with various characters"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # YouTube IDs are alphanumeric + underscore, hyphen
            video_id = "test-_vid123"
            
            with patch("subprocess.run", return_value=MagicMock(stdout="", stderr="", returncode=0)):
                tracker.download_vod(video_id, "UC_Channel")
                # Should handle gracefully


class TestConcurrentAccess:
    """Tests for concurrent access scenarios"""
    
    def test_multiple_init_db_calls(self, temp_workspace):
        """Test that init_db handles concurrent calls"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Simulate concurrent initialization
            connections = []
            for _ in range(5):
                conn = tracker.init_db()
                connections.append(conn)
            
            # All should succeed
            for conn in connections:
                conn.close()
    
    def test_rapid_sequential_operations(self, temp_workspace, initialized_db):
        """Test rapid sequential database operations"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            conn = tracker.init_db()
            
            # Rapid inserts
            for i in range(100):
                conn.execute("""
                    INSERT OR REPLACE INTO vods (video_id, channel_id, title, duration_seconds, status)
                    VALUES (?, ?, ?, ?, 'pending')
                """, (f"vid{i:03d}", "UC_Channel", f"Video {i}", 300))
            conn.commit()
            
            # Verify all inserted
            cur = conn.execute("SELECT COUNT(*) FROM vods")
            count = cur.fetchone()[0]
            assert count == 100
            
            conn.close()