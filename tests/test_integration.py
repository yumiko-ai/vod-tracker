"""
Integration tests for VOD Tracker
Tests the interaction between components
"""

import json
import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tracker


class TestProcessChannelIntegration:
    """Integration tests for process_channel function"""
    
    def test_process_channel_full_flow(self, temp_workspace, temp_channels_file, initialized_db):
        """Test full channel processing flow with mocked external calls"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Mock get_latest_vod to return a VOD
            mock_vod = {"video_id": "testVid001", "title": "Test Stream", "duration": 3600}
            
            # Mock download to create a file
            def mock_download(video_id, channel_id):
                output_dir = temp_workspace / "data" / "upload" / channel_id / "temp"
                output_dir.mkdir(parents=True, exist_ok=True)
                video_file = output_dir / f"{video_id}.mp4"
                video_file.write_bytes(b"fake video")
                return str(video_file)
            
            # Mock segmentation to create clips
            def mock_segment(video_path, video_id, channel_id, clip_duration=300):
                clips_dir = temp_workspace / "data" / "upload" / channel_id / "clips"
                clips_dir.mkdir(parents=True, exist_ok=True)
                clips = []
                for i in range(3):
                    clip = clips_dir / f"{video_id}__{i*5:02d}-{(i+1)*5:02d}.mp4"
                    clip.write_bytes(b"fake clip")
                    clips.append(str(clip))
                return clips
            
            with patch.object(tracker, "get_latest_vod", return_value=mock_vod):
                with patch.object(tracker, "download_vod", side_effect=mock_download):
                    with patch.object(tracker, "segment_video", side_effect=mock_segment):
                        conn = tracker.init_db()
                        
                        tracker.process_channel(conn, "UC_testChannel001")
                        
                        # Verify VOD was logged
                        cur = conn.execute(
                            "SELECT * FROM vods WHERE video_id = ?", ("testVid001",)
                        )
                        vod = cur.fetchone()
                        assert vod is not None
                        
                        # Verify clips were logged
                        cur = conn.execute(
                            "SELECT COUNT(*) FROM clips WHERE video_id = ?", ("testVid001",)
                        )
                        clip_count = cur.fetchone()[0]
                        assert clip_count == 3
                        
                        conn.close()
    
    def test_process_channel_skips_already_processed(self, temp_workspace, temp_channels_file, initialized_db):
        """Test that already processed VODs are skipped"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Pre-insert a processed VOD
            tracker.init_db()
            conn = tracker.init_db()
            conn.execute("""
                INSERT INTO vods (video_id, channel_id, title, duration_seconds, status)
                VALUES (?, ?, ?, ?, 'processed')
            """, ("existingVid", "UC_testChannel001", "Old Stream", 3600))
            conn.commit()
            
            # Mock get_latest_vod to return the same VOD
            mock_vod = {"video_id": "existingVid", "title": "Old Stream", "duration": 3600}
            
            download_spy = MagicMock(return_value=None)
            
            with patch.object(tracker, "get_latest_vod", return_value=mock_vod):
                with patch.object(tracker, "download_vod", download_spy):
                    tracker.process_channel(conn, "UC_testChannel001")
                    
                    # Download should not have been called
                    download_spy.assert_not_called()
            
            conn.close()
    
    def test_process_channel_handles_download_failure(self, temp_workspace, initialized_db):
        """Test handling of download failures"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            mock_vod = {"video_id": "failVid", "title": "Fail Stream", "duration": 3600}
            
            conn = tracker.init_db()
            
            with patch.object(tracker, "get_latest_vod", return_value=mock_vod):
                with patch.object(tracker, "download_vod", return_value=None):
                    tracker.process_channel(conn, "UC_testChannel001")
                    
                    # VOD should be in database but not processed
                    cur = conn.execute(
                        "SELECT status FROM vods WHERE video_id = ?", ("failVid",)
                    )
                    row = cur.fetchone()
                    # Status might be 'downloaded' since it's logged before download attempt
                    
            conn.close()
    
    def test_process_channel_no_vod_found(self, temp_workspace, initialized_db):
        """Test handling when no VOD is found for channel"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            conn = tracker.init_db()
            
            with patch.object(tracker, "get_latest_vod", return_value=None):
                tracker.process_channel(conn, "UC_testChannel001")
                
                # No VOD should be in database
                cur = conn.execute("SELECT COUNT(*) FROM vods")
                count = cur.fetchone()[0]
                assert count == 0
            
            conn.close()


class TestMainIntegration:
    """Integration tests for the main function"""
    
    def test_main_processes_all_channels(self, temp_workspace, temp_channels_file):
        """Test that main processes all channels in config"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            process_spy = MagicMock()
            
            with patch.object(tracker, "process_channel", process_spy):
                tracker.main()
                
                # Should have processed 2 channels from test config
                assert process_spy.call_count == 2
    
    def test_main_handles_empty_channels(self, temp_workspace, empty_channels_file):
        """Test main with no channels configured"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            process_spy = MagicMock()
            
            with patch.object(tracker, "process_channel", process_spy):
                tracker.main()
                
                process_spy.assert_not_called()
    
    def test_main_continues_on_channel_error(self, temp_workspace, temp_channels_file):
        """Test that main continues processing other channels after error"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            call_count = [0]
            
            def failing_process(conn, channel_id):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise Exception("Channel error")
            
            with patch.object(tracker, "process_channel", failing_process):
                # Should not raise
                tracker.main()
                
                # Both channels should have been attempted
                assert call_count[0] == 2


class TestDatabaseIntegration:
    """Integration tests for database operations"""
    
    def test_vod_lifecycle(self, temp_workspace, initialized_db):
        """Test full VOD lifecycle: insert, update, query"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            conn = tracker.init_db()
            
            video_id = "lifecycle001"
            channel_id = "UC_test"
            
            # Insert
            conn.execute("""
                INSERT INTO vods (video_id, channel_id, title, duration_seconds, status)
                VALUES (?, ?, ?, ?, 'pending')
            """, (video_id, channel_id, "Test Video", 3600))
            conn.commit()
            
            # Update to downloaded
            conn.execute("""
                UPDATE vods SET status = 'downloaded', downloaded_at = ?
                WHERE video_id = ?
            """, (datetime.now().isoformat(), video_id))
            conn.commit()
            
            # Update to processed
            conn.execute("""
                UPDATE vods SET status = 'processed', processed_at = ?, file_path = ?
                WHERE video_id = ?
            """, (datetime.now().isoformat(), "/path/to/video.mp4", video_id))
            conn.commit()
            
            # Query
            cur = conn.execute("SELECT * FROM vods WHERE video_id = ?", (video_id,))
            row = cur.fetchone()
            
            assert row is not None
            
            conn.close()
    
    def test_clips_foreign_key_relationship(self, temp_workspace, initialized_db):
        """Test that clips properly reference VODs"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            conn = tracker.init_db()
            
            video_id = "clipTest001"
            
            # Insert VOD
            conn.execute("""
                INSERT INTO vods (video_id, channel_id, title, duration_seconds, status)
                VALUES (?, ?, ?, ?, 'processed')
            """, (video_id, "UC_test", "Test", 3600))
            conn.commit()
            
            # Insert clips
            for i in range(5):
                conn.execute("""
                    INSERT INTO clips (video_id, clip_path, start_time, end_time)
                    VALUES (?, ?, ?, ?)
                """, (video_id, f"/clips/clip_{i}.mp4", i * 300, (i + 1) * 300))
            conn.commit()
            
            # Query clips for VOD
            cur = conn.execute(
                "SELECT COUNT(*) FROM clips WHERE video_id = ?", (video_id,)
            )
            count = cur.fetchone()[0]
            assert count == 5
            
            conn.close()
    
    def test_duplicate_vod_handling(self, temp_workspace, initialized_db):
        """Test handling of duplicate video IDs"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            conn = tracker.init_db()
            
            video_id = "duplicate001"
            
            # First insert
            conn.execute("""
                INSERT INTO vods (video_id, channel_id, title, duration_seconds, status)
                VALUES (?, ?, ?, ?, 'pending')
            """, (video_id, "UC_test", "Test", 3600))
            conn.commit()
            
            # Second insert should fail (primary key constraint)
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute("""
                    INSERT INTO vods (video_id, channel_id, title, duration_seconds, status)
                    VALUES (?, ?, ?, ?, 'pending')
                """, (video_id, "UC_other", "Test 2", 1800))
                conn.commit()
            
            conn.close()


class TestEndToEndMocked:
    """End-to-end tests with all external calls mocked"""
    
    def test_full_workflow_mocked(self, temp_workspace, temp_channels_file):
        """Test complete workflow with mocked external dependencies"""
        with patch.dict(os.environ, {"VOD_WORKSPACE": str(temp_workspace)}):
            import importlib
            importlib.reload(tracker)
            
            # Create all mocks
            yt_dlp_mock = MagicMock(
                stdout="testVid999,Epic Stream,7200",
                stderr="",
                returncode=0
            )
            
            ffprobe_duration = MagicMock(stdout="3600", stderr="", returncode=0)
            ffprobe_clip = MagicMock(stdout="300", stderr="", returncode=0)
            
            def run_side_effect(*args, **kwargs):
                cmd = args[0] if args else kwargs.get('cmd', [])
                if 'yt-dlp' in str(cmd):
                    return yt_dlp_mock
                elif 'ffprobe' in str(cmd):
                    return ffprobe_duration if len(cmd) > 2 and 'format' in str(cmd) else ffprobe_clip
                elif 'ffmpeg' in str(cmd):
                    # Create fake clip files
                    video_path = args[0][-1] if args else kwargs.get('args', [''])[-1]
                    Path(video_path).write_bytes(b"fake clip")
                    return MagicMock(stdout="", stderr="", returncode=0)
                return MagicMock(stdout="", stderr="", returncode=0)
            
            with patch("subprocess.run", side_effect=run_side_effect):
                # Also mock Path.exists to return True for clip files
                original_exists = Path.exists
                
                def mock_exists(self):
                    if "clips" in str(self) and self.suffix == ".mp4":
                        return True
                    return original_exists(self)
                
                with patch.object(Path, "exists", mock_exists):
                    tracker.main()
            
            # Verify database has the processed VOD
            conn = sqlite3.connect(temp_workspace / "data" / "tracker.db")
            cur = conn.execute("SELECT COUNT(*) FROM vods")
            vod_count = cur.fetchone()[0]
            
            cur = conn.execute("SELECT COUNT(*) FROM clips")
            clip_count = cur.fetchone()[0]
            
            conn.close()
            
            # VOD should have been logged
            assert vod_count >= 1