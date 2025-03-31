from datetime import datetime
from src.session_manager import SessionManager
from src.storage_manager import PreRecordingData
from dotenv import load_dotenv

def test_record_and_save_adds_recording_correctly():
    load_dotenv()
    time =datetime.now().isoformat()
    sm = SessionManager(session_start=time)
    data = PreRecordingData(
        duration_in_sec=2,
        activity="TestActivity",
        session_start=sm.get_session_name(),
        participant="TestUser"
    )
    before = sm.get_all_recordings()
    before_rows = before[0] if before else []
    recorded = sm.record_video(data)
    assert recorded is True
    saved = sm.save_last_recording()
    assert saved is True
    after = sm.get_all_recordings()
    after_rows = after[0] if after else []
    assert len(after_rows) == len(before_rows) + 1
    new_rows = [r for r in after_rows if r not in before_rows]
    assert len(new_rows) == 1
    new_row = new_rows[0]
    assert new_row[2] == data.activity
    assert new_row[3] == data.participant
    assert new_row[1].replace(' ', 'T') == data.session_start
    assert new_row[-1] == data.duration_in_sec