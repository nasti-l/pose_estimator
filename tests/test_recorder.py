import numpy as np
from src.recorder import validate_video, WebCamVideoRecorder


def test_validate_video_perfect_recording():
    assert validate_video(fps=30, amount_of_frames=90, duration=3) is False


def test_validate_video_with_frame_drop():
    assert validate_video(fps=30, amount_of_frames=85, duration=3) is True

def test_webcam_recorder_fps_getter_setter():
    recorder = WebCamVideoRecorder()
    default_fps = recorder.get_fps()
    assert isinstance(default_fps, int)

    recorder.set_fps(24)
    assert recorder.get_fps() == 24

def test_record_real_video_short():
    recorder = WebCamVideoRecorder()
    recorder.set_fps(10)

    duration = 2
    frames, fps, num_frames, start_time, end_time, is_corrupted = recorder.record_video(duration)

    assert isinstance(frames, np.ndarray)
    assert frames.ndim == 4  # [num_frames, height, width, channels]
    assert frames.shape[0] == num_frames
    assert fps > 0
    assert isinstance(start_time, str)
    assert isinstance(end_time, str)
    assert isinstance(is_corrupted, bool)