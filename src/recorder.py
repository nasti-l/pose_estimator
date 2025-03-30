import numpy as np
import time
import cv2
from abc import ABC, abstractmethod
from datetime import datetime


class VideoRecorder(ABC):
    def __init__(self, fps=60):
        self._fps: int = fps

    def set_fps(self, fps: int) -> None:
        self._fps = fps

    def get_fps(self) -> int:
        return self._fps

    @abstractmethod
    def record_video(self, duration_in_sec: int) -> tuple[np.ndarray, int, int, str, str, bool]:
        """
        Records a video for a given duration.

        Args:
            duration_in_sec (int): The duration of the recording in seconds.
        Returns:
            tuple: A tuple containing:
                - frames (np.ndarray): A NumPy array representing the video frames.
                - actual_fps (int): The actual frames per second used in recording.
                - num_frames (int): The total number of frames captured.
                - start_time (str): The recording start time in ISO format.
                - end_time (str): The recording end time in ISO format.
                - is_corrupted (bool): Whether the recording is corrupted.
        """
        pass


def validate_video(fps: int, amount_of_frames: int, duration: int) -> bool:
    if fps * duration != amount_of_frames: # Frame drop
        return True
    return False


class WebCamVideoRecorder(VideoRecorder):
    def __init__(self):
        super().__init__()

    def record_video(self, duration_in_sec: int) -> tuple[np.ndarray, int, int, str, str, bool]:
        try:
            frames, fps, start, end = self.__record_video(duration=duration_in_sec)
        except:
            raise
        amount_of_frames = len(frames)
        start_time = datetime.fromtimestamp(start).isoformat()
        end_time = datetime.fromtimestamp(end).isoformat()
        if_corrupted = validate_video(fps=fps,
                                             amount_of_frames=amount_of_frames,
                                             duration=duration_in_sec)
        return frames, fps, amount_of_frames, start_time, end_time, if_corrupted

    def __record_video(self, duration: int) -> tuple[np.ndarray, int, float, float]:
        fps = self._fps
        cap = cv2.VideoCapture(0)
        cap.set(cv2.CAP_PROP_FPS, fps)
        frames = []
        start = time.time()

        num_frames = duration * fps
        for _ in range(int(num_frames)):
            ret, frame = cap.read()
            if not ret:
                break
            frames.append(frame)
            cv2.waitKey(int(1000 / fps))

        frames = np.array(frames) # Control frame rate

        end = time.time()
        cap.release()
        return frames, fps, start, end
