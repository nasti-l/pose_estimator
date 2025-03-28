import numpy as np
from datetime import datetime

# Responsibility: record

class VideoRecorder:
    def __init__(self, fps=60):
        self.__fps: int = fps
        pass

    def record_video(self, duration_in_sec: int, fps: int=None) -> tuple[np.ndarray, int, int, str, str, bool]:
        if fps is None:
            fps = self.__fps
        start_time = datetime.now().isoformat()
        # TODO: record
        end_time = datetime.now().isoformat()
        frames = np.array(1)
        amount_of_frames = len(frames)
        if_corrupted = self.__validate_video(amount_of_frames, duration_in_sec)
        return frames, fps, amount_of_frames, start_time, end_time, if_corrupted

    def __validate_video(self, amount_of_frames: int, duration_in_sec: int) -> bool:
        if duration_in_sec//self.__fps != amount_of_frames:
            return False
        return True

#TODO: check if there is any other reason for corruption
