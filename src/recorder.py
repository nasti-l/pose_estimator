from typing import Tuple
import numpy as np


class VideoRecorder:
    def __init__(self, fps=60):
        self.__fps: int = fps
        pass

    def record_video(self, duration_in_sec: int) -> Tuple[np.ndarray, bool]:
        # TODO: record
        frames = np.ndarray[1,2,3]
        return frames, self.__validate_video(len(frames), duration_in_sec)

    def __validate_video(self, amount_of_frames: int, duration_in_sec: int) -> bool:
        if duration_in_sec//self.__fps != amount_of_frames:
            return False
        return True

