import pandas
import numpy as np
import pandas as pd

from typing import List
from storage_manager import LocalStorageManager
from db_manager import PostgresDBManager
from ultralytics import YOLO
from ultralytics.engine.results import Results


class YoloProcessor:
    def __init__(self):
        self.__name = "yolo11_pose_estimation"
        self.__model = YOLO("yolo11n-pose.pt")

    def process(self, data: np.ndarray) -> List[Results]:
        return self.__model.process_frames(data)

    def frames_results_to_video_df(self, results: List[Results]) -> pandas.DataFrame:
        frame_dfs = []
        for frame_num, result in enumerate(results):
            df = result.to_df()
            df['frame'] = frame_num
            frame_dfs.append(df)
        return pd.concat(frame_dfs, ignore_index=True)


if __name__ == '__main__':
    import os
    os.environ["PG_HOST"] = "localhost"
    os.environ["PG_PORT"] = "5432"
    os.environ["PG_USER"] = "postgres"
    os.environ["PG_DBNAME"] = "pose_est_db"
    os.environ["PG_PASS"] = "1234"
    db_manager = PostgresDBManager()
    from storage_manager import RecordingMetaData
    video_metadata = RecordingMetaData(
        # PreRecordingData fields
        duration_in_sec=120,
        activity="walking",
        session_start="2025-03-29 10:00:00",
        participant="John Doe",

        # PostRecordingData fields
        fps=30,
        amount_of_frames=3600,  # 30fps * 120 seconds
        start_time="2025-03-29 10:05:00",
        end_time="2025-03-29 10:07:00",
        if_corrupted=False,

        # RecordingMetaData specific field
        file_location="/data/videos/walking_session_john_20250329.mp4"
    )
    db_manager.save_metadata_for_video(video_metadata)
    storage_manager = LocalStorageManager()

