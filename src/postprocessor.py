import pandas
import numpy as np
import pandas as pd

from abc import abstractmethod, ABC
from data_manager import LocalStorageManager
from db_manager import PostgresDBManager
from ultralytics import YOLO


class DataProcessor(ABC):
    def __init__(self, storage_manager: LocalStorageManager, db_manager: PostgresDBManager):
        self.__storage_manager = storage_manager
        self.__db_manager = PostgresDBManager()

    @abstractmethod
    def process_frames(self, frames: np.ndarray) -> pandas.DataFrame:
        pass

class YoloProcessor(DataProcessor):
    def __init__(self, storage_manager: LocalStorageManager, db_manager: PostgresDBManager):
        super().__init__(storage_manager, db_manager)
        self.__model = YOLO("yolo11n-pose.pt")

    def process_frames(self, frames: np.ndarray)-> str:
        df = self.__get_pose_estimation(frames)
        return self.__storage_manager.write_dataframe_to_storage(df)

    def __get_pose_estimation(self, frames: np.ndarray) -> pandas.DataFrame:
        frame_dfs = []
        results = self.__model(frames)
        for frame_num, result in enumerate(results):
            df = result.to_df()
            df['frame'] = frame_num
            frame_dfs.append(df)
        return pd.concat(frame_dfs, ignore_index=True)

class PostProcessorManager():
    def __init__(self, storage_manager: LocalStorageManager, db_manager: PostgresDBManager):
        self.__storage_manager = storage_manager
        self.__db_manager = db_manager

    def run_post_processing(self, query: str = "SELECT video_path WHERE session_start_time BETWEEN..") -> pandas.DataFrame:
        videos_to_process = self.__db_manager.get_id_to_location_map(query) # get relevant videos locations and ids
        processors = [YoloProcessor(self.__storage_manager)]
        for processor in processors:
            for id, location in videos_to_process:
                frames = self.__storage_manager.read_from_storage(location)
                data_location = processor.process_frames(frames)
                self.__db_manager.add_pose_estimation(data_location, id)

#TODO add checking if writing return True

if __name__ == '__main__':
    import os
    os.environ["PG_HOST"] = "localhost"
    os.environ["PG_PORT"] = "5432"
    os.environ["PG_USER"] = "postgres"
    os.environ["PG_DBNAME"] = "pose_est_db"
    os.environ["PG_PASS"] = "1234"
    db_manager = PostgresDBManager()
    from data_manager import RecordingMetaData
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

