import logging
import pandas
import numpy as np
import pandas as pd

from abc import abstractmethod, ABC
from storage_manager import LocalStorageManager
from db_manager import PostgresDBManager
from ultralytics import YOLO


class DataProcessor(ABC):
    @abstractmethod
    def process_frames(self, frames: np.ndarray) -> pandas.DataFrame:
        pass
    @abstractmethod
    def get_name(self) -> str:
        pass

class YoloProcessor(DataProcessor):
    def __init__(self):
        self.__name = "yolo11_pose_estimation"
        self.__model = YOLO("yolo11n-pose.pt")

    def get_name(self) -> str:
        return self.__name

    def process_frames(self, frames: np.ndarray)-> pandas.DataFrame:
        return self.__get_pose_estimation(frames)

    def __get_pose_estimation(self, frames: np.ndarray) -> pandas.DataFrame:
        frame_dfs = []
        results = self.__model(frames)
        for frame_num, result in enumerate(results):
            df = result.to_df()
            df['frame'] = frame_num
            frame_dfs.append(df)
        return pd.concat(frame_dfs, ignore_index=True)

class PostProcessorManager():
    def __init__(self, storage: LocalStorageManager, db: PostgresDBManager):
        self.__storage = storage
        self.__db = db

    def run_post_processing(self, range_start: str, range_end: str) -> None:
        videos_to_process = self.__db.get_all_recordings_in_time_range(start_time=range_start,
                                                                           end_time=range_end) # get relevant videos locations and ids
        #TODO: replace with pipeline
        processors = [YoloProcessor()]
        for processor in processors:
            for video_id, video_location in videos_to_process:
                frames = self.__storage.read_video_from_storage(video_location)
                try:
                    df = processor.process_frames(frames)
                except Exception as e:
                    logging.error(f"Error processing frames {video_id} located at {video_location}: {e}")
                    continue
                try:
                    df_location = self.__storage.write_dataframe_to_storage(df)
                except Exception as e:
                    logging.error(f"Error writing results dataframe to storage {df_location}: {e}")
                    continue
                self.__db.update_results_for_video(processor_name=processor.get_name(),
                                                   results_location=df_location,
                                                   video_id=video_id)

#TODO add checking if writing return True

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

