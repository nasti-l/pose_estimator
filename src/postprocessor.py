from abc import abstractmethod, ABC
import pandas
import numpy as np
from data_manager import LocalStorageManager, DBManager


class DataProcessor(ABC):
    def __init__(self, storage_manager: LocalStorageManager):
        self.__storage_manager = storage_manager

    @abstractmethod
    def process_frames(self, frames: np.ndarray) -> pandas.DataFrame:
        pass

class YoloProcessor(DataProcessor):
    def process_frames(self, frames: np.ndarray)-> str:
        df = self.__get_pose_estimation(frames)
        return self.__storage_manager.write_dataframe_to_storage(df)

    def __get_pose_estimation(self, frames: np.ndarray) -> pandas.DataFrame:
        #TODO call YOLO
        pass


class PostProcessorManager():
    def __init__(self, storage_manager: LocalStorageManager, db_manager: DBManager):
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
