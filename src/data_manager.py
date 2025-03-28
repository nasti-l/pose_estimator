from abc import ABC, abstractmethod
from dataclasses import dataclass
import numpy as np
import os
import pandas

#TODO: check if more metadata is important
@dataclass(kw_only=True)
class PreRecordingData:
   duration_in_sec: int
   activity: str
   session_start: str
   participant: str

@dataclass(kw_only=True)
class PostRecordingData(PreRecordingData):
    fps: int
    amount_of_frames: int
    start_time: str
    end_time: str
    if_corrupted: bool

@dataclass(kw_only=True)
class RecordingMetaData(PostRecordingData):
    file_location: str | os.PathLike

#TODO change locations to pathlike

class StorageManager(ABC):
    def __init__(self, location: str):
        self.location = location
        os.makedirs(self.location, exist_ok=True)

    @abstractmethod
    def write_video_to_storage(self, frames: np.ndarray) -> bool:
        pass

    @abstractmethod
    def read_from_storage(self, location: str) -> np.ndarray:
        pass

class LocalStorageManager(StorageManager):
    def __init__(self, location:str):
        super().__init__(location)
        pass

    def write_video_to_storage(self, frames: np.ndarray) -> str:
        pass

    def write_dataframe_to_storage(self, data: pandas.DataFrame) -> str:
        pass

    def read_from_storage(self, location: str) -> np.ndarray:
        pass



class DBManager:
    def __init__(self, config: str):
        self.__init_db(config)

    def __init_db(self, config: str):
        #TODO create a db if not exists
        pass

    def save_metadata(self, metadata: PostRecordingData) -> bool:
        pass

    def get_id_to_location_map(self, query: str) -> {str:str}:
        pass

    def add_pose_estimation(self, pose_estimation_location: str, id: str):
        pass


#TODO: separate storage relevant for offline and online

#TODO: restructure DB for easier querying

#TODO: separate data_manager to diff modules



