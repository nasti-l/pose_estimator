from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

import cv2
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
        self._output_location = location
        os.makedirs(self._output_location, exist_ok=True)

    @abstractmethod
    def write_video_to_storage(self, frames: np.ndarray, fps: int) -> bool:
        pass

    @abstractmethod
    def read_from_storage(self, location: str) -> np.ndarray:
        pass

    def set_output_location(self, location: str | os.PathLike):
        self._output_location = location

    def get_output_location(self):
        return self._output_location

class LocalStorageManager(StorageManager):
    def __init__(self, location:str):
        super().__init__(location)
        pass

    def write_video_to_storage(self, frames: np.ndarray, fps: int, file_name: str = "") -> str:
        if file_name == "":
            #TODO update both timestemps for human eye
            f"recording_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            num_frames, height, width, channels = frames.shape
            fourcc = cv2.VideoWriter_fourcc(*'FFV1')
            file_location = (os.path.join(self._output_location, file_name + ".avi"))
            out = cv2.VideoWriter(file_location, fourcc, fps, (width, height))
            for frame in frames:
                out.write(frame)
            out.release()
        except Exception as e:
            raise e
        if not os.path.exists(file_location):
            raise FileNotFoundError(f"File {file_location} wasn't found")
        return file_location

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



