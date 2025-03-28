from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

import cv2
import numpy as np
import os
import pandas
import pandas as pd


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

    @abstractmethod
    def write_video_to_storage(self, frames: np.ndarray, fps: int) -> bool:
        pass

    @abstractmethod
    def read_video_from_storage(self, location: str | os.PathLike) -> np.ndarray:
        pass

    @abstractmethod
    def read_dataframe_from_storage(self, location: str | os.PathLike) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_dataframe_to_storage(self, data: pandas.DataFrame, file_name: str = "") -> str:
        pass

    def set_output_location(self, location: str | os.PathLike):
        self._output_location = location

    def get_output_location(self):
        return self._output_location

class LocalStorageManager(StorageManager):
    def __init__(self, location:str = "./output"):
        super().__init__(location)
        pass

    def write_video_to_storage(self, frames: np.ndarray, fps: int, file_name: str = "") -> str:
        output_folder = os.path.join(self._output_location, 'videos')
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        if file_name == "":
            file_name = f"recording_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            num_frames, height, width, channels = frames.shape
            fourcc = cv2.VideoWriter_fourcc(*'FFV1')
            file_location = (os.path.join(output_folder, file_name + ".avi"))
            out = cv2.VideoWriter(file_location, fourcc, fps, (width, height))
            for frame in frames:
                out.write(frame)
            out.release()
        except Exception as e:
            raise e
        if not os.path.exists(file_location):
            raise FileNotFoundError(f"File {file_location} wasn't found")
        return file_location

    def read_video_from_storage(self, location: str | os.PathLike) -> np.ndarray:
        if not os.path.exists(location):
            raise FileNotFoundError(f"Video file {location} not found")
        try:
            cap = cv2.VideoCapture(location)
            if not cap.isOpened():
                raise ValueError(f"Could not open video file {location}")
            frames = []
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                frames.append(frame)
            cap.release()
            if not frames:
                raise ValueError(f"No frames could be read from {location}")
            return np.array(frames)
        except Exception as e:
            raise e

    def write_dataframe_to_storage(self, data: pandas.DataFrame, file_name: str = "") -> str:
        output_folder = os.path.join(self._output_location, 'results')
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        if file_name == "":
            file_name = f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        file_location = os.path.join(output_folder, file_name) + ".parquet"
        try:
            data.to_parquet(file_location)
        except Exception as e:
            raise e
        if not os.path.exists(file_location):
            raise FileNotFoundError(f"File {file_location} wasn't found")
        return file_location

    def read_dataframe_from_storage(self, location: str | os.PathLike) -> pd.DataFrame:
        if not os.path.exists(location):
            raise FileNotFoundError(f"Data file {location} not found")
        try:
            df = pd.read_parquet(location)
            if df.empty:
                raise ValueError(f"No data found in {location}")
            return df
        except Exception as e:
            raise e



#TODO: separate storage relevant for offline and online

#TODO: restructure DB for easier querying




