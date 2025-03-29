from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

import cv2
import numpy as np
import os
import pandas
import pandas as pd


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

    def __prepare_file_location(self, file_name: str, file_extension: str, folder: str) -> str:
        output_folder = os.path.join(self._output_location, folder)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        if file_name == "":
            file_name = f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return os.path.join(output_folder, file_name) + file_extension

    def __save_and_verify(self, file_location: str, save_func: callable) -> str:
        try:
            save_func()
        except Exception as e:
            raise e
        if not os.path.exists(file_location):
            raise FileNotFoundError(f"File {file_location} wasn't found")
        return file_location

    def write_video_to_storage(self, frames: np.ndarray, fps: int, file_name: str = "") -> str:
        file_location = self.__prepare_file_location(file_name=file_name,
                                                     file_extension=".avi",
                                                     folder="videos")
        def save_func():
            num_frames, height, width, channels = frames.shape
            fourcc = cv2.VideoWriter_fourcc(*'FFV1')
            out = cv2.VideoWriter(file_location, fourcc, fps, (width, height))
            for frame in frames:
                out.write(frame)
            out.release()
        return self.__save_and_verify(file_location, save_func)

    def write_dataframe_to_storage(self, data: pandas.DataFrame, file_name: str = "") -> str:
        file_location = self.__prepare_file_location(file_name=file_name,
                                                     file_extension=".parquet",
                                                     folder="results")
        save_func = lambda: data.to_parquet(file_location)
        return self.__save_and_verify(file_location, save_func)

    def write_image_to_storage(self, image: np.ndarray, file_name: str = "") -> str:
        file_location = self.__prepare_file_location(file_name=file_name,
                                                     file_extension=".png",
                                                     folder="results")
        def save_func():
            success = cv2.imwrite(file_location, image)
            if not success:
                raise ValueError("Failed to write image to disk")
        return self.__save_and_verify(file_location, save_func)

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
                if not ret: # no more frames
                    break
                frames.append(frame)
            cap.release()
            if not frames:
                raise ValueError(f"No frames could be read from {location}")
            return np.array(frames)
        except Exception as e:
            raise Exception(f"Failed to read {location} from storage: {e}")

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



