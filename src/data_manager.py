from abc import ABC, abstractmethod
import numpy as np
import pandas


class StorageManager(ABC):
    def __init__(self, location: str):
        self.location = location

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



class DBManager():
    def __init__(self, config: str):
        self.__init_db(config)

    def __init_db(self, config: str):
        #TODO create a db if not exists
        pass

    #TODO: create a struct for metadata
    def save_metadata(self, metadata) -> bool:
        pass

    def get_id_to_location_map(self, query: str) -> {str:str}:
        pass

    def add_pose_estimation(self, pose_estimation_location: str, id: str):
        pass


#TODO: separate storage relevant for offline and online



