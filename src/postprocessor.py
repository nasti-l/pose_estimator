import pandas
import numpy as np
import pandas as pd

from typing import List
from ultralytics import YOLO
from ultralytics.engine.results import Results


class YoloProcessor:
    def __init__(self):
        self.__name = "yolo11_pose_estimation"
        self.__model = YOLO("yolo11n-pose.pt")

    def process(self, data: np.ndarray) -> List[Results]:
        return self.__model([_ for _ in data])

    def frames_results_to_video_df(self, results: List[Results]) -> pandas.DataFrame:
        frame_dfs = []
        for frame_num, result in enumerate(results):
            df = result.to_df()
            df['frame'] = frame_num
            frame_dfs.append(df)
        return pd.concat(frame_dfs, ignore_index=True)