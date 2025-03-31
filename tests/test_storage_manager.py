import numpy as np
import pandas as pd
import os
from src.storage_manager import LocalStorageManager


def test_write_and_read_video(tmp_path):
    storage = LocalStorageManager(location=str(tmp_path))

    # Create dummy video frames
    dummy_frames = np.zeros((5, 64, 64, 3), dtype=np.uint8)
    fps = 15
    filename = "test_video"

    # Save video
    saved_path = storage.write_video_to_storage(frames=dummy_frames, fps=fps, file_name=filename)
    assert os.path.exists(saved_path)

    # Read video
    read_frames = storage.read_video_from_storage(saved_path)
    assert isinstance(read_frames, np.ndarray)
    assert read_frames.shape[0] == 5
    os.remove(saved_path)


def test_write_and_read_dataframe(tmp_path):
    storage = LocalStorageManager(location=str(tmp_path))

    df = pd.DataFrame({
        "x": [1, 2, 3],
        "y": ["a", "b", "c"]
    })

    saved_path = storage.write_dataframe_to_storage(data=df, file_name="test_df")
    assert os.path.exists(saved_path)

    loaded_df = storage.read_dataframe_from_storage(saved_path)
    assert isinstance(loaded_df, pd.DataFrame)
    assert loaded_df.equals(df)
    os.remove(saved_path)
