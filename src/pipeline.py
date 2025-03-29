import numpy as np
import pandas as pd
from dagster import op, job, In, Out, ResourceDefinition, DynamicOut, DynamicOutput, Definitions, graph
from ultralytics.engine.results import Results
from typing import Dict, List
from src.postprocessor import YoloProcessor
from src.db_manager import PostgresDBManager
from src.storage_manager import LocalStorageManager
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

db = ResourceDefinition(
    lambda _: PostgresDBManager()
)

storage = ResourceDefinition(
    lambda _: LocalStorageManager()
)

pose_extractor = ResourceDefinition(
    lambda _: YoloProcessor())

@op(
    required_resource_keys={"db"},
    config_schema={"range_start": str, "range_end": str},
    out=Out(Dict[str, str])
)
def get_video_locations(context) -> Dict[str, str]:
    range_start = context.op_config["range_start"]
    range_end = context.op_config["range_end"]
    return context.resources.db.get_all_recordings_in_time_range(start_time=range_start,
                                        end_time=range_end)

@op(
    required_resource_keys={"storage"},
    out=Out(np.ndarray)
)
def extract_frames(context, video_location: str) -> np.ndarray:
    return context.resources.storage.read_video_from_storage(video_location)

@op(
    required_resource_keys={"pose_extractor"},
    out=Out(List[Results])
)
def get_pose_estimations(context, frames: np.ndarray) -> List[Results]:
    return context.resources.pose_extractor.process(frames)

@op(
    required_resource_keys={"pose_extractor"},
    out=Out(pd.DataFrame)
)
def yolo_results_to_dataframe(context, yolo_results: List[Results]) -> pd.DataFrame:
    return context.resources.pose_extractor.frames_results_to_video_df(yolo_results)

@op(out=DynamicOut())
def split_video_locations(videos_to_process: Dict[str, str]):
    for video_id, location in videos_to_process.items():
        yield DynamicOutput(
            value={"video_id": video_id, "location": location},
            mapping_key=video_id
        )

@op(out={"video_id": Out(str), "location": Out(str)})
def unpack_video_data(video_data: dict):
    return video_data["video_id"], video_data["location"]

@op(
    required_resource_keys={"storage"},
    out=Out(str)
)
def save_dataframe_to_storage(context, df: pd.DataFrame, video_id: str, process_description: str) -> str:
    timestamp = datetime.now().isoformat(timespec="seconds").replace(":", "-")
    description_clean = process_description.replace(" ", "_")
    filename = f"{timestamp}_{video_id}_{description_clean}"
    return context.resources.storage.write_dataframe_to_storage(data=df, file_name=filename)

@op(required_resource_keys={"db"})
def log_result_for_video_to_db(context, result_location: str, process_description: str, video_id: str):
    return context.resources.db.update_results_for_video(
        processor_name=process_description,
        results_location=result_location,
        video_id=video_id)

@op(out=Out(str))
def get_yolo_process_description() -> str:
    return "YOLO Pose Extraction"

@graph(ins={"video_data": In(dict)})
def process_single_video_graph(video_data):
    video_id, location = unpack_video_data(video_data)
    frames = extract_frames(video_location=location)
    yolo_results = get_pose_estimations(frames)
    df = yolo_results_to_dataframe(yolo_results)
    desc = get_yolo_process_description()
    result_path = save_dataframe_to_storage(df=df,
                                            process_description=desc,
                                            video_id=video_id)
    log_result_for_video_to_db(
        result_location=result_path,
        process_description=desc,
        video_id=video_id,
    )

@job(
    resource_defs={
        "db": db,
        "storage": storage,
        "pose_extractor": pose_extractor,
    }
)
def video_processing_job():
    video_locations = get_video_locations()
    split = split_video_locations(video_locations)

    split.map(process_single_video_graph)

defs = Definitions(
    jobs=[video_processing_job],
    resources={"db": db, "storage": storage, "pose_extractor": pose_extractor}
)