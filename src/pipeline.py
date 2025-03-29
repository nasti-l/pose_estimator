import inspect
import numpy as np
import pandas as pd
from dagster import op, job, Out, ResourceDefinition, DynamicOut, DynamicOutput
from ultralytics.engine.results import Results
from typing import Dict, List, Any
from postprocessor import YoloProcessor
from db_manager import PostgresDBManager
from storage_manager import LocalStorageManager

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
    out=Out(Dict[str, str])
)
def get_video_locations(context, range_start: str, range_end: str) -> Dict[str, str]:
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
    return context.resources.pose_extractor.extract(frames)

@op(
    required_resource_keys={"pose_extractor"},
    out=Out(pd.DataFrame)
)
def yolo_results_to_dataframe(context, yolo_results: List[Results]) -> pd.DataFrame:
    return context.resources.pose_extractor.frames_results_to_video_df(yolo_results)


@op(out=Out(Any))
def process_yolo_results_alternative(context, yolo_results: List[Results]) -> Any:
    yolo_results = "Something"
    #TODO: visualise results
    return yolo_results

@op(out=DynamicOut())
def split_video_locations(videos_to_process: Dict[str, str]):
    for video_id, location in videos_to_process.items():
        yield DynamicOutput(
            value={"video_id": video_id, "location": location},
            mapping_key=video_id
        )

@op(
    required_resource_keys={"storage"},
    out=Out(str)
)
def save_dataframe_to_storage(context, df: pd.DataFrame) -> str:
    return context.resources.local_storage.write_dataframe_to_storage(df)

@op(
    required_resource_keys={"storage"},
    out=Out(str)
)
def save_image_to_storage(context, image: np.array) -> str:
    return context.resources.local_storage.write_image_to_storage(image)

@op(required_resource_keys={"db"})
def log_result_for_video_to_db(context, result_location: str, process_description: str, video_id: str):
    return context.resources.db.update_results_for_video(
        processor_name=process_description,
        results_location=result_location,
        video_id=video_id)

@job(
    resource_defs={
    "postgres_db": db,
    "local_storage": storage,
    "pose_extractor": pose_extractor,
})
def video_processing_pipeline():
    video_map = get_video_locations()
    video_tasks = split_video_locations(video_map)

    def process_video_yolo(video_task):
        video_id = video_task["video_id"]
        location = video_task["location"]

        frames = extract_frames(location)
        yolo_results = get_pose_estimations(frames)

        df = yolo_results_to_dataframe(yolo_results)
        df_location = save_dataframe_to_storage(df)
        log_result_for_video_to_db( df_location, inspect.currentframe().f_code.co_name, video_id)

        return {"df_location": df_location}

    def process_video_yolo_visualise(video_task):
        video_id = video_task["video_id"]
        location = video_task["location"]

        frames = extract_frames(location)
        yolo_results = get_pose_estimations(frames)

        images = process_yolo_results_alternative(yolo_results)
        images_location = save_image_to_storage(images)  #TODO save many
        log_result_for_video_to_db( images_location, inspect.currentframe().f_code.co_name, video_id)

        return {"images_location": images_location}

    video_tasks.map(process_video_yolo)


# Example execution
if __name__ == "__main__":
    result = video_processing_pipeline.execute_in_process(
        run_config={
            "ops": {
                "get_video_locations": {
                    "inputs": {
                        "start_date": "2025-03-01",
                        "end_date": "2025-03-29"
                    }
                }
            }
        }
    )