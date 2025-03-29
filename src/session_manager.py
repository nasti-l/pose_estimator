import logging
from src.recorder import WebCamVideoRecorder
from src.storage_manager import LocalStorageManager, PreRecordingData, PostRecordingData, RecordingMetaData
from src.db_manager import PostgresDBManager

class SessionManager:
    def __init__(self):
        self.__recorder = WebCamVideoRecorder()
        self.__db = PostgresDBManager()
        self.__storage = LocalStorageManager(location='./output/')
        self.__last_recording_frames = None
        self.__last_recording_data = None

    def record_video(self, video_data: PreRecordingData) -> bool:
        self.__last_recording_frames = None
        self.__last_recording_data = None
        try:
            self.__last_recording_frames, fps, amount_of_frames, start, end, if_corrupted = self.__recorder.record_video(video_data.duration_in_sec)
            self.__last_recording_data = PostRecordingData(**video_data.__dict__,
                                                           fps=fps,
                                                           amount_of_frames=amount_of_frames,
                                                           start_time=start,
                                                           end_time=end,
                                                           if_corrupted=if_corrupted)
        except Exception as e:
            logging.error(f"Failed to record: {e}")
            return False
        return True

    def save_last_recording(self) -> bool:
        if self.__last_recording_frames is None:
            logging.error("No recordings available")
            return False
        elif self.__last_recording_data is None:
            logging.error("No recording metadata available")
            return False
        try:
            file_name = f"recording_{self.__last_recording_data.participant}_{self.__last_recording_data.activity}_{self.__last_recording_data.start_time}"
            location = self.__storage.write_video_to_storage(frames=self.__last_recording_frames,
                                                             fps=self.__last_recording_data.fps,
                                                             file_name=file_name)
        except Exception as e:
            logging.error(f"Failed to write recording to storage: {e}")
            return False
        del self.__last_recording_frames
        self.__last_recording_frames = None
        logging.info(f"Saved recording to: {location}")
        recording_metadata = RecordingMetaData(**self.__last_recording_data.__dict__,
                                               file_location=location)
        try:
            self.__db.save_metadata_for_video(metadata=recording_metadata)
        except Exception as e:
            logging.error(f"Failed writing to DB: {e}")
            return False
        logging.info(f"Saved recording metadata to DB: {recording_metadata}")
        return True


