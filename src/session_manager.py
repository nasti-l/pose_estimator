from recorder import VideoRecorder
from data_manager import LocalStorageManager, DBManager

class SessionManager:
    def __init__(self):
        self.__recorder = VideoRecorder()
        self.__db = DBManager(config="./config.py")
        self.__storage = LocalStorageManager(location='./videos/')

    def save_recording(self, duration_in_sec: int, activity: str, session: str, participant: str) -> bool:
        #TODO: save more relevant info about reason for corruption
        try:
            frames, if_corrupted = self.__recorder.record_video(duration_in_sec)
            file_location = self.__storage.write_video_to_storage(frames)
            #TODO fill metadata struct and send with location
            self.__db.save_metadata(file_location)
        except Exception as e:
            print(f"Failed to save recording: {e}")
            #TODO: separate to cases error handling
            return False
        return True





