from recorder import VideoRecorder
from data_manager import LocalStorageManager, DBManager, PreRecordingData, PostRecordingData

# Responsibility: manage session

class SessionManager:
    def __init__(self):
        self.__recorder = VideoRecorder()
        self.__db = DBManager(config="./config.py")
        self.__storage = LocalStorageManager(location='./videos/')

    def save_recording(self, video_data: PreRecordingData) -> bool:
        #TODO: save more relevant info about reason for corruption, expand validation
        try:
            frames, fps, amount_of_frames, if_corrupted = self.__recorder.record_video(video_data.duration_in_sec)
            file_location = self.__storage.write_video_to_storage(frames=frames)
            video_metadata = PostRecordingData(**video_data.__dict__,
                                               fps=fps,
                                               amount_of_frames=amount_of_frames,
                                               location=file_location,
                                               if_corrupted=if_corrupted)
            self.__db.save_metadata(metadata=video_metadata)
        except Exception as e:
            print(f"Failed to save recording: {e}")
            #TODO: separate to cases error handling, check all false returns
            return False
        return True





