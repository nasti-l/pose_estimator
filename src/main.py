from src.session_manager import SessionManager
from datetime import datetime
from src.storage_manager import PreRecordingData
from dotenv import load_dotenv

class Session:
    def __init__(self):
        self.__participant_name = None
        self.__video_types = {
        1: {"activity": "Calibration", "sec": 10},
        2: {"activity": "A-pose", "sec": 30}}
        self.__choices = list(self.__video_types.keys()) + [(len(self.__video_types)+1)] + [(len(self.__video_types)+2)]
        self.__session_start = datetime.now().isoformat()
        self.__session_manager = SessionManager(session_start = self.__session_start)

    def run(self):
        try:
            print("Welcome to the Video Recording Application!")
            self.__participant_name = input("Please enter your name: ").strip()
            while True:
                self.__print_options()
                while True:
                    try:
                        action = int(input("Enter the number of an action: "))
                        if action in self.__choices:
                            break
                        else:
                            print("Invalid choice. Please enter 1 or 2.")
                    except ValueError:
                        print("Please enter a valid number.")
                if action == self.__choices[-1]:
                    self.__exit_program()
                elif action == self.__choices[-2]:
                    raws, headers = self.__session_manager.get_all_recordings()
                    if not raws or not headers:
                        print("No recordings found in the database.")
                        continue
                    else:
                        self.__print_recordings_table(raws, headers)
                        self.__prompt_to_remove_recording()
                        continue
                else:
                    selected_video = self.__video_types[action]
                    self.__record_video(selected_video)
                    continue
        except Exception:
            self.__exit_program()


    def __record_video(self, selected_video: dict):
        print(f"\nPreparing to record {selected_video['activity']} video...")
        print(f"Video duration: {selected_video['sec']} seconds")
        input("Press Enter when you're ready to start recording...")
        print("Recording in progress...")
        video_to_record = PreRecordingData(duration_in_sec=selected_video['sec'],
                                           activity=selected_video['activity'],
                                           session_start=self.__session_start,
                                           participant=self.__participant_name)

        successfully_recorded = self.__session_manager.record_video(video_data=video_to_record)
        if successfully_recorded:
            to_save = input("Recording completed, would you like to save it? (yes/no): ").lower().strip()
            if to_save == "yes":
                successfully_saved = self.__session_manager.save_last_recording()
                if successfully_saved:
                    print(
                        f"\nRecording successfully saved. Thank you for recording the {selected_video['activity']} video!")
                else:
                    print("Failed to save the recording, please try again.")
            else:
                print("Recording wasn't saved.")
        else:
            print("Recording failed, please try again.")
        
    def __print_options(self):
        print(f"\nHello, {self.__participant_name}! Please choose an action: ")
        for key, value in self.__video_types.items():
            print(f"{key}. Record a {value['activity']} Video ({value['sec']} seconds)")
        print(f"{self.__choices[-2]}. Display all recordings / Choose a recording to remove")
        print(f"{self.__choices[-1]}. Exit")
    
    def __print_recordings_table(self, rows: list[list[str]], headers: list[str]):
        if not rows:
            print("No recordings to display.")
            return
        col_widths = [len(h) for h in headers]
        for row in rows:
            col_widths = [max(len(str(cell)), w) for cell, w in zip(row, col_widths)]
    
        def format_row(r):
            return " | ".join(str(cell).ljust(w) for cell, w in zip(r, col_widths))
    
        separator = "-+-".join("-" * w for w in col_widths)
        print(format_row(headers))
        print(separator)
        for row in rows:
            print(format_row(row))
    
    def __prompt_to_remove_recording(self):
        while True:
            user_input = input("Enter recording ID to remove, or press Enter to return to main menu: ").strip()
            if user_input == "":
                print("Returning to main menu...")
                return
            if not user_input.isdigit():
                print("Invalid ID. Please enter a numeric recording ID: ")
                continue
            try:
                video_path = self.__session_manager.remove_recording(user_input)
                if video_path:
                    print(f"Recording {user_input} removed successfully. File was located at: {video_path}")
                else:
                    print("Failed to remove recording, please try again.")
                break
            except Exception:
                raise
    
    def __exit_program(self):
        print("Exiting. Live long and prosper.")
        exit(0)


if __name__ == "__main__":
    load_dotenv()
    session = Session()
    session.run()