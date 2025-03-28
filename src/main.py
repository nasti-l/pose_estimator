from session_manager import SessionManager
from datetime import datetime
from src.data_manager import PreRecordingData

def main_loop():
    video_types = {
        1: {"activity": "Calibration", "sec": 1},
        2: {"activity": "A-pose", "sec": 30}
    }
    choices = list(video_types.keys()) + [(len(video_types)+1)]
    session_start = datetime.now().isoformat()
    session_manager = SessionManager()

    print("Welcome to the Video Recording Application!")
    participant_name = input("Please enter your name: ").strip()

    while True:
        print(f"\nHello, {participant_name}! Please choose an action:")
        for key, value in video_types.items():
            print(f"{key}. Record a {value['activity']} Video ({value['sec']} seconds)")
        print(f"{choices[-1]}. Exit")

        while True:
            try:
                action = int(input("Enter the number of an action: "))
                if action in choices:
                    break
                else:
                    print("Invalid choice. Please enter 1 or 2.")
            except ValueError:
                print("Please enter a valid number.")
        if action == choices[-1]:
            print("Exiting. Live long and prosper.")
            exit(0)
        else:
            selected_video = video_types[action]
            print(f"\nPreparing to record {selected_video['activity']} video...")
            print(f"Video duration: {selected_video['sec']} seconds")
            input("Press Enter when you're ready to start recording...")
            print("Recording in progress...")
            video_to_record = PreRecordingData(duration_in_sec=selected_video['sec'],
                                               activity=selected_video['activity'],
                                               session_start=session_start,
                                               participant=participant_name)

            successfully_recorded = session_manager.record_video(video_data=video_to_record)
            if successfully_recorded:
                to_save = input("Recording completed, would you like to save it? (yes/no):").lower().strip()
                if to_save == "yes":
                    successfully_saved = session_manager.save_last_recording()
                    if successfully_saved:
                        print(f"\nRecording successfully saved. Thank you for recording the {selected_video['activity']} video!")
                    else:
                        print("Failed to save the recording, please try again.")
                else:
                    print("Recording wasn't saved.")
            else:
                print("Recording failed, please try again.")
            continue


if __name__ == "__main__":
    main_loop()