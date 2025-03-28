from session_manager import SessionManager
from datetime import datetime

from src.data_manager import PreRecordingData


def main_loop():
    video_types = {
        1: {"activity": "Calibration", "sec": 10},
        2: {"activity": "A-pose", "sec": 30}
    }
    session = datetime.now().isoformat()
    session_manager = SessionManager()

    print("Welcome to the Video Recording Application!")
    participant_name = input("Please enter your name: ").strip()

    while True:
        print(f"\nHello, {participant_name}! Please choose a video type:")
        print("1. Calibration Video (10 seconds)")
        print("2. A-pose Video (30 seconds)")

        while True:
            try:
                video_choice = int(input("Enter the number of your chosen video type (1 or 2): "))
                if video_choice in [1, 2]:
                    break
                else:
                    print("Invalid choice. Please enter 1 or 2.")
            except ValueError:
                print("Please enter a valid number.")

        selected_video = video_types[video_choice]

        print(f"\nPreparing to record {selected_video['activity']} video...")
        print(f"Video duration: {selected_video['sec']} seconds")
        input("Press Enter when you're ready to start recording...")
        video_to_record = PreRecordingData(duration_in_sec=selected_video['sec'],
                                           activity=selected_video['activity'],
                                           session_start=session,
                                           participant=participant_name)
        session_manager.save_recording(video_data=video_to_record)
        print("Recording in progress...")
        input(f"Press Enter after {selected_video['sec']} seconds of recording.")

        while True:
            rerecord = input(f"\nAre you satisfied with the {selected_video['activity']} video? (yes/no): ").lower().strip()
            if rerecord in ['yes', 'no']:
                break
            else:
                print("Please enter 'yes' or 'no'.")

        if rerecord == 'no':
            continue
        else:
            print(f"\nThank you for recording the {selected_video['name']} video!")
            break


if __name__ == "__main__":
    main_loop()