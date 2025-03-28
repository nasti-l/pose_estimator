from session_manager import SessionManager
from datetime import datetime

def main_loop():
    video_types = {
        1: {"name": "Calibration", "duration": 10},
        2: {"name": "A-pose", "duration": 30}
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

        print(f"\nPreparing to record {selected_video['name']} video...")
        print(f"Video duration: {selected_video['duration']} seconds")
        input("Press Enter when you're ready to start recording...")
        session_manager.save_recording(selected_video['duration'], selected_video['name'], participant_name, session)
        print("Recording in progress...")
        input(f"Press Enter after {selected_video['duration']} seconds of recording.")

        while True:
            rerecord = input(f"\nAre you satisfied with the {selected_video['name']} video? (yes/no): ").lower().strip()
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