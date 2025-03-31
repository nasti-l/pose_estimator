from src.postprocessor import YoloProcessor


def test_yolo_processor_model_loads():
    processor = YoloProcessor()
    assert processor is not None
