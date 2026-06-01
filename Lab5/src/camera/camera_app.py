"""
 Camera Server: đọc dữ liệu từ webcam hoặc file video,
 -> nén hình ảnh -> mã hóa base64 -> gửi lên topic Kafka để Processing Server xử lý.
"""

import cv2
import base64
import time
from src.common.config import KAFKA_BROKER, TOPIC_FRAME
from src.common.kafka_utils import get_kafka_producer

def start_camera_server():
    producer = get_kafka_producer(KAFKA_BROKER)
    cap = cv2.VideoCapture(0)
    frame_id = 0

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        frame_base64 = base64.b64encode(buffer).decode('utf-8')

        message = {
            'frame_id': frame_id,
            'timestamp': time.time(),
            'data': frame_base64
        }

        producer.send(TOPIC_FRAME, value=message)
        frame_id += 1
        time.sleep(1/30)

    cap.release()

if __name__ == '__main__':
    start_camera_server()
