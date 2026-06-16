"""
 Camera Server: đọc dữ liệu từ webcam, file video hoặc file ảnh tĩnh,
 -> nén hình ảnh -> mã hóa base64 -> gửi lên topic Kafka để Processing Server xử lý.
"""

import cv2
import base64
import time
import argparse
import os
from src.common.config import KAFKA_BROKER, TOPIC_FRAME
from src.common.kafka_utils import get_kafka_producer

def send_frame(producer, frame, frame_id):
    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
    frame_base64 = base64.b64encode(buffer).decode('utf-8')

    message = {
        'frame_id': frame_id,
        'timestamp': time.time(),
        'data': frame_base64
    }

    producer.send(TOPIC_FRAME, value=message)
    print(f"Sent Frame {frame_id} to Kafka.")

def start_camera_server(source):
    producer = get_kafka_producer(KAFKA_BROKER)
    frame_id = 0

    # Kiểm tra xem source có phải là file ảnh không
    image_exts = ['.jpg', '.jpeg', '.png', '.bmp']
    is_image = any(str(source).lower().endswith(ext) for ext in image_exts)

    if is_image:
        if not os.path.exists(source):
            print(f"Lỗi: Không tìm thấy file ảnh {source}")
            return
        frame = cv2.imread(source)
        if frame is not None:
            send_frame(producer, frame, frame_id)
            producer.flush()
        else:
            print("Lỗi không thể đọc file ảnh.")
    else:
        if str(source).isdigit():
            source = int(source)

        cap = cv2.VideoCapture(source)
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            
            send_frame(producer, frame, frame_id)
            frame_id += 1
            time.sleep(1/30)

        cap.release()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Camera Server Producer")
    parser.add_argument('--source', default='0', help='Nguồn dữ liệu: 0 cho webcam, hoặc đường dẫn tới file video/ảnh.')
    args = parser.parse_args()

    start_camera_server(args.source)

