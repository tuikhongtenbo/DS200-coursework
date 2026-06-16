"""
Chạy Processing Server. Đóng vai trò là Kafka Consumer nhận frame ảnh từ Camera Server,
sau đó đẩy qua PersonDetector để lấy bounding box, rồi trở thành Kafka Producer gửi kết quả
lên topic Kafka để Storage Server lưu lại.
"""

import cv2
import base64
import numpy as np
from src.common.config import KAFKA_BROKER, TOPIC_FRAME, TOPIC_RESULT
from src.common.kafka_utils import get_kafka_consumer, get_kafka_producer
from src.processor.model import PersonDetector
from src.processor.counter import PeopleCounter

def start_processing_server():
    consumer = get_kafka_consumer(TOPIC_FRAME, KAFKA_BROKER, group_id='processor_group')
    producer = get_kafka_producer(KAFKA_BROKER)
    detector = PersonDetector()
    counter = PeopleCounter()
    print("Processing Server is running and listening to Kafka...")

    for msg in consumer:
        message_data = msg.value
        frame_id = message_data['frame_id']
        timestamp = message_data['timestamp']
        frame_base64 = message_data['data']

        frame_bytes = base64.b64decode(frame_base64)
        np_arr = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        if frame is not None:
            bboxes = detector.detect(frame)
            count = counter.count(bboxes)
            
            # Vẽ bounding boxes lên frame
            for box in bboxes:
                x1, y1, x2, y2 = int(box['x1']), int(box['y1']), int(box['x2']), int(box['y2'])
                conf = box['confidence']
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                cv2.putText(frame, f"Person {conf:.2f}", (x1, max(y1 - 10, 0)), 
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
            
            # Ghi số lượng người
            cv2.putText(frame, f"Count: {count}", (20, 40), 
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 3)

            # Encode ảnh đã vẽ
            _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            annotated_frame_base64 = base64.b64encode(buffer).decode('utf-8')

            result_message = {
                'frame_id': frame_id,
                'timestamp': timestamp,
                'people_count': count,
                'bounding_boxes': bboxes,
                'annotated_frame': annotated_frame_base64
            }
            producer.send(TOPIC_RESULT, value=result_message)
            print(f"Processed Frame {frame_id} - Found {count} people.")

if __name__ == '__main__':
    start_processing_server()

