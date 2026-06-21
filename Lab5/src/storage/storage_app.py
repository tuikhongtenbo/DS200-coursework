"""
Chạy Storage Server. Lắng nghe topic kết quả từ Kafka và lưu trữ dữ liệu
vào Database PostgreSQL. Đồng thời vẫn có thể ghi log ra file JSON Lines nếu cần.
"""

import os
import json
import base64
import numpy as np
import cv2
import psycopg2
from src.common.config import KAFKA_BROKER, TOPIC_RESULT
from src.common.kafka_utils import get_kafka_consumer

STORAGE_DIR = 'storage_data'
OUTPUT_FILE = os.path.join(STORAGE_DIR, 'results.jsonl')
IMAGE_OUTPUT_DIR = os.path.join(STORAGE_DIR, 'output_images')

def init_db():
    conn = psycopg2.connect(
        host='localhost',
        database='people_counter',
        user='admin',
        password='password',
        port='5433'
    )
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS detection_results (
            id SERIAL PRIMARY KEY,
            frame_id INT,
            timestamp FLOAT,
            people_count INT,
            bounding_boxes JSONB
        )
    ''')
    conn.commit()
    return conn

def start_storage_server():
    if not os.path.exists(STORAGE_DIR):
        os.makedirs(STORAGE_DIR)
        
    if not os.path.exists(IMAGE_OUTPUT_DIR):
        os.makedirs(IMAGE_OUTPUT_DIR)

    # Khởi tạo kết nối DB
    print("Connecting to PostgreSQL...")
    conn = init_db()
    cursor = conn.cursor()
    print("Connected successfully. Listening to Kafka...")

    consumer = get_kafka_consumer(TOPIC_RESULT, KAFKA_BROKER, group_id='storage_group')

    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        for msg in consumer:
            result_data = msg.value
            
            # Ghi ảnh ra đĩa nếu có
            if 'annotated_frame' in result_data:
                frame_bytes = base64.b64decode(result_data['annotated_frame'])
                np_arr = np.frombuffer(frame_bytes, np.uint8)
                annotated_frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                if annotated_frame is not None:
                    img_path = os.path.join(IMAGE_OUTPUT_DIR, f"frame_{result_data['frame_id']}.jpg")
                    cv2.imwrite(img_path, annotated_frame)
                
                # Xóa trường base64 khỏi dict trước khi lưu log để tránh log file quá lớn
                del result_data['annotated_frame']
            
            # 1. Ghi ra file JSONL
            f.write(json.dumps(result_data) + '\n')
            f.flush()

            # 2. Lưu vào Database PostgreSQL
            cursor.execute(
                """
                INSERT INTO detection_results (frame_id, timestamp, people_count, bounding_boxes)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    result_data['frame_id'], 
                    result_data['timestamp'], 
                    result_data['people_count'], 
                    json.dumps(result_data['bounding_boxes'])
                )
            )
            conn.commit()

            print(f"Saved Frame {result_data['frame_id']} | People Count: {result_data['people_count']} to Database & Image.")

if __name__ == '__main__':
    start_storage_server()
