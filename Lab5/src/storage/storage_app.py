"""
/*
 * File này chạy Storage Server. Lắng nghe topic kết quả từ Kafka và lưu trữ dữ liệu
 * vào Database PostgreSQL. Đồng thời vẫn có thể ghi log ra file JSON Lines nếu cần.
 */
"""

import os
import json
import psycopg2
from src.common.config import KAFKA_BROKER, TOPIC_RESULT
from src.common.kafka_utils import get_kafka_consumer

STORAGE_DIR = 'storage_data'
OUTPUT_FILE = os.path.join(STORAGE_DIR, 'results.jsonl')

def init_db():
    conn = psycopg2.connect(
        host='localhost',
        database='people_counter',
        user='admin',
        password='password',
        port='5432'
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

    # Khởi tạo kết nối DB
    print("Connecting to PostgreSQL...")
    conn = init_db()
    cursor = conn.cursor()
    print("Connected successfully. Listening to Kafka...")

    consumer = get_kafka_consumer(TOPIC_RESULT, KAFKA_BROKER, group_id='storage_group')

    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        for msg in consumer:
            result_data = msg.value
            
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

            print(f"Saved Frame {result_data['frame_id']} | People Count: {result_data['people_count']} to Database.")

if __name__ == '__main__':
    start_storage_server()
