"""
Task 1:
Đọc các file CSV bằng PySpark và tự động suy luận kiểu dữ liệu.
"""

import os
from pyspark.sql import SparkSession, DataFrame

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"

CSV_FILES = [
    "Customer_List.csv",
    "Order_Items.csv",
    "Order_Reviews.csv",
    "Orders.csv",
    "Products.csv"
]

OUTPUT_FILE = "task_1_output.txt"


# =========================
# UTIL FUNCTIONS
# =========================
def load_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Đọc file CSV với header và tự suy luận kiểu dữ liệu.
    """
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("inferSchema", True)
        .csv(file_path)
    )


def process_file(spark: SparkSession, filename: str, output_handle) -> None:
    """
    Đọc file, đếm số dòng, trích xuất schema và in ra cả màn hình lẫn file.
    """
    file_path = os.path.join(DATASET_DIR, filename)
    df = load_csv(spark, file_path)
    
    row_count = df.count()
    table_name = filename.replace(".csv", "")
    
    header = "=" * 50 + "\n"
    header += f"  {table_name} ({row_count} rows)\n"
    header += "=" * 50 + "\n"
    
    schema_lines = []
    for field in df.schema.fields:
        name = field.name
        dtype = str(field.dataType)
        nullable = "(nullable)" if field.nullable else "(not nullable)"
        schema_lines.append(f"  {name:<40} {dtype:<20} {nullable}")
        
    schema_str = "\n".join(schema_lines) + "\n\n"
    full_output = header + schema_str
    
    print(full_output, end="")
    output_handle.write(full_output)


# =========================
# MAIN
# =========================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = SparkSession.builder.appName("Task1").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        for filename in CSV_FILES:
            process_file(spark, filename, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
