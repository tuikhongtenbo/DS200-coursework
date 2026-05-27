"""
Task 4:
Phân tích số lượng đơn hàng nhóm theo năm, tháng đặt hàng (năm tăng dần, tháng giảm dần).
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month, count, col

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_4_output.txt"


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


def process_task(spark: SparkSession, output_handle) -> None:
    """
    Phân tích đơn hàng theo năm/tháng và ghi kết quả ra output/màn hình.
    """
    orders_df = load_csv(spark, os.path.join(DATASET_DIR, "Orders.csv"))

    result_df = (
        orders_df.withColumn("Year", year("Order_Purchase_Timestamp"))
        .withColumn("Month", month("Order_Purchase_Timestamp"))
        .groupBy("Year", "Month")
        .agg(count("Order_ID").alias("Total_Orders"))
        .orderBy(col("Year").asc(), col("Month").desc())
    )

    output_str = result_df._jdf.showString(100, 20, False)

    full_output = (
        "=" * 50 + "\n" +
        "SỐ LƯỢNG ĐƠN HÀNG THEO NĂM VÀ THÁNG\n" +
        "-" * 50 + "\n" +
        output_str +
        "=" * 50 + "\n"
    )

    print(full_output, end="")
    output_handle.write(full_output)


# =========================
# MAIN
# =========================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = SparkSession.builder.appName("Task4").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
