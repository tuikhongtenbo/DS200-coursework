"""
Task 3:
Phân tích số lượng đơn hàng theo quốc gia, sắp xếp theo thứ tự giảm dần.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_3_output.txt"


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
    Phân tích đơn hàng theo quốc gia và ghi kết quả ra output/màn hình.
    """
    orders_df = load_csv(spark, os.path.join(DATASET_DIR, "Orders.csv"))
    customers_df = load_csv(spark, os.path.join(DATASET_DIR, "Customer_List.csv"))

    joined_df = orders_df.join(customers_df, "Customer_Trx_ID", "inner")

    result_df = (
        joined_df.groupBy("Customer_Country")
        .agg(count("Order_ID").alias("Total_Orders"))
        .orderBy(col("Total_Orders").desc())
    )

    output_str = result_df._jdf.showString(50, 50, False)

    full_output = (
        "=" * 50 + "\n" +
        "SỐ LƯỢNG ĐƠN HÀNG THEO QUỐC GIA\n" +
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
    spark = SparkSession.builder.appName("Task3").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
