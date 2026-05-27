"""
Task 2:
Thống kê tổng số đơn hàng, số lượng khách hàng và người bán.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import countDistinct

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_2_output.txt"

LINE_WIDTH = 60


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


def format_row(label: str, value: int) -> str:
    formatted_value = f"{value:,}"
    return f"  {label:<50} {formatted_value:>6}\n"


def process_task(spark: SparkSession, output_handle) -> None:
    """
    Thống kê tổng số đơn hàng, khách hàng, người bán và ghi ra output/màn hình.
    """
    orders_df = load_csv(spark, os.path.join(DATASET_DIR, "Orders.csv"))
    customers_df = load_csv(spark, os.path.join(DATASET_DIR, "Customer_List.csv"))
    items_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Items.csv"))

    total_orders = orders_df.select(countDistinct("Order_ID")).collect()[0][0]
    total_real_customers = customers_df.select(countDistinct("Subscriber_ID")).collect()[0][0]
    total_sellers = items_df.select(countDistinct("Seller_ID")).collect()[0][0]

    separator = "=" * LINE_WIDTH + "\n"
    title = "Thống kê tổng quan".center(LINE_WIDTH) + "\n"

    full_output = (
        separator
        + title
        + separator
        + format_row("Tổng số đơn hàng", total_orders)
        + format_row("Số lượng khách hàng thật sự", total_real_customers)
        + format_row("Số lượng người bán", total_sellers)
        + separator
    )

    print(full_output, end="")
    output_handle.write(full_output)


# =========================
# MAIN
# =========================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = SparkSession.builder.appName("Task2").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
