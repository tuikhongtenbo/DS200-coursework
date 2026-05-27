"""
Task 9:
Nhóm khách hàng dựa trên số lượng đơn hàng, giá trị trung bình của đơn hàng và tần suất mua sắm.
  One-time   : 1 đơn hàng
  Occasional : 2-3 đơn hàng
  Loyal      : >= 4 đơn hàng
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, countDistinct, sum as _sum, avg, datediff,
    max as _max, min as _min, when, round as _round
)

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_9_output.txt"

LINE_WIDTH = 70


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


def fmt_group_row(group: str, customers: int, avg_orders: float, avg_value: float, freq: float) -> str:
    return (
        f"  {group:<16} {customers:>9,}  {avg_orders:>7.2f}  {avg_value:>11.2f}  {freq:>15.2f}\n"
    )


def process_task(spark: SparkSession, output_handle) -> None:
    """
    Phân nhóm khách hàng theo tần suất mua và tính các chỉ số hành vi ra output/màn hình.
    """
    orders_df = load_csv(spark, os.path.join(DATASET_DIR, "Orders.csv"))
    items_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Items.csv"))
    customers_df = load_csv(spark, os.path.join(DATASET_DIR, "Customer_List.csv"))

    order_value_df = (
        items_df.withColumn("Item_Value", col("Price") + col("Freight_Value"))
        .groupBy("Order_ID")
        .agg(_sum("Item_Value").alias("Order_Total_Value"))
    )

    joined_df = (
        orders_df.join(order_value_df, "Order_ID", "left")
        .join(customers_df, "Customer_Trx_ID", "left")
    )

    customer_stats = (
        joined_df.groupBy("Subscriber_ID")
        .agg(
            countDistinct("Order_ID").alias("Total_Orders"),
            avg("Order_Total_Value").alias("Avg_Order_Value"),
            datediff(
                _max("Order_Purchase_Timestamp"),
                _min("Order_Purchase_Timestamp")
            ).alias("Lifespan_Days")
        )
        .withColumn(
            "Frequency_Days",
            when(col("Total_Orders") <= 1, 0.0)
            .otherwise(col("Lifespan_Days") / (col("Total_Orders") - 1))
        )
        .withColumn(
            "Segment",
            when(col("Total_Orders") == 1, "One-time")
            .when(col("Total_Orders") <= 3, "Occasional")
            .otherwise("Loyal")
        )
    )

    segment_stats = (
        customer_stats.groupBy("Segment")
        .agg(
            countDistinct("Subscriber_ID").alias("Total_Customers"),
            _round(avg("Total_Orders"), 2).alias("Avg_Orders"),
            _round(avg("Avg_Order_Value"), 2).alias("Avg_Value"),
            _round(avg("Frequency_Days"), 2).alias("Avg_Frequency")
        )
        .collect()
    )

    segment_map = {r["Segment"]: r for r in segment_stats}

    def get_row(seg: str):
        r = segment_map.get(seg)
        if r is None:
            return seg, 0, 0.0, 0.0, 0.0
        return seg, r["Total_Customers"], r["Avg_Orders"], r["Avg_Value"], r["Avg_Frequency"]

    separator = "=" * LINE_WIDTH + "\n"
    divider = "-" * LINE_WIDTH + "\n"
    col_header = (
        f"  {'Nhóm':<16} {'Số khách':>9}  {'Đơn TB':>7}  "
        f"{'Giá trị TB':>11}  {'Tần suất (ngày)':>15}\n"
    )

    criteria = (
        "  Tiêu chí phân nhóm:\n"
        "    One-time   : 1 đơn hàng\n"
        "    Occasional : 2-3 đơn hàng\n"
        "    Loyal      : >= 4 đơn hàng\n"
    )

    rows = (
        fmt_group_row(*get_row("One-time")) +
        fmt_group_row(*get_row("Occasional")) +
        fmt_group_row(*get_row("Loyal"))
    )

    full_output = (
        separator +
        "PHÂN NHÓM KHÁCH HÀNG".center(LINE_WIDTH) + "\n" +
        separator +
        "\n" +
        criteria +
        "\n" +
        divider +
        col_header +
        divider +
        rows +
        separator
    )

    print(full_output, end="")
    output_handle.write(full_output)


# =========================
# MAIN
# =========================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = SparkSession.builder.appName("Task9").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
