"""
Task 8:
Tính hiệu số ngày giao hàng thực tế và ngày giao hàng dự kiến để đánh giá hiệu suất.
Delay_Days = Order_Delivered_Carrier_Date - Shipping_Limit_Date
Delay_Days > 0: trễ hạn | Delay_Days <= 0: đúng/sớm hạn
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, datediff, to_timestamp, avg, count, when

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_8_output.txt"

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


def fmt_row(label: str, value: str, label_width: int = 28) -> str:
    return f"  {label:<{label_width}} {value:>14}\n"


def process_task(spark: SparkSession, output_handle) -> None:
    """
    Tính độ trễ giao hàng (thực tế - dự kiến) và phân loại ra output/màn hình.
    """
    orders_df = load_csv(spark, os.path.join(DATASET_DIR, "Orders.csv"))
    items_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Items.csv"))

    delivered_df = (
        orders_df.join(items_df, "Order_ID", "inner")
        .filter(
            col("Order_Delivered_Carrier_Date").isNotNull() &
            col("Shipping_Limit_Date").isNotNull()
        )
        .withColumn(
            "Delay_Days",
            datediff(
                to_timestamp(col("Order_Delivered_Carrier_Date")),
                to_timestamp(col("Shipping_Limit_Date"))
            )
        )
        .withColumn(
            "Status",
            when(col("Delay_Days") > 0, "Trễ hạn").otherwise("Đúng/Sớm hạn")
        )
        .select("Order_ID", "Delay_Days", "Status")
        .dropDuplicates(["Order_ID"])
    )

    stats = delivered_df.agg(
        count("*").alias("total"),
        avg("Delay_Days").alias("avg_delay")
    ).collect()[0]

    status_stats = (
        delivered_df.groupBy("Status")
        .agg(
            count("*").alias("count"),
            avg("Delay_Days").alias("avg_delay")
        )
        .collect()
    )

    total = stats["total"]
    avg_delay = stats["avg_delay"]

    late_row = next((r for r in status_stats if r["Status"] == "Trễ hạn"), None)
    ontime_row = next((r for r in status_stats if r["Status"] == "Đúng/Sớm hạn"), None)

    late_count = late_row["count"] if late_row else 0
    ontime_count = ontime_row["count"] if ontime_row else 0
    late_avg = late_row["avg_delay"] if late_row else 0.0
    ontime_avg = ontime_row["avg_delay"] if ontime_row else 0.0

    separator = "=" * LINE_WIDTH + "\n"
    divider = "-" * LINE_WIDTH + "\n"
    sub_divider = "  " + "-" * (LINE_WIDTH - 2) + "\n"

    col_header = f"  {'Trạng thái':<20} {'Số đơn':>10}  {'Hiệu số TB (ngày)':>18}\n"
    col_divider = "  " + "-" * 52 + "\n"
    late_line = f"  {'Trễ hạn':<20} {late_count:>10,}  {late_avg:>18.2f}\n"
    ontime_line = f"  {'Đúng/Sớm hạn':<20} {ontime_count:>10,}  {ontime_avg:>18.2f}\n"

    full_output = (
        separator +
        "THỐNG KÊ HIỆU SUẤT GIAO HÀNG".center(LINE_WIDTH) + "\n" +
        separator +
        "\n" +
        f"  {'Tổng quan':}\n" +
        divider +
        fmt_row("Tổng đơn hàng:", f"{total:,}") +
        fmt_row("Hiệu số ngày TB:", f"{avg_delay:.2f}") +
        fmt_row("Đúng/Sớm hạn:", f"{ontime_count:,}") +
        fmt_row("Trễ hạn:", f"{late_count:,}") +
        "\n" +
        f"  {'Phân loại theo trạng thái':}\n" +
        divider +
        col_header +
        col_divider +
        late_line +
        ontime_line +
        separator
    )

    print(full_output, end="")
    output_handle.write(full_output)


# =========================
# MAIN
# =========================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = SparkSession.builder.appName("Task8").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
