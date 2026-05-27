"""
Task 10:
Xếp hạng các seller dựa trên tổng doanh thu và số lượng đơn hàng bán được.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, countDistinct, round as _round
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_10_output.txt"

LINE_WIDTH = 110


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


def fmt_row(rank: int, seller_id: str, revenue: float, price: float, freight: float, orders: int) -> str:
    return f"  {rank:>4}  {seller_id:<35} {revenue:>13,.2f} {price:>14,.2f} {freight:>14,.2f} {orders:>12,}\n"


def process_task(spark: SparkSession, output_handle) -> None:
    """
    Xếp hạng Seller ra output/màn hình.
    """
    items_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Items.csv"))

    seller_stats = (
        items_df.withColumn("Total_Item_Value", col("Price") + col("Freight_Value"))
        .groupBy("Seller_ID")
        .agg(
            _round(_sum("Total_Item_Value"), 2).alias("Total_Revenue"),
            _round(_sum("Price"), 2).alias("Total_Price"),
            _round(_sum("Freight_Value"), 2).alias("Total_Freight"),
            countDistinct("Order_ID").alias("Total_Orders")
        )
    )

    windowSpec = Window.orderBy(col("Total_Revenue").desc(), col("Total_Orders").desc())

    ranked_sellers = (
        seller_stats.withColumn("Rank", dense_rank().over(windowSpec))
        .orderBy("Rank")
        .collect()
    )

    separator = "=" * LINE_WIDTH + "\n"
    divider = "-" * LINE_WIDTH + "\n"
    
    col_header = f"  {'Rank':>4}  {'Seller_ID':<35} {'Total_Revenue':>13} {'Total_Price':>14} {'Total_Freight':>14} {'Total_Orders':>12}\n"

    rows = ""
    for r in ranked_sellers:
        rows += fmt_row(
            r["Rank"], 
            r["Seller_ID"], 
            r["Total_Revenue"], 
            r["Total_Price"], 
            r["Total_Freight"], 
            r["Total_Orders"]
        )

    full_output = (
        separator +
        "XẾP HẠNG SELLER THEO DOANH THU & SỐ ĐƠN".center(LINE_WIDTH) + "\n" +
        separator +
        "\n" +
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
    spark = SparkSession.builder.appName("Task10").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
