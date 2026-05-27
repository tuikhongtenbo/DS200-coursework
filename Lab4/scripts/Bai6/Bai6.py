"""
Task 6:
Tính doanh thu (giá sản phẩm + phí vận chuyển) trong năm 2024 và nhóm theo danh mục sản phẩm.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, col, sum as _sum, round as _round, to_timestamp

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_6_output.txt"

COL_CATEGORY = 40
COL_NUMBER = 16


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


def format_header() -> str:
    cat = "Product_Category"
    rev = "Total_Revenue"
    price = "Price"
    freight = "Freight_Value"
    divider = "-" * (COL_CATEGORY + COL_NUMBER * 3 + 2) + "\n"
    header = f"  {cat:<{COL_CATEGORY}} {rev:>{COL_NUMBER}} {price:>{COL_NUMBER}} {freight:>{COL_NUMBER}}\n"
    return header + divider


def format_row(category: str, revenue: float, price: float, freight: float) -> str:
    return (
        f"  {category:<{COL_CATEGORY}}"
        f" {revenue:>{COL_NUMBER},.2f}"
        f" {price:>{COL_NUMBER},.2f}"
        f" {freight:>{COL_NUMBER},.2f}\n"
    )


def process_task(spark: SparkSession, output_handle) -> None:
    """
    Tính doanh thu 2024 theo danh mục sản phẩm ra output/màn hình.
    """
    orders_df = load_csv(spark, os.path.join(DATASET_DIR, "Orders.csv"))
    items_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Items.csv"))
    products_df = load_csv(spark, os.path.join(DATASET_DIR, "Products.csv"))

    orders_2024 = orders_df.filter(year(to_timestamp(col("Order_Purchase_Timestamp"))) == 2024)

    joined_df = (
        orders_2024.join(items_df, "Order_ID", "inner")
        .join(products_df, "Product_ID", "inner")
    )

    revenue_df = (
        joined_df.groupBy("Product_Category_Name")
        .agg(
            _round(_sum("Price"), 2).alias("Total_Price"),
            _round(_sum("Freight_Value"), 2).alias("Total_Freight"),
            _round(_sum(col("Price") + col("Freight_Value")), 2).alias("Total_Revenue")
        )
        .orderBy(col("Total_Revenue").desc())
        .collect()
    )

    separator = "=" * (COL_CATEGORY + COL_NUMBER * 3 + 4) + "\n"
    title = "DOANH THU NĂM 2024 THEO DANH MỤC SẢN PHẨM".center(COL_CATEGORY + COL_NUMBER * 3 + 4) + "\n"

    rows = ""
    for row in revenue_df:
        rows += format_row(
            row["Product_Category_Name"] or "N/A",
            row["Total_Revenue"],
            row["Total_Price"],
            row["Total_Freight"]
        )

    full_output = (
        separator +
        title +
        separator +
        format_header() +
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
    spark = SparkSession.builder.appName("Task6").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
