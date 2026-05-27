"""
Task 7:
Xác định sản phẩm có số lượng bán ra cao nhất và tính điểm đánh giá trung bình cho từng sản phẩm.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, avg, round as _round

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_7_output.txt"


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
    Tìm sản phẩm bán chạy nhất kèm đánh giá trung bình ra output/màn hình.
    """
    spark.conf.set("spark.sql.ansi.enabled", "false")

    items_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Items.csv"))
    reviews_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Reviews.csv"))
    products_df = load_csv(spark, os.path.join(DATASET_DIR, "Products.csv"))

    sales_df = items_df.groupBy("Product_ID").agg(count("Order_Item_ID").alias("Total_Sold"))

    item_reviews = items_df.join(reviews_df, "Order_ID", "left")

    avg_reviews_df = (
        item_reviews.filter(col("Review_Score").rlike(r"^[1-5]$"))
        .withColumn("Review_Score", col("Review_Score").cast("int"))
        .groupBy("Product_ID")
        .agg(_round(avg("Review_Score"), 4).alias("Avg_Review_Score"))
    )

    result_df = (
        sales_df.join(avg_reviews_df, "Product_ID", "left")
        .join(products_df.select("Product_ID", "Product_Category_Name"), "Product_ID", "left")
        .orderBy(col("Total_Sold").desc())
    )

    total_rows = result_df.count()
    output_str = result_df._jdf.showString(total_rows, 50, False)

    full_output = (
        "=" * 50 + "\n" +
        "SẢN PHẨM BÁN CHẠY NHẤT VÀ ĐÁNH GIÁ TRUNG BÌNH\n" +
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
    spark = SparkSession.builder.appName("Task7").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
