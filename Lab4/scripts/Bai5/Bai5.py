"""
Task 5:
Thống kê điểm đánh giá trung bình, số lượng đánh giá theo từng mức (ví dụ: 1 đến 5).
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, count

# =========================
# CONFIG
# =========================
DATASET_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\dataset"
OUTPUT_DIR = r"C:\E old\DS200\DS200-coursework\Lab4\output"
OUTPUT_FILE = "task_5_output.txt"

LINE_WIDTH = 50


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
    Thống kê điểm đánh giá và số lượng từng mức điểm ra output/màn hình.
    Review_Score có thể bị suy luận sai kiểu (STRING), cần filter bằng regex trước khi cast.
    """
    spark.conf.set("spark.sql.ansi.enabled", "false")

    reviews_df = load_csv(spark, os.path.join(DATASET_DIR, "Order_Reviews.csv"))

    valid_reviews_df = (
        reviews_df.filter(col("Review_Score").rlike(r"^[1-5]$"))
        .withColumn("Review_Score", col("Review_Score").cast("int"))
    )

    avg_score = valid_reviews_df.select(avg("Review_Score")).collect()[0][0]

    count_by_score_df = (
        valid_reviews_df.groupBy("Review_Score")
        .agg(count("*").alias("Total_Reviews"))
        .orderBy(col("Review_Score").asc())
        .collect()
    )

    separator = "=" * LINE_WIDTH + "\n"
    title = "THỐNG KÊ ĐIỂM ĐÁNH GIÁ".center(LINE_WIDTH) + "\n"
    divider = "-" * LINE_WIDTH + "\n"

    rows = ""
    for row in count_by_score_df:
        score = row["Review_Score"]
        total = f"{row['Total_Reviews']:,}"
        rows += f"  {score} sao{'':10} {total:>10}\n"

    full_output = (
        separator +
        title +
        separator +
        f"  Điểm trung bình tổng thể: {avg_score:.2f} / 5.00\n" +
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
    spark = SparkSession.builder.appName("Task5").getOrCreate()
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print(f"Đang xử lý dữ liệu và ghi kết quả ra {output_path}...\n")

    with open(output_path, "w", encoding="utf-8") as output_file:
        process_task(spark, output_file)

    spark.stop()
    print("Completed.")


if __name__ == "__main__":
    main()
