import os
import sys
from pyspark import SparkConf, SparkContext
import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    data_dir = r"C:\E old\DS200\DS200-coursework\Lab3\dataset"
    output_dir = r"C:\E old\DS200\DS200-coursework\Lab3\output\Bai6"

    conf = SparkConf().setAppName("Bai6").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Đọc dữ liệu ratings từ ratings_1.txt và ratings_2.txt
    r1_rdd = sc.textFile(os.path.join(data_dir, "ratings_1.txt"))
    r2_rdd = sc.textFile(os.path.join(data_dir, "ratings_2.txt"))
    ratings_rdd = r1_rdd.union(r2_rdd)

    # Sử dụng hàm trợ giúp để chuyển đổi Timestamp thành năm, phát hành cặp (Năm, (rating, 1))
    def extract_year_rating(line):
        fields = line.split(',')
        rating = float(fields[2].strip())
        timestamp = int(fields[3].strip())
        # Chuyển đổi Unix timestamp thành năm
        year = datetime.datetime.fromtimestamp(timestamp).year
        return (year, (rating, 1))

    # Reduce để tính tổng điểm và số lượt cho mỗi năm, sau đó tính trung bình
    year_stats_rdd = ratings_rdd.map(extract_year_rating) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: (v[0] / v[1], v[1]))

    # Sắp xếp theo năm tăng dần
    results = year_stats_rdd.sortByKey().collect()

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_bai6.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        for year, (avg, count) in results:
            row = f"Year: {year} | Total Votes: {count:<5} | Avg Rating: {avg:.2f}"
            print(row)
            f.write(row + "\n")

    print(f"\n[Da luu ket qua vao {output_file}]")
    sc.stop()

if __name__ == "__main__":
    main()