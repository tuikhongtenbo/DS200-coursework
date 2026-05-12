import os
import sys
from pyspark import SparkConf, SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def main():
    data_dir = r"C:\E old\DS200\DS200-coursework\Lab3\dataset"
    output_dir = r"C:\E old\DS200\DS200-coursework\Lab3\output\Bai5"

    conf = SparkConf().setAppName("Bai5").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Tạo dictionary từ occupation.txt với mapping ID -> Occupation
    occ_rdd = sc.textFile(os.path.join(data_dir, "occupation.txt"))
    occ_map_bc = sc.broadcast(
        occ_rdd.map(lambda line: (int(line.split(',')[0].strip()), line.split(',')[1].strip())).collectAsMap()
    )

    # Tạo map (UserID -> Occupation Name)
    users_rdd = sc.textFile(os.path.join(data_dir, "users.txt"))
    user_occ_bc = sc.broadcast(
        users_rdd.map(lambda line: (int(line.split(',')[0].strip()), occ_map_bc.value.get(int(line.split(',')[3].strip()), "Unknown"))).collectAsMap()
    )

    # Với mỗi rating, gán thông tin Occupation theo UserID
    r1_rdd = sc.textFile(os.path.join(data_dir, "ratings_1.txt"))
    r2_rdd = sc.textFile(os.path.join(data_dir, "ratings_2.txt"))
    ratings_rdd = r1_rdd.union(r2_rdd)

    def map_occ_rating(line):
        fields = line.split(',')
        user_id = int(fields[0].strip())
        rating = float(fields[2].strip())
        occ_name = user_occ_bc.value.get(user_id, "Unknown")
        return (occ_name, (rating, 1))

    # Phát hành cặp key-value với key là Occupation và value là (rating, 1)
    occ_avg_rdd = ratings_rdd.map(map_occ_rating) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: (v[0] / v[1], v[1]))

    results = occ_avg_rdd.sortBy(lambda x: x[1][0], ascending=False).collect()

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_bai5.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        for occ, (avg, count) in results:
            row = f"Occupation: {occ:<20} | Avg Rating: {avg:.2f} | Total Votes: {count}"
            print(row)
            f.write(row + "\n")

    sc.stop()

if __name__ == "__main__":
    main()