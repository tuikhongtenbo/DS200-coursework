import os
import sys
from pyspark import SparkConf, SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    data_dir = r"C:\E old\DS200\DS200-coursework\Lab3\dataset"
    output_dir = r"C:\E old\DS200\DS200-coursework\Lab3\output\Bai3"

    conf = SparkConf().setAppName("Bai3").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Tạo map (MovieID -> Title)
    movies_rdd = sc.textFile(os.path.join(data_dir, "movies.txt"))
    movie_titles_bc = sc.broadcast(
        movies_rdd.map(lambda line: (int(line.split(',')[0].strip()), line.split(',')[1].strip())).collectAsMap()
    )

    #  map (UserID -> Gender)
    users_rdd = sc.textFile(os.path.join(data_dir, "users.txt"))
    user_gender_bc = sc.broadcast(
        users_rdd.map(lambda line: (int(line.split(',')[0].strip()), line.split(',')[1].strip())).collectAsMap()
    )

    # Đọc ratings và thêm thông tin giới tính -> ((MovieID, Gender), (Rating, 1))
    r1_rdd = sc.textFile(os.path.join(data_dir, "ratings_1.txt"))
    r2_rdd = sc.textFile(os.path.join(data_dir, "ratings_2.txt"))
    ratings_rdd = r1_rdd.union(r2_rdd)

    def map_gender_rating(line):
        fields = line.split(',')
        user_id = int(fields[0].strip())
        movie_id = int(fields[1].strip())
        rating = float(fields[2].strip())
        gender = user_gender_bc.value.get(user_id, "Unknown")
        return ((movie_id, gender), (rating, 1))

    # Tính trung bình rating cho mỗi phim theo từng giới tính
    gender_avg_rdd = ratings_rdd.map(map_gender_rating) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1])

    results = gender_avg_rdd.sortByKey().collect()

    # Chuẩn bị output và ghi file
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_bai3.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        for (movie_id, gender), avg in results:
            title = movie_titles_bc.value.get(movie_id, "Unknown")
            row = f"MovieID: {movie_id}, Title: {title}, Gender: {gender}, Avg Rating: {avg:.2f}"
            print(row)
            f.write(row + "\n")

    sc.stop()

if __name__ == "__main__":
    main()