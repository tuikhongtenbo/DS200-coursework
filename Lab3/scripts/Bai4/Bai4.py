import os
import sys
from pyspark import SparkConf, SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    data_dir = r"C:\E old\DS200\DS200-coursework\Lab3\dataset"
    output_dir = r"C:\E old\DS200\DS200-coursework\Lab3\output\Bai4"

    conf = SparkConf().setAppName("Bai4").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Tạo map (MovieID -> Title)
    movies_rdd = sc.textFile(os.path.join(data_dir, "movies.txt"))
    movie_titles_bc = sc.broadcast(
        movies_rdd.map(lambda line: (int(line.split(',')[0].strip()), line.split(',')[1].strip())).collectAsMap()
    )

    def get_age_group(age):
        if age < 18:
            return "<18"
        elif age <= 35:
            return "18-35"
        elif age <= 50:
            return "36-50"
        else:
            return "50+"

    # Tạo map (UserID -> Age Group)
    users_rdd = sc.textFile(os.path.join(data_dir, "users.txt"))
    user_age_bc = sc.broadcast(
        users_rdd.map(lambda line: (int(line.split(',')[0].strip()), get_age_group(int(line.split(',')[2].strip())))).collectAsMap()
    )

    #  Đọc ratings và thêm nhóm tuổi -> ((MovieID, Age Group), (Rating, 1))
    r1_rdd = sc.textFile(os.path.join(data_dir, "ratings_1.txt"))
    r2_rdd = sc.textFile(os.path.join(data_dir, "ratings_2.txt"))
    ratings_rdd = r1_rdd.union(r2_rdd)

    def map_age_rating(line):
        fields = line.split(',')
        user_id = int(fields[0].strip())
        movie_id = int(fields[1].strip())
        rating = float(fields[2].strip())
        age_group = user_age_bc.value.get(user_id, "Unknown")
        return ((movie_id, age_group), (rating, 1))

    # Tính trung bình điểm đánh giá theo nhóm tuổi
    age_avg_rdd = ratings_rdd.map(map_age_rating) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1])

    results = age_avg_rdd.sortByKey().collect()

    # Chuẩn bị output và ghi file
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_bai4.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        for (movie_id, age_group), avg in results:
            title = movie_titles_bc.value.get(movie_id, "Unknown")
            row = f"Movie: {title} | Age Group: {age_group:<9} | Avg Rating: {avg:.2f}"
            print(row)
            f.write(row + "\n")

    sc.stop()

if __name__ == "__main__":
    main()