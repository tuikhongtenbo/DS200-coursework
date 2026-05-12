import os
import sys
from pyspark import SparkConf, SparkContext

# Đồng bộ Python version để tránh lỗi Python worker crashed
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    data_dir = r"C:\E old\DS200\DS200-coursework\Lab3\dataset"
    output_dir = r"C:\E old\DS200\DS200-coursework\Lab3\output\Bai2"

    conf = SparkConf().setAppName("Bai2").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Bước 1: Đọc movies.txt -> Map(MovieID -> List of Genres)
    def parse_movie_genres(line):
        fields = line.split(',')
        movie_id = int(fields[0].strip())

        genres_str = fields[-1].strip()
        genres = genres_str.split('|')
        return (movie_id, genres)

    movies_rdd = sc.textFile(os.path.join(data_dir, "movies.txt"))
    movie_genres_map = movies_rdd.map(parse_movie_genres).collectAsMap()
    movie_genres_bc = sc.broadcast(movie_genres_map)

    # Bước 2: Đọc ratings_1.txt và ratings_2.txt
    r1_rdd = sc.textFile(os.path.join(data_dir, "ratings_1.txt"))
    r2_rdd = sc.textFile(os.path.join(data_dir, "ratings_2.txt"))
    ratings_rdd = r1_rdd.union(r2_rdd)

    def extract_genre_ratings(line):
        fields = line.split(',')
        movie_id = int(fields[1].strip())
        rating = float(fields[2].strip())
        
        genres = movie_genres_bc.value.get(movie_id, [])
        # [(Genre1, (Rating, 1)), (Genre2, (Rating, 1)), ...]
        return [(genre, (rating, 1)) for genre in genres]

    genre_ratings_rdd = ratings_rdd.flatMap(extract_genre_ratings)

    # Bước 3: Reduce tính tổng điểm và tổng số lượt đánh giá cho từng thể loại, sau đó tính trung bình
    genre_avg_rdd = genre_ratings_rdd \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1])

    # Sắp xếp theo điểm trung bình giảm dần
    sorted_genres = genre_avg_rdd.sortBy(lambda x: x[1], ascending=False).collect()

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "output_bai2.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        header1 = "Genre, Average Rating"
        header2 = "-" * 30
        print(header1)
        print(header2)
        f.write(header1 + "\n")
        f.write(header2 + "\n")

        for genre, avg in sorted_genres:
            row = f"Genre: {genre:<15} | Avg Rating: {avg:.2f}"
            print(row)
            f.write(row + "\n")

    print(f"\n[Da luu ket qua vao {output_file}]")
    sc.stop()

if __name__ == "__main__":
    main()