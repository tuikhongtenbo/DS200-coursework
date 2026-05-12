import os
import sys
from pyspark import SparkConf, SparkContext

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def parse_movie(line):
    fields = line.split(',')
    return (int(fields[0].strip()), fields[1].strip())

def parse_rating(line):
    fields = line.split(',')
    return (int(fields[1].strip()), (float(fields[2].strip()), 1))

def main():
    data_dir = r"C:\E old\DS200\DS200-coursework\Lab3\dataset"
    output_dir = r"C:\E old\DS200\DS200-coursework\Lab3\output"
    min_ratings = 5

    conf = SparkConf().setAppName("Bai1").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Đọc movies.txt -> (MovieID, Title)
    movies_rdd = sc.textFile(os.path.join(data_dir, "movies.txt"))
    movie_titles = movies_rdd.map(parse_movie).collectAsMap()
    movie_titles_bc = sc.broadcast(movie_titles)

    # Đọc ratings -> (MovieID, (Rating, 1))
    r1_rdd = sc.textFile(os.path.join(data_dir, "ratings_1.txt"))
    r2_rdd = sc.textFile(os.path.join(data_dir, "ratings_2.txt"))
    ratings_rdd = r1_rdd.union(r2_rdd)

    ratings_mapped = ratings_rdd.map(parse_rating)

    # Tính tổng điểm và tổng số lượt đánh giá
    ratings_reduced = ratings_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Tính điểm trung bình và lọc
    filtered_movies = ratings_reduced \
        .mapValues(lambda v: (v[0] / v[1], v[1])) \
        .filter(lambda x: x[1][1] >= min_ratings)

    # Chuẩn bị output
    os.makedirs(output_dir, exist_ok=True)
    output_file_all = os.path.join(output_dir, "output.txt")
    output_file_top = os.path.join(output_dir, "highest_avg_film.txt")

    results = filtered_movies.collect()
    
    # Ghi file output.txt
    with open(output_file_all, "w", encoding="utf-8") as f_all:
        for movie_id, (avg_rating, count) in results:
            title = movie_titles_bc.value.get(movie_id, "Unknown")
            row = f"MovieID: {movie_id}, Title: {title}, Avg: {avg_rating:.2f}, Count: {count}"
            print(row)
            f_all.write(row + "\n")

    # Tìm phim có điểm trung bình cao nhất và ghi file highest_avg_film.txt
    with open(output_file_top, "w", encoding="utf-8") as f_top:
        if results:
            best_movie = max(results, key=lambda x: x[1][0])
            title = movie_titles_bc.value.get(best_movie[0], "Unknown")
            top_row = f"MovieID: {best_movie[0]}, Title: {title}, Avg: {best_movie[1][0]:.2f}, Count: {best_movie[1][1]}"
            print("\n[TOP MOVIE]")
            print(top_row)
            f_top.write(top_row + "\n")
        else:
            msg = f"Khong co phim nao co it nhat {min_ratings} luot danh gia."
            print(msg)
            f_top.write(msg + "\n")

    print(f"\n[Da luu ket qua vao {output_dir}]")
    sc.stop()

if __name__ == "__main__":
    main()