import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.*;

public class Bai2_GenreRating {

    // === MAPPER ===
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Map<String, String> movieGenreMap = new HashMap<>(); // MovieID -> Genres

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    String filename = new File(uri.getPath()).getName();
                    if (filename.equals("movies.txt")) {
                        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                // Schema: MovieID, Title, Genres
                                String[] parts = line.split(", ", 3);
                                if (parts.length >= 3) {
                                    movieGenreMap.put(parts[0].trim(), parts[2].trim());
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Schema Ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = value.toString().split(", ", 4);
            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());
                String genres = movieGenreMap.get(movieId);

                if (genres != null) {
                    // Split multiple genres: Action|Comedy|Drama
                    String[] genreList = genres.split("\\|");
                    for (String genre : genreList) {
                        context.write(new Text(genre.trim()), new DoubleWritable(rating));
                    }
                }
            }
        }
    }

    // === REDUCER ===
    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = (count > 0) ? sum / count : 0.0;
            // Format: Avg: X.XX, Count: XX
            String output = String.format("Avg: %.2f, Count: %d", avg, count);
            context.write(key, new Text(output));
        }
    }

    // === DRIVER ===
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai2_GenreRating");
        job.setJarByClass(Bai2_GenreRating.class);

        job.addCacheFile(new URI("/input/hw/movies.txt#movies.txt"));

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output/bai2"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}