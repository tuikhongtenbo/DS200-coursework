import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.*;

public class Bai1_MovieRating {

    // --- MAPPER ---
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Map<String, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("movies.txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    // Schema: MovieID, Title, Genres
                    String[] parts = line.split(", ", 3);
                    if (parts.length >= 2) {
                        movieMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
                reader.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Schema Ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = value.toString().split(", ", 4);
            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());
                String title = movieMap.get(movieId);
                if (title != null) {
                    context.write(new Text(title), new DoubleWritable(rating));
                }
            }
        }
    }

    // --- REDUCER ---
    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private String maxMovie = "";
        private double maxRating = 0.0;
        private static final int MIN_RATINGS_FOR_MAX = 5;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = sum / count;

            // Output ALL movies
            context.write(key,
                new Text("Average rating: " + String.format("%.1f", avg) + " (Total ratings: " + count + ")"));

            // Track highest-rated movie among those with >= 5 ratings
            if (count >= MIN_RATINGS_FOR_MAX && avg > maxRating) {
                maxRating = avg;
                maxMovie = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(
                    new Text(maxMovie + " is the highest rated movie with an average rating of "
                        + String.format("%.1f", maxRating) + " among movies with at least 5 ratings."),
                    new Text(""));
            }
        }
    }

    // --- DRIVER ---
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai1_MovieRating");
        job.setJarByClass(Bai1_MovieRating.class);

        job.addCacheFile(new URI("/input/hw/movies.txt#movies.txt"));

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 1 Reducer so cleanup() compares globally
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output/bai1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}