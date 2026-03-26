import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.*;

public class Bai3_GenderRating {

    // === MAPPER ===
    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> movieTitleMap = new HashMap<>(); // MovieID -> Title
        private Map<String, String> userGenderMap = new HashMap<>(); // UserID -> Gender

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    String filename = new File(uri.getPath()).getName();
                    if (filename.equals("movies.txt")) {
                        // Schema: MovieID, Title, Genres
                        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] parts = line.split(", ", 3);
                                if (parts.length >= 2) {
                                    movieTitleMap.put(parts[0].trim(), parts[1].trim());
                                }
                            }
                        }
                    } else if (filename.equals("users.txt")) {
                        // Schema: UserID, Gender, Age, Occupation, Zip-code
                        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] parts = line.split(", ", 5);
                                if (parts.length >= 2) {
                                    userGenderMap.put(parts[0].trim(), parts[1].trim());
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
                String userId  = parts[0].trim();
                String movieId = parts[1].trim();
                String rating  = parts[2].trim();

                String gender = userGenderMap.get(userId);
                String title  = movieTitleMap.get(movieId);

                if (gender != null && title != null) {
                    // Output: Key=MovieTitle, Value=Gender:Rating
                    context.write(new Text(title), new Text(gender + ":" + rating));
                }
            }
        }
    }

    // === REDUCER ===
    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String gender = parts[0].trim();
                    double rating = Double.parseDouble(parts[1].trim());
                    if (gender.equalsIgnoreCase("M")) {
                        maleSum += rating;
                        maleCount++;
                    } else if (gender.equalsIgnoreCase("F")) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }
            }

            double maleAvg   = (maleCount > 0)   ? maleSum / maleCount     : 0.0;
            double femaleAvg = (femaleCount > 0)  ? femaleSum / femaleCount : 0.0;

            // Format: Male: X.XX, Female: X.XX
            String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);
            context.write(key, new Text(result));
        }
    }

    // === DRIVER ===
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai3_GenderRating");
        job.setJarByClass(Bai3_GenderRating.class);

        job.addCacheFile(new URI("/input/hw/movies.txt#movies.txt"));
        job.addCacheFile(new URI("/input/hw/users.txt#users.txt"));

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output/bai3"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}