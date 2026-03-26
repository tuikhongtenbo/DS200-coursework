import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.*;

public class Bai4_AgeGroupRating {

    // === MAPPER ===
    public static class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String>  movieTitleMap = new HashMap<>(); // MovieID -> Title
        private Map<String, Integer> userAgeMap    = new HashMap<>(); // UserID  -> Age

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
                                String[] parts = line.split(", ", 3);
                                if (parts.length >= 2) {
                                    movieTitleMap.put(parts[0].trim(), parts[1].trim());
                                }
                            }
                        }
                    } else if (filename.equals("users.txt")) {
                        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] parts = line.split(", ", 5);
                                if (parts.length >= 3) {
                                    userAgeMap.put(parts[0].trim(), Integer.parseInt(parts[2].trim()));
                                }
                            }
                        }
                    }
                }
            }
        }

        private String getAgeGroup(int age) {
            if (age <= 18) return "0-18";
            if (age <= 35) return "18-35";
            if (age <= 50) return "35-50";
            return "50+";
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Schema Ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = value.toString().split(", ", 4);
            if (parts.length >= 3) {
                String userId  = parts[0].trim();
                String movieId = parts[1].trim();
                double rating  = Double.parseDouble(parts[2].trim());

                Integer age  = userAgeMap.get(userId);
                String title = movieTitleMap.get(movieId);

                if (age != null && title != null) {
                    String ageGroup = getAgeGroup(age);
                    // Output: Key=MovieTitle, Value=AgeGroup:Rating
                    context.write(new Text(title), new Text(ageGroup + ":" + rating));
                }
            }
        }
    }

    // === REDUCER ===
    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Store ratings per age group
            Map<String, List<Double>> groupRatings = new LinkedHashMap<>();
            String[] groups = {"0-18", "18-35", "35-50", "50+"};
            for (String g : groups) {
                groupRatings.put(g, new ArrayList<>());
            }

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String group  = parts[0].trim();
                    double rating = Double.parseDouble(parts[1].trim());
                    if (groupRatings.containsKey(group)) {
                        groupRatings.get(group).add(rating);
                    }
                }
            }

            // Build output: 0-18: X.XX  18-35: X.XX  35-50: X.XX  50+: X.XX
            // Show "NA" if a group has no ratings
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < groups.length; i++) {
                String group = groups[i];
                List<Double> ratings = groupRatings.get(group);
                sb.append(group).append(": ");
                if (ratings.isEmpty()) {
                    sb.append("NA");
                } else {
                    double sum = 0;
                    for (double r : ratings) sum += r;
                    sb.append(String.format("%.2f", sum / ratings.size()));
                }
                if (i < groups.length - 1) sb.append("  ");
            }

            context.write(key, new Text(sb.toString()));
        }
    }

    // === DRIVER ===
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai4_AgeGroupRating");
        job.setJarByClass(Bai4_AgeGroupRating.class);

        job.addCacheFile(new URI("/input/hw/movies.txt#movies.txt"));
        job.addCacheFile(new URI("/input/hw/users.txt#users.txt"));

        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_1.txt"));
        FileInputFormat.addInputPath(job, new Path("/input/hw/ratings_2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output/bai4"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
