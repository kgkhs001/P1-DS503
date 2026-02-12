package com.krishnagarg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class G {

    /**
     * JOB 1: Find the maximum timestamp in the Activity dataset.
     */
    public static class MaxTimeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static Text timeKey = new Text("MAX_TIME");
        private LongWritable actionTime = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 5) {
                try {
                    // ActionTime is the 5th column
                    actionTime.set(Long.parseLong(parts[4].trim()));
                    context.write(timeKey, actionTime);
                } catch (NumberFormatException e) { /* Skip header or bad data */ }
            }
        }
    }

    /**
     * JOB 1 Reducer/Combiner: Simple max calculation.
     */
    public static class MaxTimeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long max = Long.MIN_VALUE;
            for (LongWritable val : values) {
                max = Math.max(max, val.get());
            }
            context.write(key, new LongWritable(max));
        }
    }

    /**
     * JOB 2 Mapper: Identifies outdated users.
     * Uses Distributed Cache for the Page nicknames and the Max Time value.
     */
    public static class OutdatedMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> idToNickname = new HashMap<>();
        private Map<String, Long> userLastActive = new HashMap<>();
        private long globalMaxTime = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI fileUri : cacheFiles) {
                    Path path = new Path(fileUri.getPath());
                    String fileName = path.getName();

                    // Case A: Loading the Page Nicknames
                    if (fileName.contains("circleNetPage") || fileName.contains("pages")) {
                        loadPageData(path, context.getConfiguration());
                    }
                    // Case B: Loading the Max Time from Job 1
                    else if (fileName.contains("part-r-00000")) {
                        loadMaxTime(path, context.getConfiguration());
                    }
                }
            }
        }

        private void loadPageData(Path path, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        idToNickname.put(parts[0].trim(), parts[1].trim()); // PageID -> Name
                    }
                }
            }
        }

        private void loadMaxTime(Path path, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line = br.readLine();
                if (line != null) {
                    String[] parts = line.split("\t");
                    if (parts.length >= 2) {
                        globalMaxTime = Long.parseLong(parts[1].trim());
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 5) {
                String personId = parts[1].trim();
                try {
                    long time = Long.parseLong(parts[4].trim());
                    // Keep track of the latest activity for every user seen in the log
                    userLastActive.put(personId, Math.max(userLastActive.getOrDefault(personId, 0L), time));
                } catch (NumberFormatException e) {
                    // Skip header or malformed lines
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // After processing all logs, check every user in the Page dataset
            for (Map.Entry<String, String> entry : idToNickname.entrySet()) {
                String userId = entry.getKey();
                String nickname = entry.getValue();
                long lastSeen = userLastActive.getOrDefault(userId, 0L);

                // If last activity was > 90 days ago (or they never had activity)
                if (globalMaxTime - lastSeen > 90) {
                    context.write(new Text(userId), new Text(nickname));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // args[0]: ActivityLog
        // args[1]: CircleNetPage
        // args[2]: Output

        if (args.length < 3) {
            System.err.println("Usage: G <activity_log> <pages_file> <output_path>");
            System.exit(-1);
        }

        Path activityInput = new Path(args[0]);
        Path pagesInput = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Path intermediateOutput = new Path(outputPath.toString() + "_intermediate");

        // Cleanup previous output folder if exists (to avoid FileAlreadyExistsException)
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            System.out.println("Output path exists, deleting: " + outputPath);
            fs.delete(outputPath, true);
        }
        if (fs.exists(intermediateOutput)) {
             fs.delete(intermediateOutput, true);
        }

        // JOB 1: Find Max Time
        Job job1 = Job.getInstance(conf, "Find Max Action Time");
        job1.setJarByClass(G.class);
        job1.setMapperClass(MaxTimeMapper.class);
        job1.setCombinerClass(MaxTimeReducer.class); // Optimization: Combiner
        job1.setReducerClass(MaxTimeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        
        job1.setNumReduceTasks(1); // Single reducer for global max

        FileInputFormat.addInputPath(job1, activityInput);
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (job1.waitForCompletion(true)) {
            // JOB 2: Filter Outdated Users
            Job job2 = Job.getInstance(conf, "Identify Outdated Pages");
            job2.setJarByClass(G.class);
            job2.setMapperClass(OutdatedMapper.class);
            job2.setNumReduceTasks(0); // Map-only job

            // Add Distributed Cache files
            job2.addCacheFile(new URI(intermediateOutput.toString() + "/part-r-00000"));
            job2.addCacheFile(pagesInput.toUri());

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, activityInput);
            FileOutputFormat.setOutputPath(job2, outputPath);

            boolean success = job2.waitForCompletion(true);

            // Cleanup
            if (fs.exists(intermediateOutput)) {
                fs.delete(intermediateOutput, true);
            }

            System.exit(success ? 0 : 1);
        }
    }
}