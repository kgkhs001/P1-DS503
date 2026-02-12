package com.krishnagarg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class F {

    /**
     * JOB 1: Calculate Average Followers
     * Mapper: Reads Follows.csv, outputs (ID, 1)
     */
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text userBeingFollowed = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                // ID2 is the person receiving the follow
                userBeingFollowed.set(parts[2]);
                context.write(userBeingFollowed, one);
            }
        }
    }

    //JOB 1 Reducer: Sums followers per user, then calculates global average
    public static class AvgReducer extends Reducer<Text, IntWritable, Text, Text> {
        private long totalFollows = 0;
        private long totalUsers = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalFollows += sum;
            totalUsers++;         // used later for global average
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (totalUsers > 0) {
                double average = (double) totalFollows / totalUsers;
                // Output a single key-value pair representing the global average
                context.write(new Text("AVERAGE"), new Text(String.valueOf(average)));
            }
        }
    }


    /**
     * JOB 2: Filter popular (above average) users
     * Mapper: Counts followers
     */
    public static class FilterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text userBeingFollowed = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                userBeingFollowed.set(parts[2]);
                context.write(userBeingFollowed, one);
            }
        }
    }

    //JOB 2 Reducer: Uses distributed cache to get average, then filters
    public static class FilterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private double globalAverage = 0;

        // Reads the average from the Distributed Cache
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                // Use the URI to get path, usually simpler on HDFS
                Path path = new Path(cacheFiles[0].getPath());
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line = br.readLine();
                    if (line != null) {
                        try {
                             // Job 1 outputs: AVERAGE [tab] value
                            globalAverage = Double.parseDouble(line.split("\t")[1]);
                        } catch (Exception e) {
                            // ignore or log
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            if (count > globalAverage) {
                context.write(key, new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length < 2) {
            System.err.println("Usage: F <input path> <output path>");
            System.exit(-1);
        }

        Path followsPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path intermediateOutput = new Path(outputPath.toString() + "_intermediate");

        // Cleanup output folders if exist
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(intermediateOutput)) {
             fs.delete(intermediateOutput, true);
        }
        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

        // Job 1 Configuration
        Job job1 = Job.getInstance(conf, "Calculate Average");
        job1.setJarByClass(F.class);
        job1.setMapperClass(AvgMapper.class);
        job1.setCombinerClass(IntSumCombiner.class); 
        job1.setReducerClass(AvgReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setNumReduceTasks(1); 
        
        FileInputFormat.addInputPath(job1, followsPath);
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (job1.waitForCompletion(true)) {
            // Job 2 Configuration
            Job job2 = Job.getInstance(conf, "Filter Popular Users");
            job2.setJarByClass(F.class);

            // Add Job 1 result to Distributed Cache
            URI cacheUri = new URI(intermediateOutput.toString() + "/part-r-00000");
            job2.addCacheFile(cacheUri);

            job2.setMapperClass(FilterMapper.class);
            job2.setCombinerClass(IntSumCombiner.class);
            job2.setReducerClass(FilterReducer.class);
            
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, followsPath);
            FileOutputFormat.setOutputPath(job2, outputPath);

            boolean success = job2.waitForCompletion(true);
            
            // Cleanup intermediate
            if (fs.exists(intermediateOutput)) {
                fs.delete(intermediateOutput, true);
            }
            
            System.exit(success ? 0 : 1);
        }
    }

    // Helper Combiner class for both jobs
    public static class IntSumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}