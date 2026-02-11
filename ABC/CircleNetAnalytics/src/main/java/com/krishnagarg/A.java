package com.krishnagarg;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class A {
    public static class FrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // We do this because it is better to declare these once for memory efficiency
        private final static IntWritable one = new IntWritable(1);
        private Text hobby = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // This finds the hobby column of the data
            String fav_hobby = value.toString().split(",")[4].trim();
                // This sets the found hobby in the hadoop acceptable type
                hobby.set(fav_hobby);
                context.write(hobby, one);
            }
    }


    public static class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // An attempt to aggregate the instances of a hobby
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "A");
        job.setJarByClass(A.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(FrequencyMapper.class);
        job.setCombinerClass(FrequencyReducer.class);
        job.setReducerClass(FrequencyReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}