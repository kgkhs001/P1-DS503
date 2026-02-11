package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.protobuf.MapEntry;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.example.utils.IdentifyFile;

public class taskd_combiner {

    private static class job1Mapper
            extends Mapper<Object, Text, Text, Text> {

        private  Text id1 = new Text();
        private  Text t = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            IdentifyFile.File fileType = IdentifyFile.identify(value.toString());
            if (fileType != IdentifyFile.File.Follows && fileType != IdentifyFile.File.CircleNet){
                return;
            }

            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            String item1 = itr.nextToken();
            String item2 = itr.nextToken();
            String item3 = itr.nextToken();
            if (fileType == IdentifyFile.File.Follows) {

                id1.set(item3);
                t.set("F"+item2);
                context.write(id1,t);
            } else if (fileType == IdentifyFile.File.CircleNet) {

                id1.set(item1);
                t.set("C"+item2);
                context.write(id1,t);
            }

        }
    }

    private static class job1Combiner
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> hs = new HashSet<>();
            //converting text to string and back to make sure equality works
            for (Text v: values) {
                hs.add(v.toString());
            }
            for (String s:hs){
                context.write(key, new Text(s));
            }
        }
    }

    private static class job1Reducer
            extends Reducer<Text,Text,Text,Text> {
        private Text resultValue = new Text();
        private Text resultKey = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Set<String> hs = new HashSet<>();
            String name = "";
            for (Text v : values){
                // would get the nickname from a stream
                // but we have to add everything to the arraylist anyway
                String s = v.toString();
                String sReturn = s.substring(1);
                if (s.startsWith("F")) {
                    hs.add(sReturn);
                } else {
                    name = sReturn;
                }
            }

            resultValue.set(hs.size() + "");
            resultKey.set(name);
            context.write(resultKey, resultValue);
        }
    }

    public static int job1Run(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task d");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(job1Mapper.class);
        job.setCombinerClass(job1Combiner.class);
        job.setReducerClass(job1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        // job 1
        // - just get activitylog
        // - get id1, id2 pair
        // - reducer gets count and unique count (set size)

        long startTime = System.currentTimeMillis();
        int r = job1Run(inputPath,outputPath);
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println(elapsedTime + " FOR THIS TOTAL task d combiner JOB");

        System.exit(r);
    }
}

