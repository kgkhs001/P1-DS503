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

import org.example.utils.IdentifyFile;

public class taske_combiner {

    private static class job1Mapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private  IntWritable id2 = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (IdentifyFile.identify(value.toString()) != IdentifyFile.File.ActivityLog) {
                return;
            }
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            itr.nextToken(); //throw away row ID
            int id1 = Integer.valueOf(itr.nextToken());
            id2.set(Integer.valueOf(itr.nextToken()));

            context.write(id2,new Text(id1 + " 1"));

        }
    }

    private static class job1Combiner
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Map<Integer, Integer> hm = new HashMap<>();

            Iterator<Text> i = values.iterator();
            while (i.hasNext()){
                StringTokenizer s = new StringTokenizer( i.next().toString());
                int v = Integer.valueOf(s.nextToken());
                int c = Integer.valueOf(s.nextToken());
                hm.put(v, hm.getOrDefault(v, 0)+c);
            }
            for (Map.Entry<Integer, Integer> mr: hm.entrySet()) {

                context.write(key, new Text(mr.getKey() + " " + mr.getValue()));
            }
        }
    }

    private static class job1Reducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {


        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Iterator<Text> i = values.iterator();
            Set<Integer> hs = new HashSet<>();
            int sum = 0;
            while (i.hasNext()){
                StringTokenizer s = new StringTokenizer(i.next().toString(), " ");
                int id = Integer.valueOf(s.nextToken());
                int c = Integer.valueOf(s.nextToken());
                hs.add(id);
                sum+=c;
            }

            context.write(key, new Text(hs.size() + " " + sum));

        }
    }

    public static int job1Run(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task e");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(job1Mapper.class);
        job.setCombinerClass(job1Combiner.class);
        job.setReducerClass(job1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
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
        System.out.println(elapsedTime + " FOR THIS TOTAL JOB");

        System.exit(r);
    }
}

