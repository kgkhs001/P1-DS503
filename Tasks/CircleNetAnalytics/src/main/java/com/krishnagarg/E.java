package org.nji;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.*;

import org.nji.utils.IdentifyFile;

/*
Determine which people have favorites. That is, for each CircleNetPage owner, determine
how many total accesses/actions to CircleNetPages they have made (as reported in the
ActivityLog) and how many distinct CircleNetPages they have accessed/interacted with
in total. As for the identifier of each CircleNetPage owner, you don’t have to report name.
IDs are enough.

15265,61480,Viewed: Y62UwIX65fpFTTibX68lfdF,638251
 */
public class E {

    private static class job1Mapper
            extends Mapper<Object, Text, IntWritable, Text>{
        private  IntWritable id1 = new IntWritable();
        Text t = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (IdentifyFile.identify(value.toString()) != IdentifyFile.File.ActivityLog) {
                return;
            }

            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            itr.nextToken(); //throw away row ID
            t.set(itr.nextToken());
            id1.set(Integer.valueOf(itr.nextToken()));
            context.write(id1,t);


        }
    }

    private static class job1Reducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> a = new ArrayList<>();
            for (Text v : values){
                a.add(v.toString());
            }

            result.set(a.size() + "," + (new HashSet<>(a)).size());
            context.write(key, result);
        }
    }

    public static int job1Run(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task e");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(job1Mapper.class);
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
        System.out.println(elapsedTime + " FOR THIS TOTAL TASK e JOB");

        System.exit(r);
    }
}

