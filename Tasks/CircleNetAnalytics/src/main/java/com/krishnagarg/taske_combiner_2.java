package com.krishnagarg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.krishnagarg.utils.IdentifyFile;

import java.io.IOException;
import java.util.*;

public class taske_combiner_2 {

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
            String id1 = itr.nextToken();
            id2.set(Integer.valueOf(itr.nextToken()));
            context.write(id2,new Text(id1 ));

        }
    }

    private static class job1Combiner
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Iterator<Text> i = values.iterator();

            String sReturn=i.next().toString();
            while (i.hasNext()){
                sReturn += "," + i.next().toString();
            }
            context.write(key, new Text(sReturn));

        }
    }

    private static class job1Reducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {


        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Iterator<Text> i = values.iterator();
            Set<String> hs = new HashSet<>();
            List<String> al = new ArrayList<>();

            while (i.hasNext()){
                StringTokenizer st = new StringTokenizer(i.next().toString(),",");
                while (st.hasMoreTokens()){
                    String sNext = st.nextToken();
                    hs.add(sNext);
                    al.add(sNext);
                }
            }

            context.write(key, new Text(hs.size() + " " + al.size()));

        }
    }

    public static int job1Run(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task e");
        job.setJarByClass(taske_combiner_2.class);
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
        System.out.println(elapsedTime + " FOR THIS TOTAL task e JOB ");

        System.exit(r);
    }
}

