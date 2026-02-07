package org.example;


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
import java.util.regex.Pattern;


import org.example.utils.IdentifyFile;

public class taskd {

    public static class job1Mapper
            extends Mapper<Object, Text, IntWritable, Text>{
        private  IntWritable id1 = new IntWritable();
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
                id1.set(Integer.valueOf(item3));
                t.set(item2);
                context.write(id1,t);
            } else if (fileType == IdentifyFile.File.CircleNet) {
                System.out.println();
                id1.set(Integer.valueOf(item1));
                t.set(item2);
                context.write(id1,t);
            }


        }
    }

    public static class job1Reducer
            extends Reducer<IntWritable,Text,Text,IntWritable> {
        private IntWritable resultValue = new IntWritable();
        private Text resultKey = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Pattern p = Pattern.compile("^\\d*$");
            List<String> a = new ArrayList<>();
            String name = "";
            for (Text v : values){
                // would get the nickname from a stream
                // but we have to add everything to the arraylist anyway
                String s = v.toString();
                if (p.matcher(s).find()) {
                    a.add(s);
                } else {
                    name = s;
                }
            }

            resultValue.set((new HashSet<>(a)).size());
            resultKey.set(name);
            context.write(resultKey, resultValue);
        }
    }

    public static int job1Run(Path inputPath, Path outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
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
        // map
        // match to circle or follow
        // either way get first and second key
        // reducer
        // it's going to be a bunch of numbers and a name
        // stream and collect thing that's not number
        long startTime = System.currentTimeMillis();
        int r = job1Run(inputPath,outputPath);
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println(elapsedTime + " FOR THIS TOTAL TASK d JOB");

        System.exit(r);
    }
}

