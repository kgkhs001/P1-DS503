package org.nji;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.nji.utils.IdentifyFile;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class taskd_combiner_2 {

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
                t.set("C"+item2);
                context.write(id1,t);
            } else if (fileType == IdentifyFile.File.CircleNet) {

                id1.set(item1);
                t.set("F"+item2);
                context.write(id1,t);
            }

        }
    }

    private static class job1Combiner_hs
            extends Reducer<Text,Text,Text,Text> {



        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> hs = new HashSet<>();
            //converting text to string and back to make sure equality works
            for (Text v: values) {
                hs.add(v.toString());
            }
            context.write(key, new Text(String.join(",", hs)));

        }
    }

    private static class job1Combiner_a
            extends Reducer<Text,Text,Text,Text> {



        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> al = new ArrayList<>();
            //converting text to string and back to make sure equality works
            for (Text v: values) {
                al.add(v.toString());
            }
            context.write(key, new Text(String.join(",", al)));

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
                String s_list = v.toString();
                StringTokenizer st = new StringTokenizer(s_list, ",");
                while (st.hasMoreTokens()){
                    String s = st.nextToken();
                    String sReturn = s.substring(1);
                    if (s.startsWith("F")) {
                        hs.add(sReturn);
                    } else {
                        name = sReturn;
                    }
                }
            }

            resultValue.set(hs.size() + "");
            resultKey.set(name);
            context.write(resultKey, resultValue);
        }
    }

    public static int job1Run(Path inputPath, Path outputPath, boolean useHs) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task d");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(job1Mapper.class);
        if (useHs) {
            job.setCombinerClass(job1Combiner_hs.class);
        } else {
            job.setCombinerClass(job1Combiner_a.class);
        }
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
        boolean useHS = false;
        if (args[2]=="hs"){
            useHS=true;
        }
        // job 1
        // - just get activitylog
        // - get id1, id2 pair
        // - reducer gets count and unique count (set size)

        long startTime = System.currentTimeMillis();
        int r = job1Run(inputPath,outputPath, useHS);
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println(elapsedTime + " FOR THIS TOTAL task d combiner JOB");

        System.exit(r);
    }
}

