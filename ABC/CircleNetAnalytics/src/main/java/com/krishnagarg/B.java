// Idea
/*
    Map -> Count the number of instances of each showing for each page. So pageID = User so we are getting a count and then returning (Id, NickName, and JobTitle) for the output. 
*/

package com.krishnagarg;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

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

public class B {
// Job 1 count the number of visits per page
    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pageID = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String id = value.toString().split(",")[2];
            pageID.set(id);
            context.write(pageID, one);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

// Job 2 rank them. The input should be <key, value> = <pageID, count>
// The output should be a ranked list of pageIDs with the counts
    public static class TopUsersMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> pageInfo = new HashMap<>();
        class MetaData {
            String pageID;
            String nickName;
            String jobTitle;

            MetaData(String pageID, String nickName, String jobTitle) {
                this.pageID = pageID;
                this.nickName = nickName;
                this.jobTitle = jobTitle;
            }
        }

        class User {
            MetaData info;
            int visits;

            User(MetaData info, int visits) {
                this.info = info;
                this.visits = visits;
            }
        }

        private PriorityQueue<User> top10;

        @Override
        protected void setup(Context context) throws IOException {
            // Min-heap by visits
            top10 = new PriorityQueue<>(
                (a, b) -> Integer.compare(a.visits, b.visits)
            );

            // Load CircleNetPage.csv from Distributed Cache
            BufferedReader reader = new BufferedReader(
                new FileReader("circleNetPage.csv")
            );

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 3) continue;

                pageInfo.put(parts[0], parts[1] + "," + parts[2]);
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input: pageID \t count
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return;

            String pageId = parts[0];
            int visits = Integer.parseInt(parts[1]);

            String meta = pageInfo.getOrDefault(pageId, "Unknown,Unknown");
            String[] m = meta.split(",");

            MetaData md = new MetaData(pageId, m[0], m[1]);
            top10.add(new User(md, visits));

            if (top10.size() > 10) {
                top10.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!top10.isEmpty()) {
            User u = top10.poll();
            String outKey =
                u.info.pageID + "," +
                u.info.nickName + "," +
                u.info.jobTitle;

            context.write(new Text(outKey), new IntWritable(u.visits));
        }
        }
    }

    public static class TopUsersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        class User {
            Text key;
            int visits;

            User(Text key, int visits) {
                this.key = key;
                this.visits = visits;
            }
        }

        private PriorityQueue<User> top10;

        @Override
        protected void setup(Context context) {
            top10 = new PriorityQueue<>(
                (a, b) -> Integer.compare(a.visits, b.visits)
            );
        }

        @Override
        public void reduce(Text key,
                        Iterable<IntWritable> values,
                        Context context) {

            // Exactly ONE value per key
            int visits = values.iterator().next().get();

            top10.add(new User(new Text(key), visits));

            if (top10.size() > 10) {
                top10.poll();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            while (!top10.isEmpty()) {
                User u = top10.poll();
                context.write(u.key, new IntWritable(u.visits));
            }
        }
    }

  
  
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path job1Out = new Path("tmp_job1_counts");
        Path job2Out = new Path("tmp_job2_top10");

        // Job 1
        Job job1 = Job.getInstance(conf, "B_CountPages");
        job1.setJarByClass(B.class);

        job1.setMapperClass(CountMapper.class);
        job1.setReducerClass(CountReducer.class);
        job1.setCombinerClass(CountReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, job1Out);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2
        Job job2 = Job.getInstance(conf, "B_TopUsers");
        job2.setJarByClass(B.class);

        job2.setMapperClass(TopUsersMapper.class);
        job2.setReducerClass(TopUsersReducer.class);

        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.addCacheFile(new URI(args[1] + "#circleNetPage.csv"));

        FileInputFormat.addInputPath(job2, job1Out);
        FileOutputFormat.setOutputPath(job2, job2Out);

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        System.exit(0);
    }

}