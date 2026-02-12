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
import java.util.HashSet;
import java.util.Map;

public class H {

    // Helper class to store Page metadata in memory
    private static class UserInfo {
        String nickname;
        int region;
        UserInfo(String nickname, int region) {
            this.nickname = nickname;
            this.region = region;
        }
    }

    public static class SymmetryMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, UserInfo> pageCache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI fileUri : cacheFiles) {
                    Path path = new Path(fileUri.getPath());
                    loadPageData(path, context.getConfiguration());
                }
            }
        }

        private void loadPageData(Path path, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 4) {
                        try {
                            // ID, Name, Job, Region
                            pageCache.put(parts[0].trim(), new UserInfo(parts[1].trim(), Integer.parseInt(parts[3].trim())));
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String id1 = parts[1].trim(); // Follower
                String id2 = parts[2].trim(); // Followed

                UserInfo user1 = pageCache.get(id1);
                UserInfo user2 = pageCache.get(id2);

                // Optimization: Only process if both exist and are in the same region
                if (user1 != null && user2 != null && user1.region == user2.region) {
                    // Send two signals to the reducer
                    context.write(new Text(id1), new Text("OUT:" + id2));
                    context.write(new Text(id2), new Text("IN:" + id1));
                }
            }
        }
    }

    public static class SymmetryReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, UserInfo> pageCache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Re-load cache in Reducer to get nicknames for the final report
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI fileUri : cacheFiles) {
                    Path path = new Path(fileUri.getPath());
                    loadPageData(path, context.getConfiguration());
                }
            }
        }

        private void loadPageData(Path path, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 4) {
                         try {
                            pageCache.put(parts[0].trim(), new UserInfo(parts[1].trim(), Integer.parseInt(parts[3].trim())));
                         } catch (NumberFormatException e) {}
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> peopleIFollow = new HashSet<>();
            HashSet<String> peopleWhoFollowMe = new HashSet<>();

            for (Text val : values) {
                String valStr = val.toString();
                if (valStr.startsWith("OUT:")) {
                    peopleIFollow.add(valStr.substring(4));
                } else if (valStr.startsWith("IN:")) {
                    peopleWhoFollowMe.add(valStr.substring(3));
                }
            }

            // Identify people I follow who are NOT in the 'WhoFollowMe' list
            String myID = key.toString();
            UserInfo myInfo = pageCache.get(myID);
            String myNickname = myInfo != null ? myInfo.nickname : "Unknown";

            for (String followedID : peopleIFollow) {
                if (!peopleWhoFollowMe.contains(followedID)) {
                    // Report the ID and Nickname of the person not being followed back
                    context.write(new Text(myID), new Text(myNickname + " follows ID " + followedID + " (No follow back)"));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // args[0]: Follows
        // args[1]: CircleNetPage (for cache)
        // args[2]: Output

        if (args.length < 3) {
            System.err.println("Usage: H <follows_csv> <pages_file> <output_path>");
            System.exit(-1);
        }

        Path followPath = new Path(args[0]);
        Path pagesPath = new Path(args[1]);
        Path finalOutputPath = new Path(args[2]);

        // Cleanup output (to avoid FileAlreadyExistsException)
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(finalOutputPath)) {
             fs.delete(finalOutputPath, true);
        }

        Job job = Job.getInstance(conf, "Same Region No Follow Back");
        job.setJarByClass(H.class);
        job.addCacheFile(pagesPath.toUri());

        job.setMapperClass(SymmetryMapper.class);
        job.setReducerClass(SymmetryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, followPath);
        FileOutputFormat.setOutputPath(job, finalOutputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}