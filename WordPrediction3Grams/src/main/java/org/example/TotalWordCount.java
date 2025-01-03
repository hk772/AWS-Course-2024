package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class TotalWordCount {
    private static boolean isLocal = false;

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private HashSet<String> stopSet;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            // load file
            String path = conf.get("stopwords");
            FileSystem fs = FileSystem.get(conf);
            if (!isLocal) {
                try {
                    fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
                } catch (URISyntaxException ignored) {};
            }

            Path filePath = new Path(path);

            stopSet = new HashSet<>();

            try (FSDataInputStream fsDataInputStream = fs.open(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    stopSet.add(line);
                }
            }
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            // parse 3grams line
            String[] parts = line.toString().split("\t");
            String[] words = parts[0].split(" ");
            if (words.length < 3 || parts.length < 3) {
                System.out.println("DEBUG: Malformed 3gram line: " + line.toString());
                return;
            }
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            if (stopSet.contains(w1) || stopSet.contains(w2) || stopSet.contains(w3)) {
                return;
            }

            String match_count = parts[2];
            LongWritable matchCountWritable = new LongWritable(Long.parseLong(match_count));

            context.write(new Text("") , matchCountWritable);
            context.write(new Text("") , matchCountWritable);
            context.write(new Text("") , matchCountWritable);
        }
    }


    public static class ReducerClass extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text("C0"), new Text(sum + ""));
        }
    }

    public static class CombinerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text(""), new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        if (isLocal) {
            conf.set("stopwords", "hdfs://localhost:9000/user/hdoop/input/stopwords.txt");
        }
        else {
            conf.set("stopwords", AWSApp.baseURL + "/input/stopwords.txt");
        }
        Job job = Job.getInstance(conf, "Job0");
        job.setJarByClass(Job0.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/3gram.txt"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out0"));
        }
        else {
            if (AWSApp.use_demo_3gram) {
                FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/3gram.txt"));
            } else {
                job.setInputFormatClass(SequenceFileInputFormat.class); // Added to be able to parse s3_3gram correctly
                FileInputFormat.addInputPath(job, new Path(AWSApp.s3_3gram));
            }
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/calcingTotal/out0"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}