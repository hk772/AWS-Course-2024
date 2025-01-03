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


public class Job0 {
    private static boolean isLocal = false;
    private static String SwappedTag = "swappedTag";
    private static int reg_index = 0;
    private static int swapped_index = 1;

    public static String CalculateC0Folder = "CalculateC0";
    private static String baseURL = "hdfs://localhost:9000/user/hdoop";
    public static String CalculateC0LocalPath = baseURL + "/output/" + CalculateC0Folder;
    public static String CalculateC0AppPath = AWSApp.baseURL + "/output/" + CalculateC0Folder;


    public static class MapperClass extends Mapper<LongWritable, Text, WordPairKey, TextAndCountValue> {
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

            Text w1Text = new Text(w1);
            Text w2Text = new Text(w2);
            Text w3Text = new Text(w3);
            LongWritable matchCountWritable = new LongWritable(Long.parseLong(match_count));

            WordPairKey key1 = new WordPairKey(w1Text, w2Text);
            TextAndCountValue value1 = new TextAndCountValue(w3Text, matchCountWritable);

            WordPairKey key2 = new WordPairKey(w2Text, w3Text);
            TextAndCountValue value2 = new TextAndCountValue(w1Text, matchCountWritable);
            value2.setTag(SwappedTag);

            context.write(key1 , value1);
            context.write(key2 , value2);
        }
    }


    public static class ReducerClass extends Reducer<WordPairKey, TextAndCountValue,Text,Text> {
        FSDataOutputStream out; // out stream to print word counts to CalculateC0 - it will be used on the next Job

        @Override
        public void setup(Context context) throws IOException {
            Path outputFilePath = new Path(CalculateC0LocalPath, "part-" + context.getTaskAttemptID());
            FileSystem fs = FileSystem.get(context.getConfiguration());

            if (!isLocal) {
                outputFilePath = new Path(CalculateC0AppPath, "part-" + context.getTaskAttemptID());
                try {
                    fs = FileSystem.get(new java.net.URI("s3a://" + AWSApp.bucketName), new Configuration());
                } catch (URISyntaxException ignored) {};
            }

            FSDataOutputStream outStream = fs.create(outputFilePath, context);
            out = outStream;
        }

        @Override
        public void reduce(WordPairKey key, Iterable<TextAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long[]> H = new HashMap<>();        // Long[] -> [count, had 3gram with swapped?, had 3gram not swapped?]
            long count12 = 0L;
            long count1 = 0L;
            long count2 = 0L;
            for (TextAndCountValue val : vals) {
                String word = val.getText().toString();
                LongWritable matchCount = val.getMatchCount();
                long matchCountLong = matchCount.get();

                count12 += matchCountLong;
                // to not count single middle word twice!
                if (!val.getTag().equals(SwappedTag)) {
                    count1 += matchCountLong;
                    count2 += matchCountLong;
                }
                else{
                    count2 += matchCountLong;
                }

                if (!H.containsKey(word)) {
                    H.put(word, new Long[]{0L,0L});
                }

                Long[] arr = H.get(word);
                if (val.getTag().equals(SwappedTag))
                    arr[swapped_index] += matchCountLong;
                else
                    arr[reg_index] += matchCountLong;
                H.put(word, arr);

            }
            writeSingles(key.getW1(), count1);
            writeSingles(key.getW2(), count2);


            for (String word : H.keySet()) {
                Long[] arr = H.get(word);
                if(arr[swapped_index] > 0) {
                    Text newKey = new Text(word + " " + key.getW1().toString() + " " + key.getW2().toString());
                    Text newVal = new Text(arr[swapped_index] + " 0 " + count12);
                    context.write(newKey , newVal);
                }
                if (arr[reg_index] > 0) {
                    Text newKey = new Text(key.getW1().toString() + " " + key.getW2().toString() + " " + word);
                    Text newVal = new Text(arr[reg_index] + " " + count12 + " 0");
                    context.write(newKey , newVal);
                }

            }
        }

        private void writeSingles(Text word, long matchCount) throws IOException {
            String output = word.toString() + " " + matchCount + System.lineSeparator();
            byte[] utf8Bytes = output.getBytes(StandardCharsets.UTF_8);
            out.write(utf8Bytes);
        }

        @Override
        public void cleanup(Context context) throws IOException {
            if (out != null) {
                out.close();  // Ensure the stream is closed to flush and complete file writing
            }
        }
    }

    public static class CombinerClass extends Reducer<WordPairKey, TextAndCountValue, WordPairKey, TextAndCountValue> {
        @Override
        public void reduce(WordPairKey key, Iterable<TextAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long[]> H = new HashMap<>();        // Long[] -> [count, had 3gram with swapped?, had 3gram not swapped?]
            for (TextAndCountValue val : vals) {
                Text word = val.getText();
                LongWritable matchCount = val.getMatchCount();
                long matchCountLong = matchCount.get();

                if (!H.containsKey(word.toString())) {
                    H.put(word.toString(), new Long[]{0L,0L});
                }
                Long[] arr = H.get(word.toString());
                if (val.getTag().equals(SwappedTag))
                    arr[swapped_index] += matchCountLong;
                else
                    arr[reg_index] += matchCountLong;
                H.put(word.toString(), arr);
            }
            for (String word : H.keySet()) {
                Long[] arr = H.get(word);
                if (arr[swapped_index] > 0) {
                    TextAndCountValue newVal = new TextAndCountValue(new Text(word), new LongWritable(arr[swapped_index]));
                    newVal.setTag(SwappedTag);
                    context.write(key, newVal);
                }
                if (arr[reg_index] > 0) {
                    TextAndCountValue newVal = new TextAndCountValue(new Text(word), new LongWritable(arr[reg_index]));
                    context.write(key, newVal);
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<WordPairKey, TextAndCountValue> {
        @Override
        public int getPartition(WordPairKey key, TextAndCountValue value, int numPartitions) {
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

        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(TextAndCountValue.class);
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
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out0"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}