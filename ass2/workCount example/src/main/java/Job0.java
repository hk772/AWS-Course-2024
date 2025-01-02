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
            // check if from out2 or from 3gram
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
            WordPairKey key = new WordPairKey(w1Text, w2Text);
            TextAndCountValue value = new TextAndCountValue(w3Text, matchCountWritable);

            context.write(key , value);
        }
    }


    public static class ReducerClass extends Reducer<WordPairKey, TextAndCountValue,Text,Text> {
        @Override
        public void reduce(WordPairKey key, Iterable<TextAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long> H = new HashMap<>();
            for (TextAndCountValue val : vals) {
                Text w3 = val.getText();
                LongWritable matchCount = val.getMatchCount();
                long matchCountLong = matchCount.get();
                if (!H.containsKey(w3.toString())) {
                    H.put(w3.toString(), matchCountLong);
                }
                else {
                    long oldVal = H.get(w3.toString());
                    H.put(w3.toString(), oldVal + matchCountLong);
                }
            }
            for (Map.Entry<String, Long> entry : H.entrySet()) {
                String w3 = entry.getKey();
                Long matchCountSum = entry.getValue();

                Text newKey = new Text(key.getW1().toString() + " " + key.getW2().toString() + " " + w3);
                Text value = new Text(matchCountSum.toString());
                context.write(newKey, value);
            }
        }
    }

    public static class CombinerClass extends Reducer<WordPairKey, TextAndCountValue, WordPairKey, TextAndCountValue> {
        @Override
        public void reduce(WordPairKey key, Iterable<TextAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long> H = new HashMap<>();

            for (TextAndCountValue val : vals) {
                Text w3 = val.getText();
                LongWritable matchCount = val.getMatchCount();
                long matchCountLong = matchCount.get();
                if (!H.containsKey(w3.toString())) {
                    H.put(w3.toString(), matchCountLong);
                }
                else {
                    long oldVal = H.get(w3.toString());
                    H.put(w3.toString(), oldVal + matchCountLong);
                }
            }
            for (Map.Entry<String, Long> entry : H.entrySet()) {
                String w3 = entry.getKey();
                Long matchCountSum = entry.getValue();
                TextAndCountValue totalValue = new TextAndCountValue(new Text(w3), new LongWritable(matchCountSum));
                context.write(key, totalValue);
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