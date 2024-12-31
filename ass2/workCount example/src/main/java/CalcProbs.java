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

public class CalcProbs {
    private static boolean isLocal = false;

    public static class MapperClass extends Mapper<LongWritable, Text, WordPairKey, Job3Val> {
        private final static IntWritable one = new IntWritable(1);

        private HashSet<String> stopSet;


        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            // load file and create singles table
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

        private boolean is3Gram(String line) {
            String[] parts = line.split("\t");
            return !parts[3].equals("B");
        }


        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
//            System.out.println("DEBUG: CalcProbs s3-3gram line: " + line.toString());
            // check if from out2 or from 3gram
            if (is3Gram(line.toString())) { // Tag "A"
                String[] parts = line.toString().split("\t");
                String[] words = parts[0].split(" ");
                if (words.length < 3 || parts.length < 3) {
                    System.out.println("DEBUG: Malformed 3gram line: " + line.toString());
                    return;
                }
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];

                String match_count = parts[2];

                Text w1Text = new Text(w1);
                Text w2Text = new Text(w2);
                Text w3Text = new Text(w3);
                LongWritable matchCountWritable = new LongWritable(Long.parseLong(match_count));

                if (!stopSet.contains(w1) && !stopSet.contains(w2) && !stopSet.contains(w3)) {
                    context.write(new WordPairKey(w1Text, w2Text), new Job3Val(w3Text, new Text("A"), matchCountWritable));
                }
            }
            else { // Tag "B"
                String[] parts = line.toString().split("\t");
                if (parts.length < 8) {
                    System.out.println("DEBUG: Malformed 3gram line in CalcProb: " + line.toString());
                    return;
                }
                String w1 = parts[0];
                String w2 = parts[1];
                String w3 = parts[2];

                String tag = parts[3];
                String count2 = parts[4];
                String count3 = parts[5];
                String count23 = parts[6];
                String totalCount = parts[7];

                Text w1Text = new Text(w1);
                Text w2Text = new Text(w2);
                Text w3Text = new Text(w3);
                Text tagText = new Text(tag);
                LongWritable count2Writable = new LongWritable(Long.parseLong(count2));
                LongWritable count3Writable = new LongWritable(Long.parseLong(count3));
                LongWritable count23Writable = new LongWritable(Long.parseLong(count23));
                LongWritable totalCountWritable = new LongWritable(Long.parseLong(totalCount));

                // cant contain stop words bc output from job 2
                context.write(new WordPairKey(w1Text, w2Text), new Job3Val(w3Text, tagText, count2Writable, count3Writable, count23Writable, totalCountWritable));
            }

        }
    }


    public static class ReducerClass extends Reducer<WordPairKey,Job3Val,Out3Key,Text> {



        private final int VALUE_LENGTH = 4;
        private final int COUNT2_INDEX = 0;
        private final int COUNT3_INDEX = 1;
        private final int COUNT23_INDEX = 2;
        private final int SUM_TRIPLET = 3;

        private Long C0 = -1L;

        @Override
        public void reduce(WordPairKey key, Iterable<Job3Val> vals, Context context) throws IOException,  InterruptedException {
            long currentCount12 = 0;
            HashMap<String, Long[]> H = new HashMap<>();

            for (Job3Val val : vals) {
                Text w3 = val.getW3();
                Text tag = val.getTag();
                if (val.is3Gram()){
                    LongWritable matchCount = val.getMatchCount();
                    if (!H.containsKey(w3.toString())) {
                        Long[] l  = new Long[VALUE_LENGTH];
                        Arrays.fill(l, 0L);
                        H.put(w3.toString(), l);

                    }
                    H.get(w3.toString())[SUM_TRIPLET] += matchCount.get();
                    currentCount12 += matchCount.get();
                }
                else{
                    LongWritable count2 = val.getCount2();
                    LongWritable count3 = val.getCount3();
                    LongWritable count23 = val.getCount23();
                    if (C0 < 0)
                        C0 = val.getTotalCount().get();
                    if (!H.containsKey(w3.toString())) {
                        Long[] l  = new Long[VALUE_LENGTH];
                        Arrays.fill(l, 0L);
                        H.put(w3.toString(), l);
                    }

                    H.get(w3.toString())[COUNT2_INDEX] = count2.get();
                    H.get(w3.toString())[COUNT3_INDEX] = count3.get();
                    H.get(w3.toString())[COUNT23_INDEX] = count23.get();
                }
            }
            finishCurrent(context, H, key, currentCount12);
        }



        private void finishCurrent(Context context, HashMap<String, Long[]> H, WordPairKey currentw1w2, long currentCount12) throws IOException, InterruptedException {
            for (String w3_str : H.keySet()){
                long C1 = H.get(w3_str)[COUNT2_INDEX];
                long N1 = H.get(w3_str)[COUNT3_INDEX];
                long N2 = H.get(w3_str)[COUNT23_INDEX];
                long N3 = H.get(w3_str)[SUM_TRIPLET];

                long C2 = currentCount12;

                Text w1 = currentw1w2.getW1();
                Text w2 = currentw1w2.getW2();
                Text w3 = new Text(w3_str);

                // calc prob
                double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
                double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);

                double part1 = 0;
                if (C2 != 0)
                    part1 = k3 * ((double) N3 /C2);

                double part2 = 0;
                if (C1 != 0)
                    part2 = (1 - k3) * k2 * ((double) N2 /C1);

                double part3 = 0;
                if (C0 != 0)
                    part3 = (1 - k3) * (1 - k2) * ((double) N1 /C0);

                double prob = part1 + part2 + part3;
//                System.out.println(String.format(
//                        "Debug info: w3_str=%s, C1=%d, N1=%d, N2=%d, N3=%d, C2=%d, w1=%s, w2=%s, w3=%s, k2=%f, k3=%f, prob=%f",
//                        w3_str, C1, N1, N2, N3, C2, w1, w2, w3, k2, k3, prob
//                ));
                context.write(new Out3Key(w1, w2, new DoubleWritable(prob)), w3);

            }

        }

    }

    public static class CombinerClass extends Reducer<WordPairKey,Job3Val, WordPairKey,Job3Val> {
        @Override
        public void reduce(WordPairKey key, Iterable<Job3Val> vals, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long> H = new HashMap<>();
            for (Job3Val val : vals) {
                Text w3 = val.getW3();
                Text tag = val.getTag();
                if (val.is3Gram()){
                    LongWritable matchCount = val.getMatchCount();
                    if (!H.containsKey(w3.toString())) {
                        H.put(w3.toString(), 0L);
                    }
                    long oldVal = H.get(w3.toString());
                    H.put(w3.toString(), oldVal + matchCount.get());
                }
                else {
                    context.write(key, val);
                }
            }

            for (String w3_str : H.keySet()){
                context.write(key, new Job3Val(new Text(w3_str), new Text("A"), new LongWritable(H.get(w3_str))));
//                System.out.println("combiner: out");
            }
        }
    }

    public static class PartitionerClass extends Partitioner<WordPairKey, Job3Val> {
        @Override
        public int getPartition(WordPairKey key, Job3Val value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class CustomOutputFormat extends FileOutputFormat<Out3Key, Text> {

        @Override
        public RecordWriter<Out3Key, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            return new CustomRecordWriter(context);
        }

        public static class CustomRecordWriter extends RecordWriter<Out3Key, Text> {

            private final DataOutputStream out;

            public CustomRecordWriter(TaskAttemptContext context) throws IOException {
                // Initialize writer (this would be to a file in the HDFS output)

                Path outputFilePath = new Path(CustomOutputFormat.getOutputPath(context), "part-" + context.getTaskAttemptID());
                FileSystem fs = FileSystem.get(context.getConfiguration());
                if (!isLocal) {
                    try {
                        fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
                    } catch (URISyntaxException ignored) {};
                }

                FSDataOutputStream outStream = fs.create(outputFilePath, context);
                out = outStream;
//                try {
//                    out.writeBytes("results:\r\n");
//                }
//                catch (Exception ex) {
//                }
            }

            @Override
            public void write(Out3Key key, Text value) throws IOException, InterruptedException {
                // Format the output line as w1\tw2\tw3\tprob

                String w1 = key.getW1().toString();
                String w2 = key.getW2().toString();
                String w3 = value.toString(); // Assuming value is w3
                double prob = key.getProb().get();

//                System.out.println(w1 + "\t" + w2 + "\t" + w3 + "\t" + prob);

                // Format the line with UTF-8 support
                String formattedLine = w1 + "\t" + w2 + "\t" + w3 + "\t" + prob + "\n";
                byte[] utf8Bytes = formattedLine.getBytes(StandardCharsets.UTF_8);

                // Write the bytes
                out.write(utf8Bytes);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                // Close the writer
                out.close();
            }
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
        Job job = Job.getInstance(conf, "CalcProbs");
        job.setJarByClass(CalcProbs.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(Job3Val.class);
        job.setOutputKeyClass(Out3Key.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(CustomOutputFormat.class);
        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/3gram.txt"));
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out3/part*"));
            CustomOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/outProbs"));
        }
        else {
            if (AWSApp.use_demo_3gram) {
                FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/3gram.txt"));
            } else {
                job.setInputFormatClass(SequenceFileInputFormat.class); // Added to be able to parse s3_3gram correctly
                FileInputFormat.addInputPath(job, new Path(AWSApp.s3_3gram));
            }
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out3/part*"));
            CustomOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/outProbs"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}