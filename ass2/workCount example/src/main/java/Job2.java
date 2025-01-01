import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class Job2 {
    private static boolean isLocal = true;
    public static String CalculateC0Folder = "CalculateC0";
    private static String baseURL = "hdfs://localhost:9000/user/hdoop";
    public static String CalculateC0LocalPath = baseURL + "/" + CalculateC0Folder;
    public static String CalculateC0AppPath = AWSApp.baseURL + "/" + CalculateC0Folder;

    private static String tagWord1 = "One";
    private static String tagWord2 = "Two";
    private static String tagWord3 = "Three";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, TextAndCountValue> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            String gram = parts[0];
            String[] words = gram.split(" ");
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            String[] counts = parts[1].split(" ");
            String match_count = counts[0];
            String count12 = counts[1];
            String count23 = counts[2];

            long count = Long.parseLong(match_count);
            TextAndCountValue value1 = new TextAndCountValue(line, new LongWritable(count));
            value1.setTag(tagWord1);
            TextAndCountValue value2 = new TextAndCountValue(line, new LongWritable(count));
            value2.setTag(tagWord2);
            TextAndCountValue value3 = new TextAndCountValue(line, new LongWritable(count));
            value3.setTag(tagWord3);

            context.write(new Text(w1), value1);
            context.write(new Text(w2), value2);
            context.write(new Text(w3), value3);
        }
    }

    public static class ReducerClass extends Reducer<Text,TextAndCountValue,Text,Text> {
        FSDataOutputStream out; // out stream to print word counts to CalculateC0 - it will be used on the next Job

        @Override
        public void setup(Context context) throws IOException {
            Path outputFilePath = new Path(CalculateC0LocalPath, "part-" + context.getTaskAttemptID());
            FileSystem fs = FileSystem.get(context.getConfiguration());

            if (!isLocal) {
                outputFilePath = new Path(CalculateC0AppPath, "part-" + context.getTaskAttemptID());
                try {
                    fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
                } catch (URISyntaxException ignored) {};
            }

            FSDataOutputStream outStream = fs.create(outputFilePath, context);
            out = outStream;
        }

        @Override
        public void reduce(Text key, Iterable<TextAndCountValue> values, Context context) throws IOException,  InterruptedException {
            // TODO : reduce with Hmap from multiple same 3gram emits to 1
            long sum = 0;
            for (TextAndCountValue value : values) {
                sum += value.getMatchCount().get();
            }
            // from previous job emited each 3gram twice => devide by 2
            sum = sum / 2;
            for (TextAndCountValue value : values) {
                // Also write to a side file that enters the Job that calculates C0 - CalculateC0 (CountTotalWords)
                writeCalculateC0Line(key.toString() + " " + sum);
                if (value.getTag().equals(tagWord1)) {
                    return;
                }

                String[] parts = value.getText().toString().split("\t");
                String gram = parts[0];
                String[] words = gram.split(" ");
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];

                String[] counts = parts[1].split(" ");
                String match_count = counts[0];
                String count12 = counts[1];
                String count23 = counts[2];

                Text newKey = new Text(w1 + " " + w2 + " " + w3);
                Text newValue = new Text("");
                if (value.getTag().equals(tagWord2)){
                    newValue = new Text(match_count + " " + count12 + " " + count23 + " " + sum + " 0");
                }
                else if (value.getTag().equals(tagWord3)){
                    newValue = new Text(match_count + " " + count12 + " " + count23 + " 0 " + sum);
                }
                context.write(newKey, newValue);
            }
        }

        private void writeCalculateC0Line(String line) throws IOException {
            byte[] utf8Bytes = line.getBytes(StandardCharsets.UTF_8);
            out.write(utf8Bytes);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, TextAndCountValue> {
        @Override
        public int getPartition(Text key, TextAndCountValue value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountWords123");
        job.setJarByClass(CountWords123.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        // No combiner here
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextAndCountValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out1/part*"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out2"));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out1/part*"));
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out2"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
