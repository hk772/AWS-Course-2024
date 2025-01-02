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

public class CountWords123 {
    private static boolean isLocal = false;
    public static String CalculateC0Folder = "CalculateC0";
    private static String baseURL = "hdfs://localhost:9000/user/hdoop";
    public static String CalculateC0LocalPath = baseURL + "/output/" + CalculateC0Folder;
    public static String CalculateC0AppPath = AWSApp.baseURL + "/output/" + CalculateC0Folder;

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
            String match_count_sum = counts[0];

            long count = Long.parseLong(match_count_sum);
            TextAndCountValue value = new TextAndCountValue(line, new LongWritable(count));
            context.write(new Text(w1), value);
            context.write(new Text(w2), value);
            context.write(new Text(w3), value);
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
            long sum = 0;
            for (TextAndCountValue value : values) {
                sum += value.getMatchCount().get();
            }
            for (TextAndCountValue value : values) {
                String[] parts = value.getText().toString().split("\t");
                String words = parts[0];
                String[] wordsArray = words.split(" ");
                String w1 = wordsArray[0];
                String w2 = wordsArray[1];
                String w3 = wordsArray[2];

                String rest = parts[1];
                if (key.toString().equals(w2)) {
                    context.write(new Text(words), new Text(rest + " " + sum + " " + 0));
                } else if (key.toString().equals(w3)) {
                    context.write(new Text(words), new Text(rest + " " + 0 + " " + sum));
                }
                // else it equals w1, so we don't need to emit the line to the next stage

                // Also write to a side file that enters the Job that calculates C0 - CalculateC0 (CountTotalWords)
                writeCalculateC0Line(key.toString() + " " + sum);
            }
        }

        private void writeCalculateC0Line(String line) throws IOException {
            line = line + "\n";
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
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/outCount23/part*"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/outCountWords123"));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/outCount23/part*"));
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/outCountWords123"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
