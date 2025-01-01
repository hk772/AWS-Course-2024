import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class Job3 {
    private static boolean isLocal = true;
    private static String baseURL = "hdfs://localhost:9000/user/hdoop";

    public static String C0LocalPath ="hdfs://localhost:9000/user/hdoop/output/C0";
    public static String C0AppPath = AWSApp.baseURL + "/output/C0";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split(" ");
            String w = parts[0];
            String countStr = parts[1];

            LongWritable count = new LongWritable(Long.parseLong(countStr));

            context.write(new Text(""), count);
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,Text> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text("C0"), new Text(sum + ""));
        }
    }

    public static class CombinerClass extends Reducer<Text, LongWritable,Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text(""), new LongWritable(sum));
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
        Job job = Job.getInstance(conf, "Job3");
        job.setJarByClass(Job3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextAndCountValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path(Job2.CalculateC0LocalPath));
            FileOutputFormat.setOutputPath(job, new Path(C0LocalPath));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(Job2.CalculateC0AppPath + "/part*"));
            FileOutputFormat.setOutputPath(job, new Path(C0AppPath));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
