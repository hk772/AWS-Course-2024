import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountPairs {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, MapWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] StringVal = value.toString().split(" ");
            for (int i = 0; i < StringVal.length - 1; i++) {
                MapWritable H = new MapWritable();
                Text w1 = new Text(StringVal[i]);
                Text w2 = new Text(StringVal[i+1]);
                H.put(w2, one);
                context.write(w1, H);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,MapWritable,Text,MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException,  InterruptedException {
            MapWritable Hsum = new MapWritable();
            for (MapWritable H : values) {
                for (Writable w2 : H.keySet()){
                    int val = ((IntWritable)H.get(w2)).get();

                    if (Hsum.containsKey(w2)) {
                        int oldVal = ((IntWritable)Hsum.get(w2)).get();
                        Hsum.put(w2, new IntWritable(oldVal + val));
                    } else {
                        Hsum.put(w2, new IntWritable(val));
                    }
                }
            }
            context.write(key, Hsum);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, MapWritable> {
        @Override
        public int getPartition(Text key, MapWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Pairs");
        job.setJarByClass(CountPairs.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/words2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out2.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}