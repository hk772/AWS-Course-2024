import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class Count23 {
    private static boolean isLocal = false;

    public static class MapperClass extends Mapper<LongWritable, Text, WordPairKey, TextPairAndCountValue> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            // check if from out2 or from 3gram
            String[] parts = line.toString().split("t");
            String[] words = parts[0].split(" ");

            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            String[] counts = parts[1].split(" ");
            String match_count = counts[0];
            String count12 = counts[1];

            Text w1Text = new Text(w1);
            Text w2Text = new Text(w2);
            Text w3Text = new Text(w3);
            LongWritable matchCountWritable = new LongWritable(Long.parseLong(match_count));
            WordPairKey key = new WordPairKey(w2Text, w3Text);
            TextPairAndCountValue value = new TextPairAndCountValue(w1Text, matchCountWritable, new Text(count12));

            context.write(key , value);
        }
    }


    public static class ReducerClass extends Reducer<WordPairKey,TextPairAndCountValue,Text,Text> {
        @Override
        public void reduce(WordPairKey key, Iterable<TextPairAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            long currentCount23 = 0;

            for (TextPairAndCountValue val : vals) {
                Text w1 = val.getW();
                LongWritable matchCount = val.getMatchCount();
                currentCount23 += matchCount.get();
            }
            for (TextPairAndCountValue val : vals) {
                Text newKey = new Text(val.getW() + " " + key.getW1().toString() + " " + key.getW2().toString());
                Text value = new Text(val.getMatchCount() + " " + val.getOther().toString() + " " + currentCount23);
                context.write(newKey, value);
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
        Job job = Job.getInstance(conf, "Count23");
        job.setJarByClass(Count23.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        // No combiner here
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(TextPairAndCountValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/outCount12And3"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/outCount23"));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/outCount12And3/part*"));
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/outCount23"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}