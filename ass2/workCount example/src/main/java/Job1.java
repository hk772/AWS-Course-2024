import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Job1 {
    private static boolean isLocal = false;
    private static String tag_swapped = "swapped";

    public static class MapperClass extends Mapper<LongWritable, Text, WordPairKey, TextPairAndCountValue> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            // line : -> "w1 w2 w3  match_count"
            //                      matchcount=count(w1,w2,w3)
            String[] parts = line.toString().split("\t");
            String[] words = parts[0].split(" ");

            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            String match_count = parts[1];

            Text w1Text = new Text(w1);
            Text w2Text = new Text(w2);
            Text w3Text = new Text(w3);
            LongWritable matchCountWritable = new LongWritable(Long.parseLong(match_count));

            WordPairKey key1 = new WordPairKey(w1Text, w2Text);
            TextPairAndCountValue value1 = new TextPairAndCountValue(w3Text, matchCountWritable, new Text(""));
            context.write(key1, value1);

            WordPairKey key2 = new WordPairKey(w2Text, w3Text);
            TextPairAndCountValue value2 = new TextPairAndCountValue(w1Text, matchCountWritable, new Text(""));
            value2.setTag(tag_swapped);
            context.write(key2, value2);
        }
    }


    public static class ReducerClass extends Reducer<WordPairKey,TextPairAndCountValue,Text,Text> {
        @Override
        public void reduce(WordPairKey key, Iterable<TextPairAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            long countPair = 0;
            HashMap<String,TextPairAndCountValue> H = new HashMap<>(); // could also store long+tag instead of this object
            for (TextPairAndCountValue val : vals) {
                LongWritable matchCount = val.getMatchCount();
                countPair += matchCount.get();
                H.put(val.getW().toString(), val); // word 3 should be unique at this step
            }
            for (Map.Entry<String,TextPairAndCountValue> entry : H.entrySet()) {
                String w = entry.getKey();
                TextPairAndCountValue val = entry.getValue();
                if (!val.getTag().equals(tag_swapped)){
                    Text newKey = new Text(key.getW1().toString() + " " + key.getW2().toString() + " " + w);
                    Text value = new Text(val.getMatchCount() + " " + countPair + " 0");
//                    System.out.println("DEBUG: WRITING IN REDUCE: " + newKey.toString() + "\t" + value.toString());
                    context.write(newKey, value);
                }
                else {
                    Text newKey = new Text(w + " " + key.getW1().toString() + " " + key.getW2().toString());
                    Text value = new Text(val.getMatchCount() + " 0 " + countPair);
//                    System.out.println("DEBUG: WRITING IN REDUCE: " + newKey.toString() + "\t" + value.toString());
                    context.write(newKey, value);
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<WordPairKey, TextPairAndCountValue> {
        @Override
        public int getPartition(WordPairKey key, TextPairAndCountValue value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job1");
        job.setJarByClass(Job1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        // No combiner here
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(TextPairAndCountValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out0/part*"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out1"));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out0/part*"));
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out1"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}