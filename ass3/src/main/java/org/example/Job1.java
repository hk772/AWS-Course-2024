package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class Job1 {
    private static boolean isLocal = true;

    public static final Text Lex_Tag = new Text("Lex");     // Important: Lex_Tag must be less then Pair_Tag in lexical order
    public static final Text Pair_Tag = new Text("Pair");
    public static final Text L_Tag = new Text("L");
    public static final Text F_Tag = new Text("F");


    public static class MapperClass extends Mapper<LongWritable, Text, WordAndTagKey, LongWritable> {
        // add stem
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            String lex = parts[0];

            long count_long = Long.parseLong(parts[1]);
            LongWritable count = new LongWritable(count_long);

            String[] archs = parts[1].split(" ");
            int rootIndex = -1;

            WordAndTagKey key1 = new WordAndTagKey(new Text(lex), Lex_Tag);
            context.write(key1, count);

            WordAndTagKey key3 = new WordAndTagKey(new Text(""), L_Tag);
            context.write(key3, count);

            //find root index
            for (int i=0; i < archs.length; i++) {
                String[] subArchs = archs[i].split("/");
                int headIndex = Integer.parseInt(subArchs[3]);
                if (headIndex == 0) {
                    rootIndex = i+1;
                    break;
                }
            }

            int num_feature = 0;
            for (String arch : archs) {
                String[] subArchs = arch.split("/");
                int headIndex = Integer.parseInt(subArchs[3]);
                if (headIndex == rootIndex) {
                    num_feature++;
                    String feature = subArchs[0] + "-" + subArchs[2];
                    WordAndTagKey key = new WordAndTagKey(new Text(lex + " " + feature), Pair_Tag);
                    context.write(key, count);
                }
            }

            WordAndTagKey key2 = new WordAndTagKey(new Text(""), F_Tag);
            context.write(key2, new LongWritable(num_feature * count_long));
        }
    }


    public static class ReducerClass extends Reducer<WordAndTagKey, LongWritable, Text, Text> {
        private String cur_l = "";
        private long cur_l_count = 0;

        @Override
        public void reduce(WordAndTagKey key, Iterable<LongWritable> vals, Context context) throws IOException, InterruptedException {
            if (key.getTag().equals(Lex_Tag)) {
                this.cur_l = key.getW1().toString();
                for (LongWritable v : vals) {
                    cur_l_count += v.get();
                }
            } else if (key.getTag().equals(Pair_Tag)) {
                long lf_count = 0;
                for (LongWritable v : vals) {
                    lf_count += v.get();
                }
                Text res = new Text(String.valueOf(lf_count) +  " " + String.valueOf(cur_l_count));
                context.write(key.getW1(), res);
            }
        }
    }


    public static class CombinerClass extends Reducer<WordAndTagKey, LongWritable, WordAndTagKey, LongWritable> {
        @Override
        public void reduce(WordAndTagKey key, Iterable<LongWritable> vals, Context context) throws IOException,  InterruptedException {
            long count = 0;
            for (LongWritable v : vals) {
                count += v.get();
            }
            context.write(key, new LongWritable(count));
        }
    }


    public static class PartitionerClass extends Partitioner<WordAndTagKey, LongWritable> {
        @Override
        public int getPartition(WordAndTagKey key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
//        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        if (isLocal) {
//            conf.set("stopwords", "hdfs://localhost:9000/user/hdoop/input/stopwords.txt");
//        }
//        else {
//            conf.set("stopwords", AWSApp.baseURL + "/input/stopwords.txt");
//        }
        Job job = Job.getInstance(conf, "Job1");
        job.setJarByClass(Job1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(WordAndTagKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/ngrams.txt"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out1"));
        }
        else {
            System.out.println("not implemented");
//            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out0/part*"));
//            FileInputFormat.addInputPath(job, new Path(Job1.CalculateC0AppPath + "/part*"));
//            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out1"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
