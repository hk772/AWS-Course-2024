package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;


public class Job1 {
    public static final Text Lex_Tag = new Text("Lex");     // Important: Lex_Tag must be less then Pair_Tag in lexical order
    public static final Text Pair_Tag = new Text("Pair");
    public static final Text L_Tag = new Text("L");
    public static final Text F_Tag = new Text("F");

    public static String FLFolder = "FLFolder";
    public static String baseURL = "hdfs://localhost:9000/user/hdoop";
    public static String FLLocalPath = baseURL + "/output/" + FLFolder + "/";
    public static String FLAWSPath = AWSApp.baseURL + "/output/" + FLFolder;


    public static class MapperClass extends Mapper<LongWritable, Text, Job1Key, LongWritable> {
        Stemmer s = new Stemmer();

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            if (!Character.isLetter(line.toString().charAt(0)))
                return;
            String lex = parts[0];
            System.out.println("lexeme: " + lex);
            lex = s.stemWord(lex); // deactivate stemm

            long count_long = Long.parseLong(parts[2]);
            LongWritable count = new LongWritable(count_long);

            String[] archs = parts[1].split(" ");
            int rootIndex = -1;

            Job1Key key1 = new Job1Key(new Text(lex), Lex_Tag);
            context.write(key1, count);
//            System.out.println("lex_tag key: " + key1.getW1() + " count: " + count_long);

            Job1Key key3 = new Job1Key(new Text(""), L_Tag);
            context.write(key3, count);
//            System.out.println("L_tag key: " + key3.getW1() + " count: " + count_long);

            //find root index
            for (int i=0; i < archs.length; i++) {
                if (!archs[i].isEmpty() &&Character.isLetter(archs[i].charAt(0))) {
                    String[] subArchs = archs[i].split("/");
                    int headIndex;
                    try {
                        headIndex = Integer.parseInt(subArchs[3]);
                    } catch (NumberFormatException e) {
                        return;
                    }
                    if (headIndex == 0) {
                        rootIndex = i + 1;
                        break;
                    }
                }
            }
//            System.out.println("root index for line: " + rootIndex);

            int num_feature = 0;
            int i = 1;
            for (String arch : archs) {
                if (!arch.isEmpty() && Character.isLetter(arch.charAt(0))) {
                    String[] subArchs = arch.split("/");
                    int headIndex;
                    try {
                        headIndex = Integer.parseInt(subArchs[3]);
                    } catch (NumberFormatException e) {
                        return;
                    }
                    if (headIndex == rootIndex) {
                        num_feature++;
                        String word = subArchs[0];
                        word = s.stemWord(word);    // deactivate stemm
                        String feature = word + "-" + subArchs[2];

                        Job1Key key = new Job1Key(new Text(lex + " " + feature), Pair_Tag);
                        context.write(key, count);
                        System.out.println("feature index: " + i);
                        System.out.println("key: " + key.getW1() + " count: " + count_long);
                    }
                }
                i++;
            }

            Job1Key key2 = new Job1Key(new Text(""), F_Tag);
            context.write(key2, new LongWritable(num_feature * count_long));
            System.out.println("F_tag key: " + key2.getTag() + " count: " + num_feature * count_long);
        }
    }


    public static class ReducerClass extends Reducer<Job1Key, LongWritable, Text, Text> {
        private String cur_l = "";
        private long cur_l_count = 0;

        FSDataOutputStream out; // out stream to print word counts to CalculateC0 - it will be used on the next Job

        @Override
        public void setup(Context context) throws IOException {
            Path outputFilePath = new Path(FLLocalPath, "part-" + context.getTaskAttemptID());
            FileSystem fs = FileSystem.get(context.getConfiguration());

            if (!AWSApp.isLocal) {
                outputFilePath = new Path(FLAWSPath, "part-" + context.getTaskAttemptID());
                try {
                    fs = FileSystem.get(new java.net.URI("s3a://" + AWSApp.bucketName), new Configuration());
                } catch (URISyntaxException ignored) {};
            }

            FSDataOutputStream outStream = fs.create(outputFilePath, context);
            out = outStream;
        }

        private void writeData(String word, long count) throws IOException {
            String output = word + " " + count + System.lineSeparator();
            byte[] utf8Bytes = output.getBytes(StandardCharsets.UTF_8);
            out.write(utf8Bytes);
        }

        @Override
        public void cleanup(Context context) throws IOException {
            if (out != null) {
                out.close();  // Ensure the stream is closed to flush and complete file writing
            }
        }

        @Override
        public void reduce(Job1Key key, Iterable<LongWritable> vals, Context context) throws IOException, InterruptedException {
            System.out.println("reduce: key: " + key.getW1() + " tag: " + key.getTag());

            if (key.getTag().equals(Lex_Tag)) {
                this.cur_l = key.getW1().toString();
                cur_l_count = 0;
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
                System.out.println("reducer write: key: " + key.getW1() + " count1: " + lf_count + " count2: " + cur_l_count);
            }
            else {
                long count = 0;
                for (LongWritable v : vals) {
                    count += v.get();
                }

                String k = "F";
                if (key.getTag().equals(L_Tag))
                    k = "L";
                writeData(k, count);
            }
        }
    }


    public static class CombinerClass extends Reducer<Job1Key, LongWritable, Job1Key, LongWritable> {
        @Override
        public void reduce(Job1Key key, Iterable<LongWritable> vals, Context context) throws IOException,  InterruptedException {
            long count = 0;
            for (LongWritable v : vals) {
                count += v.get();
            }
            context.write(key, new LongWritable(count));
            System.out.println("combiner: key: " + key.getW1() + " " + key.getTag() + " count: " + count);
        }
    }


    public static class PartitionerClass extends Partitioner<Job1Key, LongWritable> {
        @Override
        public int getPartition(Job1Key key, LongWritable value, int numPartitions) {
            String[] parts = key.getW1().toString().split(" ");
            String tag = key.getTag().toString();

            int hash = 0;
            if (tag.equals(Pair_Tag.toString())){
                hash = parts[0].hashCode();
            } else if (tag.equals(Lex_Tag.toString())) {
                hash = parts[0].hashCode();
            } else if (tag.equals(L_Tag.toString())) {
                hash = tag.hashCode();
            } else if (tag.equals(F_Tag.toString())) {
                hash = tag.hashCode();
            }

            return (hash & Integer.MAX_VALUE) % numPartitions;
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

        job.setMapOutputKeyClass(Job1Key.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (AWSApp.isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/ngrams.txt"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out1"));
        }
        else {
            if (AWSApp.useCustomNgrams) {
                FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/ngrams.txt"));
                FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out1"));
            }
            else{
                // setting output format
//                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out1"));
//                job.setInputFormatClass(SequenceFileInputFormat.class); // Added to be able to parse the ngrams records correctly

                if (AWSApp.corpusPercentage == AWSApp.Percentage.onePercent) {
                    FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/biarcs.00-of-99.txt"));
                }
                else if (AWSApp.corpusPercentage == AWSApp.Percentage.tenPercent) {
                    for (int i=0; i<10; i++) {
                        FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/biarcs.0" + i + "-of-99.txt"));
                    }
                }
                else if (AWSApp.corpusPercentage == AWSApp.Percentage.fullCorpus) {
                    for (int i=0; i<10; i++) {
                        FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/biarcs.0" + i + "-of-99.txt"));
                    }
                    for (int i=10; i<AWSApp.NUM_CORPUS_FILES; i++) {
                        FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/biarcs." + i + "-of-99.txt"));
                    }
                }
                else {
                    System.out.println("not implemented");
                }
            }

        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
