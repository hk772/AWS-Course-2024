package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job1 {
    private static boolean isLocal = false;
    private static final String SingleTag = "A-SingleLine";
    private static final String GramTag = "B-Gram";
    private static final String GramFirstWordTag = "B-FirstWord";
    private static final String GramSecondWordTag = "B-SecondWord";
    private static final String GramThirdWordTag = "B-ThirdWord";

    public static class MapperClass extends Mapper<LongWritable, Text, WordAndTagKey, TextAndCountValue> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\t");
            String[] words = parts[0].split(" ");
            if (words.length == 2) {
                // line from singles
                Text word = new Text(words[0]);
                long count = Long.parseLong(words[1]);
                WordAndTagKey key = new WordAndTagKey(word, new Text(SingleTag));
                TextAndCountValue val = new TextAndCountValue(new Text(""), new LongWritable(count));
                context.write(key, val);
            } else {
                // line from job0 out => 3gram
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];

                String[] counts = parts[1].split(" ");
                long m_c = Long.parseLong(counts[0]);
                long count12 = Long.parseLong(counts[1]);
                long count23 = Long.parseLong(counts[2]);

                // not emmiting dupolicates
                if (count23 == 0) {
                    WordAndTagKey k1 = new WordAndTagKey(new Text(w1), new Text(GramTag));
                    TextAndCountValue v1 = new TextAndCountValue(new Text(line), new LongWritable(0L));
                    v1.setTag(GramFirstWordTag);
                    context.write(k1, v1);

                    WordAndTagKey k2 = new WordAndTagKey(new Text(w2), new Text(GramTag));
                    TextAndCountValue v2 = new TextAndCountValue(new Text(line), new LongWritable(0L));
                    v2.setTag(GramSecondWordTag);
                    context.write(k2, v2);

                    WordAndTagKey k3 = new WordAndTagKey(new Text(w3), new Text(GramTag));
                    TextAndCountValue v3 = new TextAndCountValue(new Text(line), new LongWritable(0L));
                    v3.setTag(GramThirdWordTag);
                    context.write(k3, v3);
                }
            }
        }
    }


        public static class ReducerClass extends Reducer<WordAndTagKey, TextAndCountValue,Text,Text> {
        private long count = 0L;
        private String curWord = "";


        @Override
        public void reduce(WordAndTagKey key, Iterable<TextAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            if (!curWord.equals(key.getW1().toString())){
                System.out.println("diff: " + curWord + " : " + key.getW1().toString() + " count: " + this.count);
                this.count = 0;
                curWord = key.getW1().toString();
            }

            if (key.getTag().toString().equals(SingleTag)){
                System.out.println("same: " + curWord + " : " + key.getW1().toString()+ " count: " + this.count);
                for (TextAndCountValue v : vals){
                    this.count += v.getMatchCount().get();
                }
            }else if (key.getTag().toString().equals(GramTag)){
                System.out.println("same: " + curWord + " : " + key.getW1().toString()+ " count: " + this.count);

                for (TextAndCountValue v : vals){
                    String[] parts = v.getText().toString().split("\t");

                    Text k = new Text(parts[0]);
                    if (v.getTag().equals(GramSecondWordTag)){
                        Text val = new Text(parts[1] + " " + this.count + " 0");
                        context.write(k, val);
                    }else if (v.getTag().equals(GramThirdWordTag)){
                        Text val = new Text(parts[1] + " 0 " + this.count);
                        context.write(k, val);
                    }
                }
            }

        }
    }

    public static class CombinerClass extends Reducer<WordAndTagKey, TextAndCountValue, WordAndTagKey, TextAndCountValue> {
        @Override
        public void reduce(WordAndTagKey key, Iterable<TextAndCountValue> vals, Context context) throws IOException,  InterruptedException {
            if (key.getTag().toString().equals(SingleTag)){
                long count = 0L;
                for (TextAndCountValue v : vals){
                    count += v.getMatchCount().get();
                }
                TextAndCountValue newVal = new TextAndCountValue(new Text(""), new LongWritable(count));
                context.write(key, newVal);
            }else{
                for (TextAndCountValue v : vals){
                    context.write(key, v);
                }
            }
        }
    }




    public static class PartitionerClass extends Partitioner<WordAndTagKey, TextAndCountValue> {
        @Override
        public int getPartition(WordAndTagKey key, TextAndCountValue value, int numPartitions) {
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
        job.setJarByClass(Job1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(WordAndTagKey.class);
        job.setMapOutputValueClass(TextAndCountValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out0"));
            FileInputFormat.addInputPath(job, new Path(Job0.CalculateC0LocalPath + "/part*"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out1"));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out0/part*"));
            FileInputFormat.addInputPath(job, new Path(Job0.CalculateC0AppPath + "/part*"));
            FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out1"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
