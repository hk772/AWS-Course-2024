import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

public class WordCount3Gram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private HashSet<String> stopSet;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            // load file and create singles table
            String path = conf.get("stopwords");
            FileSystem fs = FileSystem.get(conf);

            Path filePath = new Path(path);

            stopSet = new HashSet<>();

            try (FSDataInputStream fsDataInputStream = fs.open(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    stopSet.add(line);
//                    System.out.println(Arrays.toString(line.getBytes(StandardCharsets.UTF_8)));
                }
            }


        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            String gram = parts[0];
            String[] words = gram.split(" ");
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            String year = parts[1];
            String match_count = parts[2];

            int count = Integer.parseInt(match_count);
            this.emitIfNotStopWord(w1, count, context);
            this.emitIfNotStopWord(w2, count, context);
            this.emitIfNotStopWord(w3, count, context);
        }

        private void emitIfNotStopWord(String word, int count, Context context) throws IOException,  InterruptedException {
            if (!stopSet.contains(word)) {
                context.write(new Text(word), new IntWritable(count));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.set("stopwords", "hdfs://localhost:9000/user/hdoop/input/stopwords.txt");
        Job job = Job.getInstance(conf, "Word Count 3Gram");
        job.setJarByClass(WordCount3Gram.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/3gram.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/3gramsOut"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
