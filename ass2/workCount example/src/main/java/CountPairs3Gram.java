import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class CountPairs3Gram {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, MapWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

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

            Text w1Text = new Text(w1);
            Text w2Text = new Text(w2);
            Text w3Text = new Text(w3);
            IntWritable matchCountWritable = new IntWritable(Integer.parseInt(match_count));

            MapWritable H1 = new MapWritable();
            MapWritable H2 = new MapWritable();

            H1.put(w2Text, matchCountWritable);
            H2.put(w3Text, matchCountWritable);

            context.write(w1Text, H1);
            context.write(w2Text, H2);

        }
    }

    public static class ReducerClass extends Reducer<Text,MapWritable,Text,Writable[]> {

        private Map<String,Integer> HSingles;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // load file and create singles table
            String path = conf.get("job1Out");
            FileSystem fs = FileSystem.get(conf);

            Path dirPath = new Path(path);

            FileStatus[] statusList = fs.listStatus(dirPath);

            // Create a map to hold the processed data
            HSingles = new HashMap<>();

            for (FileStatus status : statusList) {
                // Only process files that start with "part"
                if (!status.isDirectory() && status.getPath().getName().startsWith("part")) {
                    Path filePath = status.getPath();

                    // Open the file and read it
                    FSDataInputStream fsDataInputStream = fs.open(filePath);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));

                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String w = parts[0];
                        String n = parts[1];
                        int count = Integer.parseInt(n);
                        HSingles.put(w, count);
                    }

                    reader.close();
                    fsDataInputStream.close();
                }
            }


        }


        @Override
        public void reduce(Text w, Iterable<MapWritable> Hs, Context context) throws IOException,  InterruptedException {
            MapWritable Hsum = new MapWritable();
            for (MapWritable H : Hs) {
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

            IntWritable count2 = new IntWritable(HSingles.get(w.toString()));
            for (Writable w2: Hsum.keySet()) {
                Text w_next = (Text)(w2);
                IntWritable count23 = ((IntWritable)Hsum.get(w_next));
                IntWritable count3 = new IntWritable(HSingles.get(w_next.toString()));

                for (Map.Entry<String,Integer> entry : HSingles.entrySet()) {
                    Text w0 = new Text(entry.getKey());

                    Writable[] writableArray = new Writable[] { w0, w, w_next, count2, count3, count23 };
                    context.write(w0, writableArray);
                }
            }


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
        conf.set("job1Out", "hdfs://localhost:9000/user/hdoop/output/3gramsOut");
        Job job = Job.getInstance(conf, "CountPairs3Gram");
        job.setJarByClass(CountPairs3Gram.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/3gram.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}