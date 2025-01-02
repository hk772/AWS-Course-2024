import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class Job4 {
    private static boolean isLocal = false;
    private static String baseURL = "hdfs://localhost:9000/user/hdoop";


    public static class MapperClass extends Mapper<LongWritable, Text, WordPairKey, FinalMapVal> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            String gram = parts[0];
            String[] words = gram.split(" ");
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];

            String[] counts = parts[1].split(" ");
            LongWritable match_count = new LongWritable(Long.parseLong(counts[0]));
            LongWritable count12 = new LongWritable(Long.parseLong(counts[1]));
            LongWritable count23 = new LongWritable(Long.parseLong(counts[2]));
            LongWritable count2 = new LongWritable(Long.parseLong(counts[3]));
            LongWritable count3 = new LongWritable(Long.parseLong(counts[4]));

            WordPairKey key = new WordPairKey(new Text(w1), new Text(w2));
            FinalMapVal val = new FinalMapVal(new Text(w3), match_count, count12, count23, count2, count3);

            context.write(key, val);

        }
    }

    public static class ReducerClass extends Reducer<WordPairKey,FinalMapVal,Out4Key,Text> {
        private long C0;

        @Override
        public void setup(Reducer.Context context) throws IOException {
            Path c0Path = new Path(Job3.C0LocalPath);
            Configuration conf = context.getConfiguration();

            // load file
            FileSystem fs = FileSystem.get(conf);
            if (!isLocal) {
                c0Path = new Path(Job3.C0AppPath);
                try {
                    fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
                } catch (URISyntaxException ignored) {};
            }
            try {
                // List the files in the directory
                FileStatus[] fileStatuses = fs.listStatus(c0Path);
                if (fileStatuses.length == 0) {
                    throw new IOException("No files found in the directory: " + c0Path.toString());
                }

                // Iterate over each file in the directory
                for (FileStatus fileStatus : fileStatuses) {
                    if (!fileStatus.isFile()) {
                        continue; // Skip directories if any are present
                    }
                    Path filePath = fileStatus.getPath();
                    try (FSDataInputStream fsDataInputStream = fs.open(filePath);
                         BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {

                        String line;
                        while ((line = reader.readLine()) != null) {
                            try {
                                C0 = Long.parseLong(line.split("\t")[1]);
                                if (C0 > 0) {
                                    return; // Exit if a valid C0 is found
                                }
                            } catch (NumberFormatException e) {
                                // Ignore lines that can't be parsed
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                // Handle the exception
            }
        }

        @Override
        public void reduce(WordPairKey key, Iterable<FinalMapVal> values, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long[]> H = new HashMap<>();
            for (FinalMapVal val : values) {
                if (!H.containsKey(val.getW3())) {
                    H.put(val.getW3(), new Long[]{val.getMatchCount(), val.getCount12(), val.getCount23(), val.getCount2(), val.getCount3()});
                }
                else {
                    long count2 = val.getCount2();
                    if (count2 != 0) {
                        H.get(val.getW3())[3] = count2;
                    }
                    long count3 = val.getCount3();
                    if (count3 != 0) {
                        H.get(val.getW3())[4] = count3;
                    }
                }
            }

            for (String w3 : H.keySet()) {
                Long[] counts = H.get(w3);

                long N1 = counts[4];
                long N2 = counts[2];
                long N3 = counts[0];
                long C1 = counts[3];
                long C2 = counts[1];

                double k2 = ((Math.log(N2+1) + 1) / (Math.log(N2+1) + 2));
                double k3 = ((Math.log(N3+1) + 1) / (Math.log(N3+1) + 2));

//                double prob = k3 * ((double) N3 / C2) + (1 - k3) * k2 * ((double) N2 / C1) + (1 - k3) * (1 - k2) * ((double) N1 / C0);
                double part1 = 0;
                if (C2 != 0)
                    part1 = k3 * ((double) N3 /C2);

                double part2 = 0;
                if (C1 != 0)
                    part2 = (1 - k3) * k2 * ((double) N2 /C1);

                double part3 = 0;
                if (C0 != 0)
                    part3 = (1 - k3) * (1 - k2) * ((double) N1 /C0);

                double prob = part1 + part2 + part3;

                Out4Key out4Key = new Out4Key(key.getW1(), key.getW2(), new DoubleWritable(prob));
                System.out.println(String.format("DEBUG: Reducer calcs: <N1:%d N2:%d N3:%d C1:%d C2:%d C0:%d k2:%f k3:%f prob:%f>: ",N1,N2,N3,C1,C2,C0,k2,k3,prob));
                System.out.println("DEBUG: Reducer writing: " + out4Key.toString() + " with w3: " + w3);
                context.write(out4Key, new Text(w3));
            }

        }
    }

    public static class CombinerClass extends Reducer<WordPairKey, FinalMapVal, WordPairKey, FinalMapVal> {
        @Override
        public void reduce(WordPairKey key, Iterable<FinalMapVal> values, Context context) throws IOException,  InterruptedException {
            HashMap<String, Long[]> H = new HashMap<>();
            for (FinalMapVal val : values) {
                if (!H.containsKey(val.getW3())) {
                    H.put(val.getW3().toString(), new Long[]{val.getMatchCount(), val.getCount12(), val.getCount23(), val.getCount2(), val.getCount3()});
                }
                else {
                    long count2 = val.getCount2();
                    if (count2 != 0) {
                        H.get(val.getW3())[3] = count2;
                    }
                    long count3 = val.getCount3();
                    if (count3 != 0) {
                        H.get(val.getW3())[4] = count3;
                    }
                }
            }

            for (String w3 : H.keySet()) {
                Long[] counts = H.get(w3);

                LongWritable match_count = new LongWritable(counts[0]);
                LongWritable count12 = new LongWritable(counts[1]);
                LongWritable count23 = new LongWritable(counts[2]);
                LongWritable count2 = new LongWritable(counts[3]);
                LongWritable count3 = new LongWritable(counts[4]);

                FinalMapVal val = new FinalMapVal(new Text(w3), match_count, count12, count23, count2, count3);
                System.out.println("DEBUG: Combiner writing: " + key.toString() + " " + val.toString());
                context.write(key, val);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<WordPairKey, FinalMapVal> {
        @Override
        public int getPartition(WordPairKey key, FinalMapVal value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class CustomOutputFormat extends FileOutputFormat<Out4Key, Text> {

        @Override
        public RecordWriter<Out4Key, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            return new CustomOutputFormat.CustomRecordWriter(context);
        }

        public static class CustomRecordWriter extends RecordWriter<Out4Key, Text> {

            private final DataOutputStream out;

            public CustomRecordWriter(TaskAttemptContext context) throws IOException {
                // Initialize writer (this would be to a file in the HDFS output)

                Path outputFilePath = new Path(CustomOutputFormat.getOutputPath(context), "part-" + context.getTaskAttemptID());
                FileSystem fs = FileSystem.get(context.getConfiguration());
                if (!isLocal) {
                    try {
                        fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
                    } catch (URISyntaxException ignored) {};
                }

                FSDataOutputStream outStream = fs.create(outputFilePath, context);
                out = outStream;
            }

            @Override
            public void write(Out4Key key, Text value) throws IOException, InterruptedException {
                // Format the output line as w1\tw2\tw3\tprob

                String w1 = key.getW1().toString();
                String w2 = key.getW2().toString();
                String w3 = value.toString(); // Assuming value is w3
                double prob = key.getProb().get();

                // Format the line with UTF-8 support
                String formattedLine = w1 + "\t" + w2 + "\t" + w3 + "\t" + prob + "\n";
                byte[] utf8Bytes = formattedLine.getBytes(StandardCharsets.UTF_8);

                // Write the bytes
                out.write(utf8Bytes);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                out.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job4");
        job.setJarByClass(Job4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputFormatClass(CustomOutputFormat.class);
        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(FinalMapVal.class);
        job.setOutputKeyClass(Out4Key.class);
        job.setOutputValueClass(Text.class);


        if (isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out2/part*"));
            CustomOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out4"));
        }
        else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out2/part*"));
            CustomOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out4"));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
