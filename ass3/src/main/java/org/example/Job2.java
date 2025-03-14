package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class Job2 {
    public static final String out_Tag = "B";
    public static final String feature_Tag = "A";

    public static class MapperClass extends Mapper<LongWritable, Text, WordAndTagKey, Text> {
        Stemmer s = new Stemmer();

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            if (!Character.isLetter(line.toString().charAt(0)))
                return;
            if (parts.length == 2) {
                // line from out1
                String feature = parts[0].split(" ")[1];
                WordAndTagKey key = new WordAndTagKey(new Text(feature), new Text(out_Tag));
                context.write(key, line);

            }else{
                // corpus line
                Text count = new Text(parts[2].trim());

                String[] archs = parts[1].split(" ");
                int rootIndex = -1;

                //find root index
                for (int i=0; i < archs.length; i++) {
                    String[] subArchs = archs[i].split("/");
                    int headIndex;
                    try {
                        headIndex = Integer.parseInt(subArchs[3]);
                    } catch (NumberFormatException e) {
                        return;
                    }
                    if (headIndex == 0) {
                        rootIndex = i+1;
                        break;
                    }
                }
                System.out.println("root index for line: " + rootIndex);

                int i = 1;
                for (String arch : archs) {
                    String[] subArchs = arch.split("/");
                    int headIndex;
                    try {
                        headIndex = Integer.parseInt(subArchs[3]);
                    } catch (NumberFormatException e) {
                        return;
                    }
                    if (headIndex == rootIndex) {
                        String word = subArchs[0];
                        word = s.stemWord(word);    // deactivate stemm
                        String feature = word + "-" + subArchs[2];

                        WordAndTagKey key = new WordAndTagKey(new Text(feature), new Text(feature_Tag));
                        context.write(key, count);
                        System.out.println("feature index: " + i);
                        System.out.println("key: " + key.getW1() + " count: " + count);
                    }
                    i++;
                }

            }

        }
    }



    public static class ReducerClass extends Reducer<WordAndTagKey, Text, Text, Text> {
        private long cur_f_count = 0;

        private long F;
        private long L;

        @Override
        public void setup(Reducer.Context context) throws IOException {
            Path FLpath = new Path(Job1.FLLocalPath);
            Configuration conf = context.getConfiguration();

            // load file
            FileSystem fs = FileSystem.get(conf);
            if (!AWSApp.isLocal) {
                FLpath = new Path(Job1.FLAWSPath);
                try {
                    fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
                } catch (URISyntaxException ignored) {};
            }
            try {
                // List the files in the directory
                FileStatus[] fileStatuses = fs.listStatus(FLpath);
                if (fileStatuses.length == 0) {
                    throw new IOException("No files found in the directory: " + FLpath.toString());
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
                                String tag = line.split(" ")[0];
                                long num = Long.parseLong(line.split(" ")[1]);
                                if (num > 0) {
                                    if (tag.equals("F"))
                                        F = num;
                                    else if (tag.equals("L"))
                                        L = num;
                                    if (F > 0 && L > 0)
                                        return; // Exit if a valid F and L are found
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
        public void reduce(WordAndTagKey key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
            System.out.println("reduce: key: " + key.getW1() + " tag: " + key.getTag());

            if (key.getTag().toString().equals(feature_Tag)) {
                cur_f_count = 0;
                for (Text v : vals) {
                    cur_f_count += Long.parseLong(v.toString().trim());
                }
            } else  {
                for (Text line: vals) {
                    // lex feature  count_lf count_l
                    String[] parts = line.toString().split("\t");
                    String lex = parts[0].split(" ")[0];
                    String feature = parts[0].split(" ")[1];
                    long count_lf = Long.parseLong(parts[1].split(" ")[0]);
                    long count_l = Long.parseLong(parts[1].split(" ")[1]);

                    long assoc1 = count_lf;
                    double assoc2 = ((double)count_lf) / count_l;
                    double assoc3 = log2(((double)(count_lf * F))/(count_l * cur_f_count));

                    double t1 = (count_lf * F) - (count_l * cur_f_count);
                    double t2 = L * count_l * cur_f_count * F;
                    t2 = Math.sqrt(t2);
                    double assoc4 = t1 / t2;

                    Text assocs = new Text(assoc1 + " " + assoc2 + " " + assoc3 + " " + assoc4);
                    context.write(new Text(lex + " " + feature), assocs);

                    System.out.println("lex: " + lex + " feature: " + feature);
                    System.out.println("count lf: " + count_lf + " count_l: " + count_l + " count_f " + cur_f_count);

                }
            }
        }
    }

    public static double log2(double x) {
        return Math.log(x) / Math.log(2);
    }




    public static class CombinerClass extends Reducer<WordAndTagKey, Text, WordAndTagKey, Text> {
        @Override
        public void reduce(WordAndTagKey key, Iterable<Text> vals, Context context) throws IOException,  InterruptedException {
            if (key.getTag().toString().equals(feature_Tag)) {
                long cur_f_count = 0;
                for (Text v : vals) {
                    cur_f_count += Long.parseLong(v.toString());
                }
                context.write(key, new Text(cur_f_count + " "));
            }
            else{
                for (Text v : vals) {
                    context.write(key, v);
                }
            }
        }
    }


    public static class PartitionerClass extends Partitioner<WordAndTagKey, Text> {
        @Override
        public int getPartition(WordAndTagKey key, Text value, int numPartitions) {
            return (key.getW1().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
//        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        if (isLocal) {
//            conf.set("stopwords", "hdfs://localhost:9000/user/hdoop/input/stopwords.txt");
//        }
//        else {
//            conf.set("stopwords", AWSApp.baseURL + "/input/stopwords.txt");
//        }
        Job job = Job.getInstance(conf, "Job2");
        job.setJarByClass(Job2.class);
        job.setMapperClass(Job2.MapperClass.class);
        job.setPartitionerClass(Job2.PartitionerClass.class);

        job.setCombinerClass(Job2.CombinerClass.class);
        job.setReducerClass(Job2.ReducerClass.class);

        job.setMapOutputKeyClass(WordAndTagKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (AWSApp.isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/input/ngrams.txt"));
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out1/part*"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out2"));
        } else {
            FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/output/out1/part*"));
            if (AWSApp.useCustomNgrams) {
                FileInputFormat.addInputPath(job, new Path(AWSApp.baseURL + "/input/ngrams.txt"));
                FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out2"));
            } else {
                FileOutputFormat.setOutputPath(job, new Path(AWSApp.baseURL + "/output/out2"));
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
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }

}
