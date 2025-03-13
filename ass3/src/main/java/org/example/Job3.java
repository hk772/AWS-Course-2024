package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

public class Job3 {
    private static final String GoldStandardLocalPath = "hdfs://localhost:9000/user/hdoop/input/word-relatedness.txt";

    public static class MapperClass extends Mapper<LongWritable, Text, TwoWordsAndFeatureKey, WordAndTagKey> {
        private Stemmer s = new Stemmer();
        private HashMap<String, HashSet<String>[]> hashMap = new HashMap<>(); // w -> [all words w2 such that w=w1 and <w1,w2>, all words w1 such that w=w2 and <w1,w2>]

        @Override
        public void setup(Mapper.Context context) throws IOException {
            Path filePath = new Path(GoldStandardLocalPath);
            Configuration conf = context.getConfiguration();

            // load file
            FileSystem fs = FileSystem.get(conf);
//            if (!Job1.isLocal) {
//                filePath = new Path(JobC0.C0AppPath);
//                try {
//                    fs = FileSystem.get(new java.net.URI(AWSApp.baseURL), new Configuration());
//                } catch (URISyntaxException ignored) {};
//            }

            try (FSDataInputStream fsDataInputStream = fs.open(filePath);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    String w1 = line.split("\t")[0];
                    w1 = s.stemWord(w1); // remove this line to disable stemmer
                    String w2 = line.split("\t")[1];
                    w2 = s.stemWord(w2); // remove this line to disable stemmer

                    if (!hashMap.containsKey(w1)) {
                        hashMap.put(w1, new HashSet[]{new HashSet<String>(),new HashSet<String>()});
                    }
                    if (!hashMap.containsKey(w2)) {
                        hashMap.put(w2, new HashSet[]{new HashSet<String>(),new HashSet<String>()});
                    }
                    hashMap.get(w1)[0].add(w2);
                    hashMap.get(w2)[1].add(w1);
                }
            }
            hashMap.forEach((key, value) -> System.out.println(key + " " + Arrays.toString(Arrays.stream(value).toArray()))); // print hashMap
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] parts = line.toString().split("\t");
            // line from out2
            String lex = parts[0].split(" ")[0];
            String feature = parts[0].split(" ")[1];
            String assocs = parts[1];
            System.out.println("received in mapper: key: " + line);

            for (String w2 : hashMap.get(lex)[0]) {
                TwoWordsAndFeatureKey k = new TwoWordsAndFeatureKey(new Text(lex), new Text(w2), new Text(feature));
                WordAndTagKey v = new WordAndTagKey(new Text(assocs), new Text("First"));
                System.out.println("sent from mapper: key: " + k.getW1W2() + " " + k.getFeature() + " value:" + v.getW1() + " " + v.getTag());
                context.write(k, v);
            }
            for (String w1 : hashMap.get(lex)[1]) {
                TwoWordsAndFeatureKey k = new TwoWordsAndFeatureKey(new Text(w1), new Text(lex), new Text(feature));
                WordAndTagKey v = new WordAndTagKey(new Text(assocs), new Text("Second"));
                System.out.println("sent from mapper: key: " + k.getW1W2() + " " + k.getFeature() + " value: " + v.getW1() + " " + v.getTag());
                context.write(k, v); // <axe,adapt,f_i> Second 0.5 0.4 0.4 0.5
            }
        }
    }



    public static class ReducerClass extends Reducer<TwoWordsAndFeatureKey, WordAndTagKey, Text, Text> {
        private String currentW1W2 = null;
        private final int SUM_MANHATTAN = 0;
        private final int SUM_EUCLID = 1;
        private final int SUM_MULT = 2;
        private final int SUM_LI1_SQUARED = 3;
        private final int SUM_LI2_SQUARED = 4;
        private final int SUM_MIN = 5;
        private final int SUM_MAX = 6;
        private final int SUM_ADD = 7;
        private final int SUM_JS = 8; // equation (17)
        private final int NUM_SUMS = 9;
        private final int NUM_ASSOC = 4;
        private double[][] sums = new double[NUM_SUMS][NUM_ASSOC];

        private Double[] assoc1 = new Double[NUM_ASSOC];
        private Double[] assoc2 = new Double[NUM_ASSOC];


        @Override
        public void reduce(TwoWordsAndFeatureKey key, Iterable<WordAndTagKey> vals, Context context) throws IOException, InterruptedException {
            WordAndTagKey val1 = new WordAndTagKey(new Text("0 0 0 0"), new Text("First"));
            WordAndTagKey val2 = new WordAndTagKey(new Text("0 0 0 0"), new Text("Second"));
            for (WordAndTagKey val : vals) {
                if (val.getTag().toString().equals("First")) {
                    val1 = val;
                }
                if (val.getTag().toString().equals("Second")) {
                    val2 = val;
                }
            }

            System.out.println("received in reducer: key: " + key.getW1W2() + " " + key.getFeature() + " val1: " + val1.getW1() + " " + val1.getTag() + " val2: " + val2.getW1() + " " + val2.getTag());

            // extract the assoc values for each word
            assoc1 = Arrays.stream(val1.getW1().toString().split(" ")).map(Double::parseDouble).collect(Collectors.toList()).toArray(assoc1);
            assoc2 = Arrays.stream(val2.getW1().toString().split(" ")).map(Double::parseDouble).collect(Collectors.toList()).toArray(assoc2);

            // emit all 24 values when the W1W2 pair swaps
            if (currentW1W2 != null && !key.getW1W2().equals(currentW1W2)) {
                Text k = new Text(currentW1W2);
                Text distances = calc_distances(); // 24 values of all possible distances with all possible assocs
                System.out.println("sent from reducer: key: " + k.toString() + " value: " + distances);
                context.write(k, distances);
                sums = new double[NUM_SUMS][NUM_ASSOC];
            }

            // calc according to val1 and val2, add to summaries
            for (int i=0; i<NUM_ASSOC; i++) {
                sums[SUM_MANHATTAN][i] += Math.abs(assoc1[i] - assoc2[i]);
                sums[SUM_EUCLID][i] += Math.pow(assoc1[i] - assoc2[i],2);
                sums[SUM_MULT][i] += assoc1[i] * assoc2[i];
                sums[SUM_LI1_SQUARED][i] += Math.pow(assoc1[i],2);
                sums[SUM_LI2_SQUARED][i] += Math.pow(assoc2[i],2);
                sums[SUM_MIN][i] += Math.min(assoc1[i], assoc2[i]);
                sums[SUM_MAX][i] += Math.max(assoc1[i], assoc2[i]);
                sums[SUM_ADD][i] += assoc1[i] + assoc2[i]; // need to verify this
                double term1 = ((double)2*assoc1[i]/(assoc1[i]+assoc2[i]));
                term1 = assoc1[i]*Job2.log2(term1);
                double term2 = ((double)2*assoc2[i]/(assoc1[i]+assoc2[i]));
                term2 = assoc2[i]*Job2.log2(term2);
                sums[SUM_JS][i] += term1+term2;
            }

            currentW1W2 = key.getW1W2();
        }

        private Text calc_distances() {
            // use the assoc1 and assoc2 arrays of the last w1-w2 pair?
            String dist_manhattan_string = String.join(" ", Arrays.stream(sums[SUM_MANHATTAN]).mapToObj(String::valueOf).collect(Collectors.toList()));
            String dist_euclid_string = String.join(" ", Arrays.stream(sums[SUM_EUCLID]).mapToObj(x -> String.valueOf(Math.sqrt(x))).collect(Collectors.toList()));
            double[] dist_cos_sim = new double[NUM_ASSOC];
            for (int i=0; i < dist_cos_sim.length; i++) {
                dist_cos_sim[i] = (sums[SUM_MULT][i]) / (Math.sqrt(sums[SUM_LI1_SQUARED][i])*Math.sqrt(sums[SUM_LI2_SQUARED][i]));
            }
            String dist_cos_sim_string = String.join(" ", Arrays.stream(dist_cos_sim).mapToObj(String::valueOf).collect(Collectors.toList()));
            double[] dist_jaccard_sim = new double[NUM_ASSOC];
            for (int i=0; i < dist_jaccard_sim.length; i++) {
                dist_jaccard_sim[i] = (sums[SUM_MIN][i]) / (sums[SUM_MAX][i]);
            }
            String dist_jaccard_sim_string = String.join(" ", Arrays.stream(dist_jaccard_sim).mapToObj(String::valueOf).collect(Collectors.toList()));

            double[] dist_dice_sim = new double[NUM_ASSOC];
            for (int i=0; i < dist_dice_sim.length; i++) {
                dist_dice_sim[i] = 2*(sums[SUM_MIN][i]) / (sums[SUM_ADD][i]);
            }
            String dist_dice_sim_string = String.join(" ", Arrays.stream(dist_dice_sim).mapToObj(String::valueOf).collect(Collectors.toList()));
            String dist_js_string = String.join(" ", Arrays.stream(sums[SUM_JS]).mapToObj(String::valueOf).collect(Collectors.toList()));

            String result = dist_manhattan_string + " " + dist_euclid_string + " " + dist_cos_sim_string + " " + dist_jaccard_sim_string + " " + dist_dice_sim_string + " " + dist_js_string;
            return new Text(result);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            if (currentW1W2 != null) {
                Text k = new Text(currentW1W2);
                Text distances = calc_distances(); // 24 values of all possible distances with all possible assocs
                System.out.println("sent from reducer: key: " + k.toString() + " value: " + distances);
                context.write(k, distances);
                sums = new double[NUM_SUMS][NUM_ASSOC]; // probably not necessary
            }
            else {
                System.out.println("Something wrong happened, cleanup executed before reduce");
            }
        }

    }

//    private static void resetMatrix(double[][] matrix) {
//        for (int i=0; i<matrix.length; i++) {
//            for (int j = 0; j < matrix[0].length; j++) {
//                matrix[i][j] = 0;
//            }
//        }
//    }



//    public static class CombinerClass extends Reducer<WordTripletKey, WordAndTagKey, WordTripletKey, WordAndTagKey> {
//        @Override
//        public void reduce(WordTripletKey key, Iterable<WordAndTagKey> vals, Context context) throws IOException,  InterruptedException {
//            WordAndTagKey first = null;
//            WordAndTagKey second = null;
//            for (WordAndTagKey val : vals) {
//                if (val.getTag().equals("First")) {
//                    first = val;
//                }
//                else if (val.getTag().equals("Second")) {
//                    second = val;
//                }
//                if (first != null && second != null)
//                    break;
//            }
//            if (first != null) {
//                context.write(key,first);
//            }
//            if (second != null) {
//                context.write(key,second);
//            }
//        }
//    }


    public static class PartitionerClass extends Partitioner<TwoWordsAndFeatureKey, WordAndTagKey> {
        @Override
        public int getPartition(TwoWordsAndFeatureKey key, WordAndTagKey value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
//        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
//        if (isLocal) {
//            conf.set("stopwords", "hdfs://localhost:9000/user/hdoop/input/stopwords.txt");
//        }
//        else {
//            conf.set("stopwords", AWSApp.baseURL + "/input/stopwords.txt");
//        }
        Job job = Job.getInstance(conf, "Job3");
        job.setJarByClass(Job3.class);
        job.setMapperClass(Job3.MapperClass.class);
        job.setPartitionerClass(Job3.PartitionerClass.class);

//        job.setCombinerClass(Job3.CombinerClass.class);
        job.setReducerClass(Job3.ReducerClass.class);

        job.setMapOutputKeyClass(TwoWordsAndFeatureKey.class);
        job.setMapOutputValueClass(WordAndTagKey.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (Job1.isLocal) {
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out2/part*"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hdoop/output/out3"));
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
