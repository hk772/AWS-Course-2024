import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HadoopApp extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopApp(),
                args);
        System.exit(res);
    }

    public static class HebrewInputFormat extends FileInputFormat<LongWritable, Text> {


        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new HebrewRecordReader();
        }

        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            CompressionCodec codec =
                    new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
            return codec == null;
        }

    }



    @Override
    public int run(String[] args) throws Exception {
//        if (args.length < 3) {
//            System.err.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
//            System.exit(-1);
//        }

        String input1Path = "hdfs://localhost:9000/user/hdoop/input/3gram.txt";
        String output1Path ="hdfs://localhost:9000/user/hdoop/output/3gramsOut";
        String input2Path = input1Path;
        String output2Path = "hdfs://localhost:9000/user/hdoop/output/out3";
        String input3Path1 = input1Path;
        String input3Path2 = output2Path + "/part*";
        String output3Path = "hdfs://localhost:9000/user/hdoop/output/outProbs";
        int numReduce = 3;

        Configuration conf = getConf();
        conf.set("stopwords", "hdfs://localhost:9000/user/hdoop/input/stopwords.txt");
        conf.set("job1Out", "hdfs://localhost:9000/user/hdoop/output/3gramsOut");

        Job job1 = new Job(conf, "Job1");
        job1.setJarByClass(WordCount3Gram.class);
        job1.setMapperClass(WordCount3Gram.MapperClass.class);
        job1.setPartitionerClass(WordCount3Gram.PartitionerClass.class);
        job1.setCombinerClass(WordCount3Gram.ReducerClass.class);
        job1.setReducerClass(WordCount3Gram.ReducerClass.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job1, new Path(input1Path));
        FileOutputFormat.setOutputPath(job1, new Path(output1Path));
        job1.setNumReduceTasks(numReduce);

        Job job2 = new Job(conf, "Job2");
        job2.setJarByClass(CountPairs3Gram.class);
        job2.setMapperClass(CountPairs3Gram.MapperClass.class);
        job2.setPartitionerClass(CountPairs3Gram.PartitionerClass.class);
        job2.setCombinerClass(CountPairs3Gram.CombinerClass.class);
        job2.setReducerClass(CountPairs3Gram.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(MapWritable.class);
        FileInputFormat.setInputPaths(job2, new Path(input2Path));
        FileOutputFormat.setOutputPath(job2, new Path(output2Path));
        job2.setNumReduceTasks(numReduce);

        Job job3 = new Job(conf, "Job3");
        job3.setJarByClass(CalcProbs.class);
        job3.setMapperClass(CalcProbs.MapperClass.class);
        job3.setPartitionerClass(CalcProbs.PartitionerClass.class);
        job3.setCombinerClass(CalcProbs.CombinerClass.class);
        job3.setReducerClass(CalcProbs.ReducerClass.class);
        job3.setMapOutputKeyClass(Job3Keys.class);
        job3.setMapOutputValueClass(Job3Val.class);
        job3.setOutputKeyClass(Out3Key.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(CalcProbs.CustomOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(input3Path1));
        FileInputFormat.setInputPaths(job3, new Path(input3Path2));
        CalcProbs.CustomOutputFormat.setOutputPath(job3, new Path(output3Path));
        job3.setNumReduceTasks(numReduce);


        ControlledJob controlledJob1 =  new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 =  new ControlledJob(job2.getConfiguration());
        ControlledJob controlledJob3 =  new ControlledJob(job3.getConfiguration());
        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);

        JobControl jobControl = new JobControl("JobControlDemoGroup");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()){
            Thread.sleep(500);
        }

        jobControl.stop();
        return 0;
    }
}