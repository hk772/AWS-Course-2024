package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2 {
    public static class MapperClass extends Mapper<LongWritable, Text, WordAndTagKey, Text> {
        Stemmer s = new Stemmer();

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
        String[] parts = line.toString().split("\t");
        if (parts.length == 1) {
            // line from out1

        }

        }
    }
}
