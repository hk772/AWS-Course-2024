
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class HebrewRecordReader extends RecordReader<LongWritable, Text> {

    protected LineRecordReader reader;
    protected LongWritable key;
    protected Text value;


    HebrewRecordReader() {
        reader = new LineRecordReader();
        key = new LongWritable(0);
        value = null;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }


    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            // Ensure key is updated properly as a LongWritable
            key.set(reader.getCurrentKey().get() + 1);
            // Read and convert the value to UTF-8
            String rawValue = reader.getCurrentValue().toString();
            value.set(new String(rawValue.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
            return true;
        } else {
            // Reset key and value when no more records
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

}
