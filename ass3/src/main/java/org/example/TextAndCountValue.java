package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextAndCountValue implements Writable {
    private Text text;
    private LongWritable matchCount;
    private Text tag;

    public TextAndCountValue() {}

    public TextAndCountValue(Text text, LongWritable matchCount) {
        this.text = text;
        this.matchCount = matchCount;
        this.tag = new Text("");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(text.toString());
        out.writeLong(matchCount.get());
        out.writeUTF(tag.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        text = new Text(in.readUTF());
        matchCount = new LongWritable(in.readLong());
        tag = new Text(in.readUTF());
    }

    public String getTag(){
        return tag.toString();
    }
    public void setTag(String tag){
        this.tag = new Text(tag);
    }

    public Text getText() {
        return text;
    }

    public LongWritable getMatchCount() {
        return matchCount;
    }
}
