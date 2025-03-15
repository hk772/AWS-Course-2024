package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Job2Key implements WritableComparable<Job2Key> {
    private Text w1;
    private Text tag;

    public Job2Key() {
    }

    public Job2Key(Text w1, Text tag) {
        this.w1 = w1;
        this.tag = tag;
    }

    public void setW1(Text w1) {
        this.w1 = w1;
    }

    public void setTag(Text tag) {
        this.tag = tag;
    }

    public Text getW1() {
        return w1;
    }

    public Text getTag() {
        return tag;
    }


    @Override
    public int compareTo(Job2Key o) {
        String f1 = w1.toString();
        String f2 = o.w1.toString();
        if (f1.compareTo(f2) != 0)
            return f1.compareTo(f2);
        return tag.compareTo(o.tag);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w1.toString());
        out.writeUTF(tag.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w1 = new Text(in.readUTF());
        tag = new Text(in.readUTF());
    }

    @Override
    public int hashCode() {
        return (w1.toString() + " " + tag.toString()).hashCode();
    }

}


