import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Out4Key implements WritableComparable<Out4Key> {
    private Text w1;
    private Text w2;
    private DoubleWritable prob;

    public Out4Key() {}

    public Out4Key(Text w1, Text w2, DoubleWritable doubleWritable) {
        this.w1 = w1;
        this.w2 = w2;
        this.prob = doubleWritable;
    }


    @Override
    public int compareTo(Out4Key o) {
        String combined = w1.toString() + w2.toString();
        String combined2 = o.w1.toString() + o.w2.toString();

        if (combined.compareTo(combined2) == 0) {
            if (prob.get() < o.prob.get())
                return -1;
            else if (prob.get() > o.prob.get()) {
                return 1;
            }
            return 0;
        }
        return combined.compareTo(combined2);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w1.toString());
        out.writeUTF(w2.toString());
        out.writeDouble(prob.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w1 = new Text(in.readUTF());
        w2 = new Text(in.readUTF());
        prob = new DoubleWritable(in.readDouble());
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public DoubleWritable getProb() {
        return prob;
    }
}
