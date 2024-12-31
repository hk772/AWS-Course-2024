import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPairAndCountValue implements Writable {
    private Text w;
    private LongWritable matchCount;
    private Text other;

    public TextPairAndCountValue() {}

    public TextPairAndCountValue(Text w, LongWritable matchCount, Text other) {
        this.w = w;
        this.matchCount = matchCount;
        this.other = other;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w.toString());
        out.writeLong(matchCount.get());
        out.writeUTF(other.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w = new Text(in.readUTF());
        matchCount = new LongWritable(in.readLong());
        other = new Text(in.readUTF());
    }


    public Text getW() {
        return w;
    }

    public LongWritable getMatchCount() {
        return matchCount;
    }

    public Text getOther() {
        return other;
    }
}
