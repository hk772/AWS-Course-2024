import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FinalMapVal implements Writable {
    private Text w3;
    private LongWritable matchCount;
    private LongWritable count12;
    private LongWritable count23;
    private LongWritable count2;
    private LongWritable count3;


    public FinalMapVal() {}

    public FinalMapVal(Text w3, LongWritable matchCount, LongWritable count12, LongWritable count23, LongWritable count2, LongWritable count3) {
        this.w3 = w3;
        this.matchCount = matchCount;
        this.count12 = count12;
        this.count23 = count23;
        this.count2 = count2;
        this.count3 = count3;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        w3.write(out);
        out.writeLong(matchCount.get());
        out.writeLong(count12.get());
        out.writeLong(count23.get());
        out.writeLong(count2.get());
        out.writeLong(count3.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w3 = new Text();
        matchCount = new LongWritable(in.readLong());
        count12 = new LongWritable(in.readLong());
        count23 = new LongWritable(in.readLong());
        count2 = new LongWritable(in.readLong());
        count3 = new LongWritable(in.readLong());
    }

    public String getW3() {
        return w3.toString();
    }

    public long getMatchCount() {
        return matchCount.get();
    }

    public long getCount12() {
        return count12.get();
    }

    public long getCount23() {
        return count23.get();
    }

    public long getCount2() {
        return count2.get();
    }

    public long getCount3() {
        return count3.get();
    }
}
