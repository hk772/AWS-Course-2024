import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Job3Val implements Writable {
    private Text w3;
    private Text tag;
    private LongWritable matchCount;
    private LongWritable count2;
    private LongWritable count3;
    private LongWritable count23;
    private LongWritable totalCount;

    public Job3Val() {}

    public Job3Val(Text w3, Text tag, LongWritable matchCount) {
        this.w3 = w3;
        this.tag = tag;
        this.matchCount = matchCount;
    }

    public Job3Val(Text w3, Text tag, LongWritable count2, LongWritable count3, LongWritable count23, LongWritable totalCount) {
        this.w3 = w3;
        this.tag = tag;
        this.count2 = count2;
        this.count3 = count3;
        this.count23 = count23;
        this.totalCount = totalCount;
    }



    public boolean is3Gram(){
        return tag.toString().equals("A");
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w3.toString());
        out.writeUTF(tag.toString());
        if (is3Gram()){
            out.writeLong(matchCount.get());
        }
        else{
            out.writeLong(count2.get());
            out.writeLong(count3.get());
            out.writeLong(count23.get());
            out.writeLong(totalCount.get());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w3 = new Text(in.readUTF());
        tag = new Text(in.readUTF());
        if (is3Gram()){
            matchCount = new LongWritable(in.readLong());
        }
        else{
            count2 = new LongWritable(in.readLong());
            count3 = new LongWritable(in.readLong());
            count23 = new LongWritable(in.readLong());
            totalCount = new LongWritable(in.readLong());
        }
    }


    public Text getW3() {
        return w3;
    }

    public Text getTag() {
        return tag;
    }

    public LongWritable getMatchCount() {
        return matchCount;
    }

    public LongWritable getCount2() {
        return count2;
    }

    public LongWritable getCount3() {
        return count3;
    }

    public LongWritable getCount23() {
        return count23;
    }

    public LongWritable getTotalCount() {
        return totalCount;
    }
}
