import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPairKey implements WritableComparable<WordPairKey> {
    private Text w1;
    private Text w2;

    public WordPairKey() {}

    public WordPairKey(Text w1, Text w2) {
        this.w1 = w1;
        this.w2 = w2;
    }

    public void setW1(Text w1) {this.w1 = w1;}
    public void setW2(Text w2) {this.w2 = w2;}

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    @Override
    public int compareTo(WordPairKey o) {
        String combined = w1.toString() + w2.toString();
        String combined2 = o.w1.toString() + o.w2.toString();

        return combined.compareTo(combined2);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w1.toString());
        out.writeUTF(w2.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w1 = new Text(in.readUTF());
        w2 = new Text(in.readUTF());
    }

    @Override
    public int hashCode(){
        String combined = w1.toString() + w2.toString();
        return combined.hashCode();
    }
}


