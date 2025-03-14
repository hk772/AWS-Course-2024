package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwoWordsAndFeatureKey implements WritableComparable<TwoWordsAndFeatureKey> {
    private Text w1;
    private Text w2;
    private Text feature;

    public TwoWordsAndFeatureKey() {
    }

    public TwoWordsAndFeatureKey(Text w1, Text w2, Text feature) {
        this.w1 = w1;
        this.w2 = w2;
        this.feature = feature;
    }

    public void setW1(Text w1) {
        this.w1 = w1;
    }

    public void setW2(Text w2) {
        this.w2 = w1;
    }

    public void setFeature(Text feature) {
        this.feature = feature;
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public Text getFeature() {
        return feature;
    }

    public String getW1W2() {
        return w1.toString() + " " + w2.toString();
    }

    @Override
    public int compareTo(TwoWordsAndFeatureKey o) {
        if (w1.compareTo(o.w1) != 0) {
            return w1.compareTo(o.w1);
        }
        if (w2.compareTo(o.w2) != 0) {
            return w2.compareTo(o.w2);
        }
        return feature.compareTo(o.feature);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(w1.toString());
        out.writeUTF(w2.toString());
        out.writeUTF(feature.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        w1 = new Text(in.readUTF());
        w2 = new Text(in.readUTF());
        feature = new Text(in.readUTF());
    }

    @Override
    public int hashCode() {
        return (w1 + " " + w2 + " " + feature).hashCode();
    }

    public static void main(String[] args) {
        TwoWordsAndFeatureKey k1 = new TwoWordsAndFeatureKey(new Text("alligator"), new Text("crocodile"), new Text("f1"));
        TwoWordsAndFeatureKey k2 = new TwoWordsAndFeatureKey(new Text("alligator"), new Text("frog"), new Text("f2"));
        TwoWordsAndFeatureKey k3 = new TwoWordsAndFeatureKey(new Text("alligator"), new Text("lizard"), new Text("f3"));

        System.out.println(k1.compareTo(k2));
        System.out.println(k1.compareTo(k3));
    }
}


