package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordAndTagKey implements WritableComparable<WordAndTagKey> {
    private Text w1;
    private Text tag;

    public WordAndTagKey() {
    }

    public WordAndTagKey(Text w1, Text tag) {
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
    public int compareTo(WordAndTagKey o) {
        String l1 = w1.toString();
        String l2 = o.w1.toString();





        if (tag.equals(Job1.L_Tag) || tag.equals(Job1.F_Tag))
            l1 = tag.toString();
        if (o.tag.equals(Job1.L_Tag) || o.tag.equals(Job1.F_Tag))
            l2 = o.tag.toString();

        String f2 = "";
        String f1 = "";

        if (tag.equals(Job1.Pair_Tag)){
            l1 = w1.toString().split(" ")[0];
            f1 = w1.toString().split(" ")[1];
        }
        if (o.tag.equals(Job1.Pair_Tag)){
            l2 = o.w1.toString().split(" ")[0];
            f2 = o.w1.toString().split(" ")[1];
        }

        if (l1.compareTo(l2) == 0){
            if (f1.compareTo(f2) == 0)
                return tag.compareTo(o.tag);    // Lex < Pair
            return f1.compareTo(f2);
        }
        else{
            return l1.compareTo(l2);
        }

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
        String l1 = w1.toString();
        if (tag.equals(Job1.Pair_Tag)){
            l1 = w1.toString().split(" ")[1];
        }
        else if (tag.equals(Job1.L_Tag) || tag.equals(Job1.F_Tag)){
            l1 = tag.toString();
        }
        return l1.hashCode();
    }

    public static void main(String[] args) {
        WordAndTagKey k1 = new WordAndTagKey(new Text("d"), new Text("First"));
        WordAndTagKey k2 = new WordAndTagKey(new Text("a"), new Text("Second"));
        WordAndTagKey k3 = new WordAndTagKey(new Text("g"), new Text("Second"));

        System.out.println(k1.compareTo(k2));
        System.out.println(k1.compareTo(k3));
    }
}


