package org.example;

import org.apache.hadoop.io.Text;

public class Main {

    public static void main(String[] args) {
        Text t = new Text("a");
        Text t2 = new Text("we");
        System.out.println(t.equals(t2));
    }
}