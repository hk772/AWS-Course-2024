package org.example;

import org.apache.hadoop.io.Text;
import org.example.Stemmer;

public class Main {



    public static void main(String[] args) {
        Stemmer s = new Stemmer();
        System.out.println(s.stemWord("Dogs"));
        System.out.println(s.stemWord("activity"));
        System.out.println(s.stemWord("toys"));
    }
}