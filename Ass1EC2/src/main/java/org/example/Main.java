package org.example;

import org.example.Local.Local;
import org.example.Manager.Manager;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        App aws = new App();
        Manager manager = null;
        try {
            manager = new Manager();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Local l1 = new Local("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\Local\\input2.txt",
                "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\S3\\",
                "t", manager);
//        Local l2 = new Local("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\Local\\input2.txt", "t", manager);
        Local l3 = new Local("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\Local\\input3.txt",
                "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\S3\\",
                "t", manager);
        l1.start();
////        l2.start();
        l3.start();
    }
}
