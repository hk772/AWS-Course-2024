package org.example;

import org.example.Local.Local;
import org.example.Manager.Manager;

public class Main {
    public static void main(String[] args) {
        Manager manager = new Manager();
        Local l1 = new Local("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\Local\\input1.txt", "t", manager);
        Local l2 = new Local("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\Local\\input2.txt", "t", manager);
        Local l3 = new Local("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\Local\\input3.txt", "t", manager);

        l1.start();
        l2.start();
        l3.start();
    }
}
