package org.example;

import org.example.Local.Local;
import org.example.Manager.Manager;
import org.example.Worker.Worker;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class Main {

    public static void downloadPDF(String pdfAddress, String localAddress) {
        try {
            InputStream in = new URL(pdfAddress).openStream();
            Files.copy(in, Paths.get(localAddress), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        downloadPDF("https://yahweh.com/pdf/Booklet_Passover.pdf", "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\downloaded.pdf");
    }
}
