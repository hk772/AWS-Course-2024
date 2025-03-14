package org.example;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

public class local {


    public static void main(String[] args) {
        Stemmer s = new Stemmer();
        HashMap<String, String> hashMap = new HashMap<>();


        String filePath = "others/word-relatedness.txt";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String w1 = line.split("\t")[0];
                w1 = s.stemWord(w1); // remove this line to disable stemmer
                String w2 = line.split("\t")[1];
                w2 = s.stemWord(w2); // remove this line to disable stemmer
                String b = line.split("\t")[2];

                if (!hashMap.containsKey(w1)) {
                    hashMap.put(w1+"-"+w2, b);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }




        String folderPath = "outputs/out-100percent/out3";
        String outputFilePath = "outputs/out-100percent/combined.csv";

        File folder = new File(folderPath);
        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("Invalid folder path.");
            return;
        }

        File[] files = folder.listFiles(File::isFile);
        if (files == null || files.length == 0) {
            System.out.println("No .txt files found in the folder.");
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            boolean first = true;
            for (File file : files) {
                int i = 0;
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        if(first) {
                            line = line + ",result";
                            writer.write(line);
                            writer.newLine();
                            first = false;
                        } else if (i > 0) {
                            String[] parts = line.split(",");
                            String b = hashMap.get(parts[0] + "-" + parts[1]);
                            String newLine = line + "," + b;
                            writer.write(newLine);
                            writer.newLine();
                        }
                        i ++;
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + file.getName());
                    e.printStackTrace();
                }
                writer.newLine(); // Separate file contents
            }
            System.out.println("Merged content saved to: " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Error writing to output file.");
            e.printStackTrace();
        }
    }
}

