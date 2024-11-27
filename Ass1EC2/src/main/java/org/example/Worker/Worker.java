package org.example.Worker;

import org.example.App;
import org.example.Messages.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;

public class Worker extends Thread {

    BlockingQueue<Message> jobsQ;
    BlockingQueue<Message> jobsDoneQ;
    Operations operations;
    String outAddress;// = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\S3\\worker ";
    boolean terminated = false;
    App aws;
    int workerid;
    String myDirPath;

    public Worker(BlockingQueue<Message> jobsQ, BlockingQueue<Message> jobsDoneQ, App aws, int id) throws IOException {
        this.jobsQ = jobsQ;
        this.jobsDoneQ = jobsDoneQ;
        this.operations = new Operations();
        this.aws = aws;
        this.workerid = id;
        this.myDirPath = System.getProperty("user.dir") + "\\WorkersDir" + this.workerid;
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "\\WorkersDir" + this.workerid));
    }


    private Message execute(String job, int id){
        String[] parts = job.split("\t");
        String op = parts[0];
        String keyName = parts[1];
        String name = keyName.split("\\.")[0];


        //download from s3
        String address = System.getProperty("user.dir") + "\\WorkersDir" + this.workerid + "\\" + keyName;
        this.aws.downloadFromS3(keyName, address);

        String outputAddress = System.getProperty("user.dir") + "\\WorkersDir" + this.workerid + "\\";
        String contentStart = op + "\t" + keyName + "\t";
        String newName = name;

        System.out.println("worker -----------------");
        System.out.println("address: " + address);
        System.out.println("keyName: " + keyName);

        try{
            switch (op) {
                case "ToImage":
                    newName += "ToImage.jpg";
                    operations.performOperation(1, address,  outputAddress + newName);
                    break;
                case "ToHTML":
                    newName += "ToHTML.html";
                    operations.performOperation(2, address, outputAddress  + newName);
                    break;
                case "ToText":
                    newName += "ToText.txt";
                    operations.performOperation(2, address, outputAddress  + newName);
                    break;
            }

            //upload res to s3
            String outKeyName = "DoneLocalId"+id+newName;
            this.aws.uploadFileToS3(outputAddress  + newName, outKeyName);

            //delete local files
            Files.delete(Paths.get(outputAddress  + newName));
            Files.delete(Paths.get(address));

            return new Message(id, contentStart + outKeyName);
        } catch (Exception e){
            return new Message(id, contentStart + e.getMessage());

        }

    }

    @Override
    public void run() {
        while (!this.terminated) {
            try {
                Message msg = jobsQ.take();
                Message msgDone = this.execute(msg.content, msg.localID);
                jobsDoneQ.put(msgDone);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void terminate(){
        this.terminated = true;
    }
}
