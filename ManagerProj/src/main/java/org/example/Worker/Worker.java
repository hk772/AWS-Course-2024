package org.example.Worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.App;
import org.example.Manager.Manager;
import org.example.Messages.Message;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class Worker extends Thread {

    Operations operations;
    boolean terminated = false;
    App aws;
    String myDirPath;
    String jobsQUrl;
    String jobsDoneQUrl;
    String terminateQUrl;

    public Worker() throws IOException {
        this.operations = new Operations();
        this.aws = new App();
        this.myDirPath = System.getProperty("user.dir") + "/WorkersDir";
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "/WorkersDir"));

        this.jobsQUrl = this.aws.getQueueUrl(App.jobQ);
        this.jobsDoneQUrl = this.aws.getQueueUrl(App.jobDoneQ);
        this.terminateQUrl = this.aws.getQueueUrl(App.terminationQ);
    }

    public void downloadPDF(String pdfAddress, String localAddress) {
        try {
            InputStream in = new URL(pdfAddress).openStream();
            Files.copy(in, Paths.get(localAddress), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Message execute(String job, String id){
        String[] parts = job.split("\t");
        String op = parts[0];
        String pdfUrl = parts[1];
        Path p = Paths.get(pdfUrl);
        String name = p.getFileName().toString();

        String outputAddress = System.getProperty("user.dir") + "/WorkersDir" + "/";
        String contentStart = op + "\t" + pdfUrl + "\t";
        String newName = name;

        try{
            //download from web
            String address = System.getProperty("user.dir") + "/WorkersDir" + "/" + name;
            this.downloadPDF(pdfUrl, address);

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
                    operations.performOperation(3, address, outputAddress  + newName);
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
                // TODO handle exception better!!
                Object[] obj = aws.popFromSQS(this.jobsQUrl);
                Message msg = (Message) obj[0];
                if (msg != null) {
                    Message msgDone = this.execute(msg.content, msg.localID);
                    this.aws.pushToSQS(this.jobsDoneQUrl, msgDone);
                    // this is the recovery from dead worker protocol, only if the job done has been posted then we can safely delete the job from jobsQ
                    this.aws.deleteMsgFromSqs((DeleteMessageRequest) obj[1]);
                } else{
                    Thread.sleep(100);
                }
            } catch (InterruptedException | JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            this.terminated = (this.aws.getQueueSize(terminateQUrl) > 0);
        }

        this.aws.terminateMyself();
    }

    public static void main(String[] args) {
        try{
            new Worker().start();
        } catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }
}
