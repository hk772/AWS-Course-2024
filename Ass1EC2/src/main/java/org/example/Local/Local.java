package org.example.Local;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.App;
import org.example.Messages.Message;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;


public class Local extends Thread {
    String id;
    String terminate;
    String url;
    String fileKey;
    String outPath;
    App aws;

    String inputQUrl = null;
    String outputQUrl = null;
    String terminateQ = null;

    int loadFactor;


    public Local(String url, String outPath, int loadFactor, String terminate){
        this.terminate = terminate;
        this.url = url;
        this.aws = new App();
        this.outPath = outPath;
//        String macAddress = getMacAddress();
        long timestamp = System.currentTimeMillis();
        this.id = /*macAddress +*/ "-" + timestamp + "-";
        this.loadFactor = loadFactor;

    }

    public void initManagerIfNotExists() {
        try {
            this.aws.initManagerIfNotExists(this.loadFactor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void uploadInputFile() {
        System.out.println("Uploading input file...");
        this.fileKey = "LocalId" + this.id + "input.txt";

        try {
            this.aws.uploadFileToS3(this.url, this.fileKey);
            System.out.println("Local: uploaded input file");
        } catch (Exception e) {
            System.err.println("Error while copying file: " + e.getMessage());
        }
    }

    public void sendMsgToManager() {
        System.out.println("Sending message to manager ...");
        Message msg = new Message(this.id, this.fileKey);
        try{
            this.aws.pushToSQS(this.inputQUrl, msg);
            System.out.println("Local Message sent to managed successfully!");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void downloadOutputFile(String outFileKey) {
        this.aws.downloadFromS3(outFileKey, this.outPath);
        // the downloaded file is .txt need to make it html document
        Path txtFilePath = Paths.get(this.outPath); // Path to the downloaded .txt file
        Path htmlFilePath = Paths.get(this.outPath.replace(".txt", ".html")); // Target .html file path

        try {
            String content = Files.readString(txtFilePath);
            String htmlContent = "<!DOCTYPE html>\n<html>\n<head>\n<title>Document</title>\n</head>\n<body>\n<pre>"
                    + content +
                    "</pre>\n</body>\n</html>";
            Files.writeString(htmlFilePath, htmlContent);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void run() {
        System.out.println("Local is running, id: " + this.id);
        try {
            this.initManagerIfNotExists();

            this.inputQUrl = this.aws.getQueueUrl(App.inputQ);
            this.outputQUrl = this.aws.getQueueUrl(App.outputQ);
            this.terminateQ = this.aws.getQueueUrl(App.terminationQ);

            this.uploadInputFile();
            this.sendMsgToManager();
        } catch (Exception e) {
            System.err.println(e.getMessage() + "\n restart");
//            this.aws.deleteAllResources();
            System.exit(1);
        }


        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Thread is waiting for user input...");

            while (true) {
                String userInput = scanner.nextLine();
                if (this.terminate.equals(userInput)) {
                    System.out.println("Termination activated!");
                    try {
                        this.aws.pushToSQS(this.terminateQ, new Message(this.id, "TERMINATE"));
                        break;
                    } catch (JsonProcessingException e) {
                        System.out.println("Termination failed: " + e.getMessage());
                    }
                }
            }

            scanner.close();
        });

        Thread messageThread = new Thread(() -> {
            System.out.println("Local Thread " + id + " started and waiting for messages...");

            try {
                while (true) {
                    Object[] obj = aws.popFromSQS(this.outputQUrl);
                    if (obj[0] != null) {
                        Message msg = (Message) obj[0];
                        if (msg.localID.equals(id)) {
                            this.aws.deleteMsgFromSqs((DeleteMessageRequest) obj[1]);
                            this.downloadOutputFile(msg.content);
                        } else {
                            Thread.sleep(100); // Avoid busy waiting
                        }
                    } else {
                        Thread.sleep(100);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted.");
                Thread.currentThread().interrupt();
            }
        });

        messageThread.start();
        inputThread.start();


    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java ArgumentParser <url> <outPath> <loadFactor> [terminate]");
            System.exit(1);
        }

        String url = args[0];
        String outPath = args[1];
        int loadFactor;
        try {
            loadFactor = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Error: loadFactor must be an integer.");
            System.exit(1);
            return;
        }

        String terminate = args.length > 3 ? args[3] : null;

        Local loc = new Local(url, outPath, loadFactor, terminate);
        loc.start();
    }

}
