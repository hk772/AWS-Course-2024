package org.example.Local;

import org.example.App;
import org.example.Messages.Message;
import org.example.Manager.Manager;
import org.example.MsgJsonizer;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Local extends Thread {
//    BlockingQueue<Message> toManager;
//    BlockingQueue<Message> fromManager;
    int id;
    Manager manager;
    String terminate;
    String url;
    String fileKey;
    String outPath;
    App aws;

    String inputQUrl;
    String outputQUrl;


    public Local(String url, String outPath, String terminate, App aws){
//        this.toManager = new LinkedBlockingQueue<>();
        this.terminate = terminate;
        this.url = url;
        this.aws = aws;
        this.outPath = outPath;
    }

    public Local(String url, String outPath, String terminate, Manager manager, App aws){
//        this.toManager = new LinkedBlockingQueue<>();
        this.terminate = terminate;
        this.url = url;
        this.manager = manager;
        this.aws = aws;
        this.outPath = outPath;
    }

    public void initManagerIfNotExists() {
        this.id = manager.signIn();
        this.inputQUrl = this.aws.getQueueUrl(App.inputQ);
        this.outputQUrl = this.aws.getQueueUrl(App.outputQ);
    }

    public void uploadInputFile() {
        System.out.println("Uploading input file...");
        Path source = Paths.get(this.url);
        String name = source.getFileName().toString();
        this.fileKey = "LocalId" + this.id + "input.txt";

        try {
            this.aws.uploadFileToS3(this.url, this.fileKey);
            System.out.println("input File uploaded successfully!");
        } catch (Exception e) {
            System.err.println("Error while copying file: " + e.getMessage());
        }
    }

    public void sendMsgToManager() {
        System.out.println("Sending message to manager ...");
        // TODO: try until success?
        Message msg = new Message(this.id, this.fileKey);
        try{
            this.aws.pushToSQS(this.inputQUrl, msg);
            System.out.println("Local Message sent to managed successfully!");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void downloadOutputFile(String outFileKey) {
        this.aws.downloadFromS3(outFileKey, this.outPath + outFileKey);
    }

    public void sendTerminateSignal() {}

    public void run() {
        System.out.println("Local is running");
        this.initManagerIfNotExists();
        this.uploadInputFile();
        this.sendMsgToManager();


        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Thread is waiting for user input...");

            while (true) {
                String userInput = scanner.nextLine();
                if (this.terminate.equalsIgnoreCase(userInput)) {
                    System.out.println("Termination activated!");
                    break;
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
                        if (msg.localID == id) {
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

}
