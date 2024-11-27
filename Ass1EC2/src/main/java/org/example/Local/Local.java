package org.example.Local;

import org.example.App;
import org.example.Messages.Message;
import org.example.Manager.Manager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Local extends Thread {
    BlockingQueue<Message> toManager;
    BlockingQueue<Message> fromManager;
    int id;
    Manager manager;
    String terminate;
    String url;
    String fileKey;
    String outPath;
    App aws;


    public Local(String url, String outPath, String terminate, App aws){
        this.toManager = new LinkedBlockingQueue<>();
        this.terminate = terminate;
        this.url = url;
        this.aws = aws;
        this.outPath = outPath;
    }

    public Local(String url, String outPath, String terminate, Manager manager, App aws){
        this.toManager = new LinkedBlockingQueue<>();
        this.terminate = terminate;
        this.url = url;
        this.manager = manager;
        this.aws = aws;
        this.outPath = outPath;
    }

    public void initManagerIfNotExists() {
//        if (this.manager == null) {
//            manager = new Manager();
//        }
        Object[] arr = manager.signIn();
        this.id = (int)arr[0];
        this.toManager = (BlockingQueue<Message>) arr[1];
        this.fromManager = (BlockingQueue) arr[2];
    }

    public void uploadInputFile() {
        Path source = Paths.get(this.url);
        String name = source.getFileName().toString();
        this.fileKey = "LocalId" + this.id + "input.txt";

        try {
            this.aws.uploadFileToS3(this.url, this.fileKey);
            System.out.println("File copied successfully!");
        } catch (Exception e) {
            System.err.println("Error while copying file: " + e.getMessage());
        }
    }

    public void sendMsgToManager() {
        Message msg = new Message(this.id, this.fileKey);
        try{
            this.toManager.put(msg);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void downloadOutputFile(String outFileKey) {
        this.aws.downloadFromS3(outFileKey, this.outPath + outFileKey);
    }

    public void sendTerminateSignal() {}

    public void run() {
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
            System.out.println("Thread " + id + " started and waiting for messages...");

            try {
                while (true) {
                    Message message = fromManager.take(); // Wait for a message
                    if (message.localID == id) {
                        this.downloadOutputFile(message.content);
                    } else {
                        // Put the message back if it doesn't match
                        fromManager.put(message);
                        Thread.sleep(100); // Avoid busy waiting
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
