package org.example.Manager;


import org.example.App;
import org.example.Messages.Message;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class InputProcessor extends Thread{
    String keyName;
    String url;
    BlockingQueue<Message> jobsQ;
    BlockingQueue<Message> jobsDoneQ;
    Manager manager;
    int localID;
    boolean terminate = false;
    App aws;

    public InputProcessor(Manager manager, Message msg, BlockingQueue<Message> jobsQ, BlockingQueue<Message> jobsDoneQ, App aws) {
        this.keyName = msg.content;
        this.localID = msg.localID;
        this.jobsQ = jobsQ;
        this.jobsDoneQ = jobsDoneQ;
        this.manager = manager;
        this.aws = aws;
        this.url = System.getProperty("user.dir") +"\\InputProcessorsDir\\"+ this.keyName;
    }


    public void run() {
        // download the input from the S3
        this.aws.downloadFromS3(this.keyName, this.url);
        // create sqs msg for ach url in the input
        // if the msg is a termination msg -> notify the manager and start termination process
        AtomicInteger jobsCount = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Path.of(this.url))) {
            lines.forEach(line -> {
                if (this.terminate)
                    return;
                try {
                    this.jobsQ.put(new Message(this.localID, line));
                    jobsCount.addAndGet(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            // TODO : delete downloaded file
            this.jobsDoneQ.put(new Message(this.localID, "UPLOAD DONE-" + jobsCount.get()));
        } catch (Exception e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    public void terminate() {
        this.terminate = true;
    }



}