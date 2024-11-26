package org.example.Manager;


import org.example.Messages.Message;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class InputProcessor extends Thread{
    String url;
    BlockingQueue<Message> jobsQ;
    BlockingQueue<Message> jobsDoneQ;
    Manager manager;
    int localID;
    boolean terminate = false;

    public InputProcessor(Manager manager, Message msg, BlockingQueue<Message> jobsQ, BlockingQueue<Message> jobsDoneQ) {
        this.url = msg.content;
        this.localID = msg.localID;
        this.jobsQ = jobsQ;
        this.jobsDoneQ = jobsDoneQ;
        this.manager = manager;
    }


    public void run() {
        // download the input from the S3
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
            this.jobsDoneQ.put(new Message(this.localID, "UPLOAD DONE-" + jobsCount.get()));
        } catch (Exception e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    public void terminate() {
        this.terminate = true;
    }



}