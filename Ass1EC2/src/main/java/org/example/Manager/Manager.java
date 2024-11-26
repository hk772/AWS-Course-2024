package org.example.Manager;

import org.example.Messages.Message;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Manager {

    BlockingQueue<Message> jobs;
    JobQueueController jobQController;
    final String WORKER_JAR = "worker.jar";
    final String S3_PATH = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\S3";
    BlockingQueue<Message> inputs;
    BlockingQueue<Message> outputs;
    BlockingQueue<Message> jobsDone;
    int ids = 0;
    Map<Integer, Integer[]> jobsCount;
    Map<Integer, Boolean> finishedUploading;
    Object jobsCountLock = new Object();
    boolean terminated = false;
    Map<Integer, InputProcessor> inputProcs;
    final Object terminationLock = new Object();



    public Manager() {
        this.jobs = new LinkedBlockingQueue<>();
        this.inputs = new LinkedBlockingQueue<>();
        this.outputs = new LinkedBlockingQueue<>();
        this.jobsDone = new LinkedBlockingQueue<>();

        this.jobsCount = new HashMap<>();
        this.finishedUploading = new HashMap<>();

        this.inputProcs = new HashMap<>();
        jobQController = new JobQueueController(jobs, jobsDone, 3, 1);

        jobQController.start();
        this.listenForInputs();
        this.listenForOutputs();
    }


    private void startOutputFileForNewLocal(String path){
        File file = new File(path);
        try{
            FileWriter myWriter = new FileWriter(this.S3_PATH + "\\" + ids + "out.html");
            String html = "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "<title>PDF Page 1</title>\n" +
                    "</head>\n" +
                    "<body>\n";
            myWriter.write(html);
            myWriter.close();
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public synchronized Object[] signIn() {
        synchronized (this) {
            ids++;
            this.startOutputFileForNewLocal(this.S3_PATH + ids + "out.html");
            this.jobsCount.put(ids, new Integer[]{0,0});
            this.finishedUploading.put(ids, false);
            return new Object[]{ids, inputs, outputs};
        }
    }

    public void listenForInputs() {
        Thread messageThread = new Thread(() -> {
            System.out.println("Manager is listening to msgs :)");

            try {
                while (!this.terminated) {
                    Message message = inputs.take(); // Wait for a message
                    if (message.content.equals("TERMINATE")) {
                        this.terminate();
                    }
                    else{
                        InputProcessor inputProcessor = new InputProcessor(this, message, jobs, jobsDone);
                        inputProcessor.start();
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted.");
                Thread.currentThread().interrupt();
            }
        });

        messageThread.start();
    }

    private void handleUploadDoneMsg(Message message){
        this.finishedUploading.put(message.localID, true);
        int totalJobs = Integer.parseInt(message.content.split("-")[1]);
        synchronized (jobsCountLock) {
            int done = jobsCount.get(message.localID)[0];
            jobsCount.put(message.localID, new Integer[]{done,totalJobs});
        }
        synchronized (this.terminationLock) {
            this.inputProcs.remove(message.localID);
        }
    }


    private void handleJobDoneMsg(Message message){
        try {
            // add entry to output file
            FileWriter myWriter = new FileWriter(this.S3_PATH + "//" + message.localID + "out.html", true);
            myWriter.write(message.content + "\n");

            // update the amount of jobs done for the local and check if itsa the last one
            synchronized (jobsCountLock) {
                int done = this.jobsCount.get(message.localID)[0];
                int notDone = this.jobsCount.get(message.localID)[1];

                if (done + 1 == notDone && this.finishedUploading.get(message.localID)) {
                    this.jobsCount.remove(message.localID);

                    myWriter.write("</body>\n</html>");
                    this.outputs.put(new Message(message.localID, this.S3_PATH + "//" + message.localID + "out.html"));

                    if (this.jobsCount.isEmpty()) {
                        System.out.println("finished");
                    }
                } else {
                    this.jobsCount.put(message.localID, new Integer[]{done + 1, notDone});
                }
            }
            myWriter.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void listenForOutputs() {
        Thread doneMessageThread = new Thread(() -> {
            System.out.println("Manager is listening to workers :)");

            try {
                while (true) {
                    Message message = jobsDone.take(); // Wait for a message
                    if (message.content.contains("UPLOAD DONE")) {
                        this.handleUploadDoneMsg(message);
                    } else {
                        this.handleJobDoneMsg(message);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted.");
                Thread.currentThread().interrupt();
            }
        });

        doneMessageThread.start();
    }

    public synchronized void terminate(){
        this.terminated = true;
        this.jobQController.terminate();
        synchronized (this.terminationLock) {
            for (InputProcessor ip : inputProcs.values()) {
                ip.terminate();
            }
        }
    }


}


