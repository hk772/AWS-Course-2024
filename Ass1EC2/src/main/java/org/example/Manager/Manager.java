package org.example.Manager;

import org.example.App;
import org.example.Messages.Message;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Manager {

//    BlockingQueue<Message> jobs;
    JobQueueController jobQController;
//    final String WORKER_JAR = "worker.jar";
//    final String S3_PATH = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\S3";
//    BlockingQueue<Message> inputs;
//    BlockingQueue<Message> outputs;
//    BlockingQueue<Message> jobsDone;
    int ids = 0;
    Map<Integer, Integer[]> jobsCount;
    Map<Integer, Boolean> finishedUploading;
    final Object jobsCountLock = new Object();
    boolean terminated = false;
    Map<Integer, InputProcessor> inputProcs;
    final Object terminationLock = new Object();
    final Object signInLock = new Object();
    App aws;
    String myDirPath;

    String jobsQUrl;
    String inputsQUrl;
    String outputsQUrl;
    String jobsDoneQUrl;


    public Manager() throws IOException {
        this.aws = new App();
        this.initQs();

        this.myDirPath = System.getProperty("user.dir") + "\\ManagersDir";
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "\\ManagersDir"));




        this.jobsCount = new HashMap<>();
        this.finishedUploading = new HashMap<>();

        this.inputProcs = new HashMap<>();
        jobQController = new JobQueueController(this.jobsQUrl, this.jobsDoneQUrl, 3, 1);

        jobQController.start();
        this.listenForInputs();
        this.listenForOutputs();
    }

    private void initQs(){
        this.jobsQUrl = this.aws.getQueueUrl(App.jobQ);
        this.inputsQUrl = this.aws.getQueueUrl(App.inputQ);
        this.outputsQUrl = this.aws.getQueueUrl(App.outputQ);
        this.jobsDoneQUrl = this.aws.getQueueUrl(App.jobDoneQ);
    }


    private void startOutputFileForNewLocal(String path) {
        File file = new File(path);
        try (FileWriter myWriter = new FileWriter(this.myDirPath + "\\" + ids + "out.html")) {
            String html = """
            <!DOCTYPE html>
            <html>
            <head>
            <title>PDF Page 1</title>
            </head>
            <body>
            """;
            myWriter.write(html);
        } catch (IOException e) {
            System.err.println("Error creating output file: " + e.getMessage());
        }
    }

    public synchronized int signIn() {
        synchronized (this.signInLock) {
            ids++;
            this.startOutputFileForNewLocal(this.myDirPath + ids + "out.html");
            this.jobsCount.put(ids, new Integer[]{0,0});
            this.finishedUploading.put(ids, false);
            return ids;
        }
    }

    public void listenForInputs() {
        Thread messageThread = new Thread(() -> {
            System.out.println("Manager is listening to inputs:)");

            try {
                while (!this.terminated) {
                    Message message = this.aws.popFromSQSAutoDel(this.inputsQUrl);
                    if (message != null) {
                        if (message.content.equals("TERMINATE")) {
                            this.terminate();
                        } else if (message.content.equals("SIGNIN")) {
                            // TODO: do this
//                        this.signIn();
                        } else{
                            InputProcessor inputProcessor = new InputProcessor(this, message, this.jobsQUrl, this.jobsDoneQUrl);
                            inputProcessor.start();
                        }
                    }
                    else {
                        Thread.sleep(1000);
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


    private void handleJobDoneMsg(Message message) {
        String name = message.localID + "out.html";
        String curPath = this.myDirPath + "\\" + name;

        synchronized (jobsCountLock) {
            try (FileWriter myWriter = new FileWriter(curPath, true)) {
                // Append job result to the file
                myWriter.write(message.content + "\n");

                    int done = this.jobsCount.get(message.localID)[0];
                    int notDone = this.jobsCount.get(message.localID)[1];

                    // If all jobs are done for this local ID
                    if (done + 1 == notDone && this.finishedUploading.get(message.localID)) {
                        this.jobsCount.remove(message.localID);
                        myWriter.write("</body>\n</html>");
                        myWriter.close();
                        // Upload file to S3 and clean up
                        this.aws.uploadFileToS3(curPath, name);
                        this.aws.pushToSQS(this.outputsQUrl, new Message(message.localID, name));

//                        this.outputs.put(new Message(message.localID, name));
//                        Files.delete(Paths.get(curPath));

                        if (this.jobsCount.isEmpty()) {
                            System.out.println("Finished all jobs");
                        }
                    } else {
                        this.jobsCount.put(message.localID, new Integer[]{done + 1, notDone});
                    }
            } catch (IOException e) {
                System.err.println("Error handling job done: " + e.getMessage());
            }
        }
    }

    public void listenForOutputs() {
        Thread doneMessageThread = new Thread(() -> {
            System.out.println("Manager is listening to workers :)");

            try {
                while (true) {
                    Message message = this.aws.popFromSQSAutoDel(this.jobsDoneQUrl);
                    if (message != null) {
                        if (message.content.contains("UPLOAD DONE")) {
                            this.handleUploadDoneMsg(message);
                        } else {
                            this.handleJobDoneMsg(message);
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


