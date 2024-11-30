package org.example.Manager;

import com.fasterxml.jackson.core.JsonProcessingException;
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
//    final int EXPECTED_AMOUNT_OF_LOCALS = 5;
//    int ids = EXPECTED_AMOUNT_OF_LOCALS;
    Map<String, Integer[]> jobsCount;
    Map<String, Boolean> finishedUploading;
    final Object jobsCountLock = new Object();
    boolean terminated = false;
    Map<String, InputProcessor> inputProcs;
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
//        this.initSignIns();

        this.myDirPath = System.getProperty("user.dir") + "\\ManagersDir";
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "\\ManagersDir"));
        Files.createDirectories(Paths.get(System.getProperty("user.dir"), "\\InputProcessorsDir"));

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

//    private void initSignIns() throws JsonProcessingException {
//        for(int i = 0; i < this.EXPECTED_AMOUNT_OF_LOCALS; i++){
//            this.aws.pushToSQS(this.signInQUrl, new Message(i+1, "Available Id"));
//        }
//    }

    private void startOutputFileForNewLocal(String path) {
        File file = new File(path);
        try (FileWriter myWriter = new FileWriter(path)) {
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

    public void signIn(String localId) {
        synchronized (this.signInLock) {
            this.startOutputFileForNewLocal(this.myDirPath + "\\" + localId + "out.html");
            this.jobsCount.put(localId, new Integer[]{0,0});    // for local 2, 12/14 jobs done
            this.finishedUploading.put(localId, false);
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
                        } else{
                            this.signIn(message.localID);
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

    private void checkIfFinishedJobsForLocal(String id){
        int done = this.jobsCount.get(id)[0];
        int all = this.jobsCount.get(id)[1];

        // If all jobs are done for this local ID
        if (done == all && this.finishedUploading.get(id)) {
            this.jobsCount.remove(id);
            String name = id + "out.html";
            String curPath = this.myDirPath + "\\" + name;

            try (FileWriter myWriter = new FileWriter(curPath, true)) {
                myWriter.write("</body>\n</html>");
            } catch (IOException e) {
                System.err.println("Error handling job done: " + e.getMessage());
            }

            this.aws.uploadFileToS3(curPath, name);
            try {
                this.aws.pushToSQS(this.outputsQUrl, new Message(id, name));
            } catch (JsonProcessingException e) {
                System.out.println(e.getMessage());
            }

            try {
                Files.delete(Paths.get(curPath));
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private void handleUploadDoneMsg(Message message){
        this.finishedUploading.put(message.localID, true);
        int totalJobs = Integer.parseInt(message.content.split("-")[1]);
        synchronized (jobsCountLock) {
            int done = jobsCount.get(message.localID)[0];
            jobsCount.put(message.localID, new Integer[]{done,totalJobs});
            this.checkIfFinishedJobsForLocal(message.localID);
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
                myWriter.write(message.content + "\n");
                this.jobsCount.get(message.localID)[0]++;
            } catch (IOException e) {
                System.err.println("Error handling job done: " + e.getMessage());
            }

            this.checkIfFinishedJobsForLocal(message.localID);

            if (this.jobsCount.isEmpty()) {
                System.out.println("Finished all jobs");
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


