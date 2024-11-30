package org.example.Manager;

import org.example.App;
import org.example.Messages.Message;
import org.example.Worker.Worker;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

public class JobQueueController extends Thread {
//    BlockingQueue<Message> jobsQ;
//    BlockingQueue<Message> jobsDoneQ;
    int workersCount = 0;
    int MAX_WORKERS_COUNT = 10;
    int jobsPerWorker;
    LinkedList<Worker> workers;
    boolean allWorkersTerminated = false;
    App aws;
    String jobsQUrl;
    String jobsDoneQUrl;

    public JobQueueController(String jobsQUrl, String jobsDoneQUrl, int maxWorkersCount, int jobsPerWorker) {
        this.jobsQUrl = jobsQUrl;
        this.jobsDoneQUrl = jobsDoneQUrl;
//        this.MAX_WORKERS_COUNT = maxWorkersCount;
        this.jobsPerWorker = jobsPerWorker;
        workers = new LinkedList<>();
        this.aws = new App();
    }

    public void run() {
        while (!allWorkersTerminated) {
            if(this.workersCount == this.MAX_WORKERS_COUNT) {
                // do nothing?
            } else if (this.workersCount < this.MAX_WORKERS_COUNT) {
                int jobsQSize = this.aws.getQueueSize(this.jobsQUrl);
                if (jobsQSize > this.workersCount * this.jobsPerWorker) {
                    this.createWorker();
                }
            }
            try {
                sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createWorker() {
        Worker w = null;
        try {
            w = new Worker(this.jobsQUrl, this.jobsDoneQUrl, this.workersCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        workersCount += 1;
        w.start();
    }

    public void terminate() {
        for (Worker w : workers) {
            w.terminate();
        }
        allWorkersTerminated = true;
    }
}
