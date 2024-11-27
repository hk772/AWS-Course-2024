package org.example.Manager;

import org.example.App;
import org.example.Messages.Message;
import org.example.Worker.Worker;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

public class JobQueueController extends Thread {
    BlockingQueue<Message> jobsQ;
    BlockingQueue<Message> jobsDoneQ;
    int workersCount = 0;
    int MAX_WORKERS_COUNT = 10;
    int jobsPerWorker;
    LinkedList<Worker> workers;
    boolean allWorkersTerminated = false;
    App aws;

    public JobQueueController(BlockingQueue<Message> jobsQ, BlockingQueue<Message> jobsDoneQ, int maxWorkersCount, int jobsPerWorker, App aws) {
        this.jobsQ = jobsQ;
        this.jobsDoneQ = jobsDoneQ;
//        this.MAX_WORKERS_COUNT = maxWorkersCount;
        this.jobsPerWorker = jobsPerWorker;
        workers = new LinkedList<>();
        this.aws = aws;
    }

    public void run() {
        while (!allWorkersTerminated) {
            if(this.workersCount == this.MAX_WORKERS_COUNT) {
                // do nothing?
            } else if (this.workersCount < this.MAX_WORKERS_COUNT) {
                if (jobsQ.size() > this.workersCount * this.jobsPerWorker) {
                    this.createWorker();
                }
            }
        }
    }

    private void createWorker() {
        Worker w = null;
        try {
            w = new Worker(this.jobsQ, this.jobsDoneQ, this.aws, this.workersCount);
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
