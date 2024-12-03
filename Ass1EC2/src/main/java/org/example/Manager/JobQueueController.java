package org.example.Manager;

import org.example.App;
import org.example.Worker.Worker;

import java.util.LinkedList;

public class JobQueueController extends Thread {
    int workersCount = 0;
    int MAX_WORKERS_COUNT = 3;
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
                // TODO: sleep until termination
            } else if (this.workersCount < this.MAX_WORKERS_COUNT) {
                int jobsQSize = this.aws.getQueueSize(this.jobsQUrl);
                while (this.workersCount < this.MAX_WORKERS_COUNT && jobsQSize > this.workersCount * this.jobsPerWorker) {
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
        this.aws.initWorker(String.valueOf(this.workersCount));
        System.out.println("created worker");
        workersCount += 1;
    }

    public void terminate() {

    }
}
