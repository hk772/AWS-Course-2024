package org.example.Manager;

import org.example.App;
import software.amazon.awssdk.services.ec2.model.Instance;

import java.util.LinkedList;
import java.util.List;

public class JobQueueController extends Thread {
    int workersCount = 0;
    int MAX_WORKERS_COUNT = 10;
    int jobsPerWorker;
    boolean allWorkersTerminated = false;
    App aws;
    String jobsQUrl;
    String jobsDoneQUrl;
    List<String> workers;
    boolean terminated = false;

    public JobQueueController(String jobsQUrl, String jobsDoneQUrl, int jobsPerWorker) {
        this.jobsQUrl = jobsQUrl;
        this.jobsDoneQUrl = jobsDoneQUrl;
        this.jobsPerWorker = jobsPerWorker;
        workers = new LinkedList<>();
        this.aws = new App();
    }

    public void run() {
        while (!this.terminated) {
            if(this.workersCount == this.MAX_WORKERS_COUNT) {
                // TODO: sleep someone calls my terminate method
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else if (this.workersCount < this.MAX_WORKERS_COUNT) {
                int jobsQSize = this.aws.getQueueSize(this.jobsQUrl);
                int workersNeededTotal = (int)(jobsQSize / this.jobsPerWorker);
                int workersToAdd = Math.min(workersNeededTotal - this.workersCount, this.MAX_WORKERS_COUNT - this.workersCount);
                if (workersToAdd > 0) {
                    this.createWorkers(workersToAdd);
                }
            }
            try {
                sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createWorkers(int workersNeeded) {
        List<Instance> instances = this.aws.initWorkers("Worker", workersNeeded);
        for (Instance instance : instances) {
            this.workers.add(instance.instanceId());
        }
        System.out.println("created " + instances.size() + " workers");
        workersCount += instances.size();
    }


}
