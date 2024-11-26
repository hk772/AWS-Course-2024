package org.example.Worker;

import org.example.Messages.Message;

import java.util.concurrent.BlockingQueue;

public class Worker extends Thread {

    BlockingQueue<Message> jobsQ;
    BlockingQueue<Message> jobsDoneQ;
    Operations operations;
    final String outAddress = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\S3\\worker ";
    boolean terminated = false;

    public Worker(BlockingQueue<Message> jobsQ, BlockingQueue<Message> jobsDoneQ) {
        this.jobsQ = jobsQ;
        this.jobsDoneQ = jobsDoneQ;
        this.operations = new Operations();
    }


    private Message execute(String job, int id){
        String[] parts = job.split("\t");
        String op = parts[0];
        String address = parts[1];
        String[] chain = address.split("\\\\");
        String name = chain[chain.length - 1].split("\\.")[0];
        String outputAddress = this.outAddress + id + name;

        String contentStart = op + "\t" + address + "\t";

        try{
            switch (op) {
                case "ToImage":
                    outputAddress += ".jpg";
                    operations.performOperation(1, address,  outputAddress);
                    break;
                case "ToHTML":
                    outputAddress += ".html";
                    operations.performOperation(2, address, outputAddress );
                    break;
                case "ToText":
                    outputAddress += ".txt";
                    operations.performOperation(2, address, outputAddress );
                    break;
            }
            return new Message(id, contentStart + outputAddress);
        } catch (Exception e){
            return new Message(id, contentStart + e.getMessage());

        }

    }

    @Override
    public void run() {
        while (!this.terminated) {
            try {
                Message msg = jobsQ.take();
                Message msgDone = this.execute(msg.content, msg.localID);
                jobsDoneQ.put(msgDone);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void terminate(){
        this.terminated = true;
    }
}
