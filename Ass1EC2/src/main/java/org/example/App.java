package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.example.Messages.Message;

public class App {

    public static final String inputQ = "INPUTS";
    public static final String outputQ = "OUTPUTS";
    public static final String jobQ = "JOBS";
    public static final String jobDoneQ = "JOBS_DONE";
    public static final String BUCKET_NAME = "my-great-bucket-mevuzarot-2024";
    public static final String KEY_PAIR = "Mevuzarot2024";

    public static final String Manager_AMI = "ami-09a36cd4f4f8be752";
    public static final String Worker_AMI = "ami-0d5f07a9b7d320b33";

    public software.amazon.awssdk.regions.Region region = Region.US_EAST_1;
    public software.amazon.awssdk.regions.Region region2 = Region.US_WEST_2;
    private final S3Client s3;
    private SqsClient sqs;
    private Ec2Client ec2;

    public App(){
        s3 = S3Client.builder().region(region2).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public void createS3Bucket(String bucketName){
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            System.out.println("Bucket created: " + bucketName);
        } catch (Exception e) {
            // TODO : handle cant create bucket exception
            System.out.println(e.getMessage());
        }
    }

    public void uploadFileToS3(String filePath, String keyName) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(keyName)
                    .build();

            this.s3.putObject(putObjectRequest, Paths.get(filePath));
            System.out.println("File uploaded successfully: " + keyName);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public void downloadFromS3(String keyName, String downloadPath) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(keyName)
                    .build();

            this.s3.getObject(getObjectRequest, Paths.get(downloadPath));
            System.out.println("File downloaded successfully: " + keyName);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

    }

    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse create_result = null;
        create_result = sqs.createQueue(request);

        assert create_result != null;
        String queueUrl = create_result.queueUrl();
        System.out.println("Created queue '" + queueName + "', queue URL: " + queueUrl);
        return queueUrl;
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = null;
        queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.println("Queue URL: " + queueUrl);
        return queueUrl;
    }

    public int getQueueSize(String queueUrl) {
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED
                )
                .build();

        GetQueueAttributesResponse queueAttributesResponse = null;
        queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }

    public Message popFromSQSAutoDel(String qUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(qUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(3)
                    .build();

            ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);

            if (!receiveMessageResponse.messages().isEmpty()) {
                software.amazon.awssdk.services.sqs.model.Message msg = receiveMessageResponse.messages().get(0); // it returns array of msgs
                String json = msg.body();
                Message myMsg = MsgJsonizer.deJasonize(json);

                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(qUrl)
                        .receiptHandle(msg.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteMessageRequest);
                return myMsg;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    //returns [Message, DeleteMsgRequest]
    public Object[] popFromSQS(String qUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(qUrl)
                    .maxNumberOfMessages(1)
                    .build();

            ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);

            if (!receiveMessageResponse.messages().isEmpty()) {
                software.amazon.awssdk.services.sqs.model.Message msg = receiveMessageResponse.messages().get(0); // it returns array of msgs
                String json = msg.body();
                Message myMsg = MsgJsonizer.deJasonize(json);

                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(qUrl)
                        .receiptHandle(msg.receiptHandle())
                        .build();

                return new Object[]{myMsg, deleteMessageRequest};
            } else {
                return new Object[]{null, null};
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return new Object[]{null, null};
        }
    }

    public void deleteMsgFromSqs(DeleteMessageRequest deleteMessageRequest){
        sqs.deleteMessage(deleteMessageRequest);
    }

    public void pushToSQS(String queueUrl, Message message) throws JsonProcessingException {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(MsgJsonizer.jsonize(message))
//                .delaySeconds(1) // Optional: Delay the message by 10 seconds
                .build();

        SendMessageResponse sendMessageResponse = sqs.sendMessage(sendMessageRequest);
    }

    public void runInstanceFromAmiWithScript(String ami, int min, int max, String script, String instanceName, String labelValue) {
        Tag nameTag = Tag.builder()
                .key("Name")
                .value(instanceName)
                .build();

        Tag labelTag = Tag.builder()
                .key("Label")
                .value(labelValue)
                .build();

        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(min)
                .maxCount(max)
                .keyName(App.KEY_PAIR)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .tagSpecifications(
                        TagSpecification.builder()
                                .resourceType(ResourceType.INSTANCE)
                                .tags(nameTag, labelTag)  // Assign both the name and label tags
                                .build()
                )
                .build();

        // Launch the instance
        ec2.runInstances(runInstancesRequest);
        System.out.println("instance inited");
    }

    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();

        DescribeInstancesResponse describeInstancesResponse = null;
        describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<Instance> getAllInstancesWithLabel(String label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                    .name("tag:Label")
                                    .values(label)
                                    .build(),
                                Filter.builder()
                                        .name("instance-state-name")
                                        .values("pending", "running")
                                        .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public void initManagerIfNotExists(){
        try {
            System.out.println("checking if ahve manager");
            List<Instance> lst = this.getAllInstancesWithLabel("Manager");
            System.out.println("number of managers: " + lst.size());
            if (lst.size() == 0){
                String filePath = "C:\\Users\\hagai\\.aws\\credentials";
                initSpecificEC2(Manager_AMI, filePath, "Manager", "manager", "Manager");
                System.out.println("manager inited");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void initWorker(String name){
        try {
            String filePath = "/root/.aws/credentials";
            initSpecificEC2(Worker_AMI, filePath, name, "worker", "Worker");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initSpecificEC2(String AMI, String filePath, String name, String jarName, String label) throws IOException {
        String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));

//        String script = String.format("#!/bin/bash\n" +
//                        "set -e\n" +
//                        "mkdir -p /root/.aws && \\\n" +
//                        "echo -e \"%s\" > /root/.aws/credentials && \\\n" +
//                        "cd /root && \\\n" +  // Change to /root/ directory
//                        "if [ -f %s.jar ]; then \\\n" +
//                        "    java -jar %s.jar >> /var/log/user-data.log 2>&1; \\\n" +
//                        "else \\\n" +
//                        "    echo \"%s.jar not found\" >> /var/log/user-data.log; \\\n" +
//                        "fi\n",
//                fileContent, jarName, jarName, jarName);

        // with doenload
        String script = String.format("#!/bin/bash\n" +
                        "set -e\n" +
                        "mkdir -p /root/.aws && \\\n" +
                        "echo -e \"%s\" > /root/.aws/credentials && \\\n" +
                        "aws s3 cp s3://%s/%s.jar /root/ && \\\n" +  // Download to /root/
                        "echo \"Downloaded %s.jar\" >> /var/log/user-data.log && \\\n" +
                        "cd /root && \\\n" +  // Change to /root/ directory where manager.jar is
                        "if [ -f %s.jar ]; then \\\n" +
                        "    java -jar %s.jar >> /var/log/user-data.log 2>&1; \\\n" +
                        "else \\\n" +
                        "    echo \"%s.jar not found\" >> /var/log/user-data.log; \\\n" +
                        "fi\n",
                fileContent, App.BUCKET_NAME, jarName, jarName, jarName, jarName, jarName);

        this.runInstanceFromAmiWithScript(AMI, 1, 1, script, name, label);
    }

    public void initForFirstRun(){
//        this.createS3Bucket(App.BUCKET_NAME);

        this.createQueue(App.inputQ);
        this.createQueue(App.outputQ);
        this.createQueue(App.jobQ);
        this.createQueue(App.jobDoneQ);
    }

    public static void main(String[] args) {
        App app = new App();
        app.initForFirstRun();

        // upload testing files to the bucket
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass1.pdf", "ass1.pdf");
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass2.pdf", "ass2.pdf");
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass3.pdf", "ass3.pdf");

        // upload manager and worker jars:
//        String managerJarPath = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Jars Newest\\Manager\\manager.jar";
//        String workerJarPath = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Jars Newest\\Worker\\worker.jar";
//
//        app.uploadFileToS3(managerJarPath, "manager.jar");
//        app.uploadFileToS3(workerJarPath, "worker.jar");


//        app.initManagerIfNotExists();

//        String managerJarPath = "C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Jars Newest\\Manager\\manager.jar";
//        app.uploadFileToS3(managerJarPath, "manager.jar");

//        List<Instance> instances = app.getAllInstances();
//        System.out.println("number total: " + instances.size());
//
//        try {
//            List<Instance> managerInstances = app.getAllInstancesWithLabel("Manager");
//            System.out.println("number of managers: " + managerInstances.size());
//        } catch (InterruptedException e) {
//            System.out.println(e.getMessage());
//        }

    }
}