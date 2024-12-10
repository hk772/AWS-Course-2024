package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.other.Guard;
import org.example.other.MsgJsonizer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.example.other.Message;

public class App {

    public static final String terminationQ = "TERMINATEQ";
    public static final String inputQ = "INPUTS";
    public static final String outputQ = "OUTPUTS";
    public static final String jobQ = "JOBS";
    public static final String jobDoneQ = "JOBS_DONE";
    public static final String BUCKET_NAME = "my-great-bucket-mevuzarot-2024";
    public static final String KEY_PAIR = "Mevuzarot2024";

    public static final String Manager_AMI = "ami-0ceac033b9abf68fb";
    public static final String Worker_AMI = "ami-0c9a06467c728f680";

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

    public void uploadFileToS3DiffernetBucket(String filePath, String keyName, String bucketName) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
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

    public String createQueueWithCustomVisibilityTimout(String queueName, int visibilityTimeout) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilityTimeout)))
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
                .build();

        SendMessageResponse sendMessageResponse = sqs.sendMessage(sendMessageRequest);
    }

    public List<Instance> runInstanceFromAmiWithScript(String ami, int min, int max, String script, String instanceName, String labelValue) {
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
        RunInstancesResponse response = ec2.runInstances(runInstancesRequest);
        List<Instance> instances = response.instances();
        System.out.println("instance inited");
        return instances;
    }

    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();

        DescribeInstancesResponse describeInstancesResponse = null;
        describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<Instance> getAllInstancesWithLabelAllStates(String label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                        .name("tag:Label")
                                        .values(label)
                                        .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

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

    public void initManagerIfNotExists(int loadFactor){
        try {
            List<Instance> lst = this.getAllInstancesWithLabel("Manager");
            System.out.println("number of managers: " + lst.size());

            if (lst.isEmpty()){
                try {
                    this.initForFirstRun();
                } catch (Exception e) {
                    System.out.println("Q's exist");
                }

                String filePath = "C:\\Users\\hagai\\.aws\\credentials";
                initSpecificEC2(Manager_AMI, filePath, "Manager", "manager", "Manager", 1, String.valueOf(loadFactor));
                System.out.println("manager inited");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void initWorker(String name){
        try {
            String filePath = "/root/.aws/credentials";
            initSpecificEC2(Worker_AMI, filePath, name, "worker", "Worker", 1, "");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Instance> initWorkers(String name, int max){
        try {
            String filePath = "/root/.aws/credentials";
            return initSpecificEC2(Worker_AMI, filePath, name, "worker", "Worker", max, "");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<Instance> initSpecificEC2(String AMI, String filePath, String name, String jarName, String label, int max, String jarArgs) throws IOException {
        String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));

        try {
            String encryptedCredentials = Guard.encrypt(fileContent);
            String script = String.format("#!/bin/bash\n" +
                            "set -e\n" +
                            "cd /root && \\\n" +
                            "if [ -f %s.jar ]; then \\\n" +
                            "    java -jar %s.jar %s %s >> /var/log/user-data.log 2>&1; \\\n" +
                            "else \\\n" +
                            "    echo \"%s.jar not found\" >> /var/log/user-data.log; \\\n" +
                            "fi\n",
                    jarName, jarName, encryptedCredentials, jarArgs, jarName);

            return this.runInstanceFromAmiWithScript(AMI, 1, max, script, name, label);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }


    private void prepareQInit(String name, int visibility){
        try {
            String qUrl = this.getQueueUrl(name);
            sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(qUrl).build());
        } catch (QueueDoesNotExistException e) {
            this.createQueueWithCustomVisibilityTimout(name, visibility);
        }
    }

    public void initForFirstRun(){
        System.out.println("initForFirstRun bucket and queues");
        this.createS3Bucket(App.BUCKET_NAME);

        this.prepareQInit(App.inputQ, 30);
        this.prepareQInit(App.outputQ, 0);
        this.prepareQInit(App.jobQ, 30);
        this.prepareQInit(App.jobDoneQ, 30);
        this.prepareQInit(App.terminationQ, 0);
//
//        this.createQueue(App.inputQ);
//        this.createQueueWithCustomVisibilityTimout(App.outputQ, 0);
//        this.createQueue(App.jobQ);
//        this.createQueue(App.jobDoneQ);
//        this.createQueueWithCustomVisibilityTimout(App.terminationQ, 0);
    }


    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // Terminate the instance
        ec2.terminateInstances(terminateRequest);

        System.out.println("Terminated instance: " + instanceId);
    }

    public String getMyInstanceID(){
        try {
            String tokenUrl = "http://169.254.169.254/latest/api/token";
            HttpURLConnection tokenConn = (HttpURLConnection) new URL(tokenUrl).openConnection();
            tokenConn.setRequestMethod("PUT");
            tokenConn.setRequestProperty("X-aws-ec2-metadata-token-ttl-seconds", "21600"); // Token valid for 6 hours
            tokenConn.connect();

            BufferedReader tokenReader = new BufferedReader(new InputStreamReader(tokenConn.getInputStream()));
            String token = tokenReader.readLine();
            tokenReader.close();

            String metadataUrl = "http://169.254.169.254/latest/meta-data/instance-id";
            HttpURLConnection metadataConn = (HttpURLConnection) new URL(metadataUrl).openConnection();
            metadataConn.setRequestMethod("GET");
            metadataConn.setRequestProperty("X-aws-ec2-metadata-token", token); // Pass the token

            BufferedReader metadataReader = new BufferedReader(new InputStreamReader(metadataConn.getInputStream()));
            String instanceId = metadataReader.readLine();
            metadataReader.close();

            return instanceId;
        } catch (Exception e) {
            System.err.println("Error retrieving instance ID: " + e.getMessage());
        }
        return "";
    }

    public void terminateMyself(){
        String instanceId = getMyInstanceID();
        this.terminateInstance(instanceId);
    }

    public void deleteFileFromBucket(String fileName) {
        try {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(App.BUCKET_NAME)
                    .key(fileName)
                    .build();
            s3.deleteObject(deleteObjectRequest);

            System.out.println("File deleted successfully: " + fileName);
        } catch (S3Exception e) {
            e.printStackTrace();
            System.out.println("Error occurred while deleting the file" + fileName);
        }
    }

    public void emptyBycket(){
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(App.BUCKET_NAME).build();

        ListObjectsResponse listResponse = s3.listObjects(listRequest);
        List<S3Object> listObjects = listResponse.contents();

        List<ObjectIdentifier> objectsToDelete = new ArrayList<ObjectIdentifier>();

        for (S3Object s3Object : listObjects) {
            objectsToDelete.add(ObjectIdentifier.builder().key(s3Object.key()).build());
        }

        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(App.BUCKET_NAME)
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();

        DeleteObjectsResponse deleteObjectsResponse = s3.deleteObjects(deleteObjectsRequest);
    }

    public void deleteBucket(){
        this.emptyBycket();

        DeleteBucketRequest request = DeleteBucketRequest.builder().bucket(App.BUCKET_NAME).build();
        s3.deleteBucket(request);
        System.out.println("Bucket deleted.");
    }

    public void deleteSQS(String name){
        String queueUrl = this.getQueueUrl(name);

        try {
            sqs.deleteQueue(DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build());

            System.out.println("Queue deleted successfully: " + name);
        } catch (QueueDoesNotExistException e) {
            System.out.println("Queue does not exist: " + queueUrl);
        } catch (SqsException e) {
            e.printStackTrace();
            System.out.println("Error occurred while deleting the queue");
        }
    }

    public void verifyAllEC2sTerminated(){
        List<Instance> instances = this.getAllInstances();
        for (Instance instance : instances){
            if (!instance.state().nameAsString().equals("terminated")) {
                this.terminateInstance(instance.instanceId());
            }
        }
    }

    public void deleteQs(){
        this.deleteSQS(App.inputQ);
        this.deleteSQS(App.outputQ);
        this.deleteSQS(App.jobDoneQ);
        this.deleteSQS(App.jobQ);
        this.deleteSQS(App.terminationQ);
    }

    public void deleteAllResources(){
        this.deleteBucket();
        this.deleteQs();
        this.verifyAllEC2sTerminated();
    }

    public static void main(String[] args) throws JsonProcessingException {
        App app = new App();
//        String pdfBucket = "pdfs-bucket-mevuzarot-2024";
//        app.createS3Bucket(pdfBucket);
//        app.initForFirstRun();
        app.createQueue(App.terminationQ);
        app.pushToSQS(app.getQueueUrl(App.terminationQ), new Message("12", "asd"));
    }
}