package org.example;


import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.file.Paths;
import java.util.Map;

import org.example.Messages.Message;

public class App {

    public static String inputQ = "INPUTS";
    public static String outputQ = "OUTPUTS";
    public static String jobQ = "JOBS";
    public static String jobDoneQ = "JOBS_DONE";
    public static String BUCKET_NAME = "my-great-bucket-mevuzarot-2024";

    public final String IMAGE_AMI = "";
    public software.amazon.awssdk.regions.Region region = Region.US_EAST_1;
    public software.amazon.awssdk.regions.Region region2 = Region.US_WEST_2;
    private final S3Client s3;
    private SqsClient sqs;
    private Ec2Client ec2;

    public App(){
        s3 = S3Client.builder().region(region2).build();
        sqs = SqsClient.builder().region(region2).build();
//        ec2 = Ec2Client.builder().region(region).build();
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


    public static void main(String[] args) {
//        App app = new App();
//        app.createS3Bucket(App.BUCKET_NAME);
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass1.pdf", "ass1.pdf");
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass2.pdf", "ass2.pdf");
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass3.pdf", "ass3.pdf");
//
//
//        app.createQueue(App.signInQ);
//        app.createQueue(App.inputQ);
//        app.createQueue(App.outputQ);
//        app.createQueue(App.jobQ);
//        app.createQueue(App.jobDoneQ);

//        String inputsQUrl = app.getQueueUrl(App.inputQ);
//        Object ans = null;
//        while (ans == null) {
//            ans = app.popFromSQSAutoDel(inputsQUrl);
//            System.out.println("try");
//        }
//        Message msg = (Message) ans;
//        System.out.println("msg id " + msg.localID + "\nmsg content: " + msg.content);

    }
}