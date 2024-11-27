package org.example;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.File;
import java.nio.file.Paths;

public class App {

    public final String IMAGE_AMI = "";
    public software.amazon.awssdk.regions.Region region = Region.US_EAST_1;
    private final S3Client s3;
    private SqsClient sqs;
    private Ec2Client ec2;
    public final String BUCKET_NAME = "firstbucketmevuzaroe2024";

    public App(){
        s3 = S3Client.builder().region(region).build();
//        SqsClient sqs = SqsClient.builder().region(region).build();
//        Ec2Client ec2 = Ec2Client.builder().region(region).build();
    }

    public void createS3Bucket(String bucketName){
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
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
            System.out.println("File uploaded successfully.");
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
            System.out.println("File downloaded successfully.");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

    }

    public void createSQS(){

    }

    public void getFromSQS(){

    }

    public void uploadToSQS(){

    }


    public static void main(String[] args) {
        App app = new App();
//        app.createS3Bucket("firstbucketmevuzaroe2024");
        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass1.pdf", "ass1.pdf");
//        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass2.pdf", "ass2.pdf");
        app.uploadFileToS3("C:\\Users\\hagai\\Documents\\uni\\year 5\\mevuzarot\\assignments\\Ass1EC2\\src\\main\\java\\org\\example\\PDFS\\ass3.pdf", "ass3.pdf");
//
    }
}