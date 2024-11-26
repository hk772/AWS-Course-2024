package API;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AWS {

    public final String IMAGE_AMI = "ami-08902199a8aa0bc09";
    public Region region1 = Region.US_WEST_2;
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;
    private final String bucketName;
    private static AWS instance = null;

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region1).build();
        bucketName = "my-bucket";
    }

    public static AWS getInstance() {
        if (instance == null) {
            return new AWS();
        }

        return instance;
    }


//////////////////////////////////////////  EC2

    public void runInstanceFromAMI(String ami) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(InstanceType.T2_MICRO)
                .minCount(1)
                .maxCount(5) // todo decide what to put here
                .build();

        // Launch the instance
        try {
            ec2.runInstances(runInstancesRequest);
        } catch (InterruptedException ignored) {
        }
    }

    public RunInstancesResponse runInstanceFromAmiWithScript(String ami, InstanceType instanceType, int min, int max, String script) {
        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(ami)
                .instanceType(instanceType)
                .minCount(min)
                .maxCount(max)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                // @ADD security feratures
                .build();

        // Launch the instance
        try {
            ec2.runInstances(runInstancesRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Instance> getAllInstances() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();

        DescribeInstancesResponse describeInstancesResponse = null;
        try {
            describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
        } catch (InterruptedException ignored) {
        }

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public List<Instance> getAllInstancesWithLabel(Label label) throws InterruptedException {
        DescribeInstancesRequest describeInstancesRequest =
                DescribeInstancesRequest.builder()
                        .filters(Filter.builder()
                                .name("tag:Label")
                                .values(label.toString())
                                .build())
                        .build();

        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);

        return describeInstancesResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .toList();
    }

    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        // Terminate the instance
        try {
            ec2.terminateInstances(terminateRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Terminated instance: " + instanceId);
    }


    ////////////////////////////// S3

    public String uploadFileToS3(String keyPath, File file) throws Exception {
        System.out.printf("Start upload: %s, to S3\n", file.getName());

        PutObjectRequest req =
                PutObjectRequest.builder()
                
                .bucket(bucketName)
                        .key(keyPath)
                        .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already
        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }

    public void downloadFileFromS3(String keyPath, File outputFile) {
        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file.
            OutputStream os = new FileOutputStream(outputFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (InterruptedException ignored) {
        }
    }

    public void createBucket(String bucketName) {
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
    }

    public SdkIterable<S3Object> listObjectsInBucket(String bucketName) {
        // Build the list objects request
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();

        ListObjectsV2Iterable listRes = null;
        try {
            listRes = s3.listObjectsV2Paginator(listReq);
        } catch (InterruptedException ignored) {
        }
        // Process response pages
        listRes.stream()
                .flatMap(r -> r.contents().stream())
                .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));

        return listRes.contents();
    }

    public void deleteEmptyBucket(String bucketName) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
        try {
            s3.deleteBucket(deleteBucketRequest);
        } catch (InterruptedException ignored) {
        }
    }

    public void deleteAllObjectsFromBucket(String bucketName) {
        SdkIterable<S3Object> contents = listObjectsInBucket(bucketName);

        Collection<ObjectIdentifier> keys = contents.stream()
                .map(content ->
                        ObjectIdentifier.builder()
                                .key(content.key())
                                .build())
                .toList();

        Delete del = Delete.builder().objects(keys).build();

        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(del)
                .build();

        try {
            s3.deleteObjects(multiObjectDeleteRequest);
        } catch (InterruptedException ignored) {
        }
    }

    public void deleteBucket(String bucketName) {
        deleteAllObjectsFromBucket(bucketName);
        deleteEmptyBucket(bucketName);
    }

    //////////////////////////////////////////////SQS

    /**
     * @param queueName
     * @return queueUrl
     */
    public String createQueue(String queueName) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse create_result = null;
        try {
            create_result = sqs.createQueue(request);
        } catch (InterruptedException ignored) {
        }

        assert create_result != null;
        String queueUrl = create_result.queueUrl();
        System.out.println("Created queue '" + queueName + "', queue URL: " + queueUrl);
        return queueUrl;
    }

    public void deleteQueue(String queueUrl) {
        DeleteQueueRequest req =
                DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();

        try {
            sqs.deleteQueue(req);
        } catch (InterruptedException ignored) {
        }
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = null;
        try {
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (InterruptedException ignored) {
        }
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
        try {
            queueAttributesResponse = sqs.getQueueAttributes(getQueueAttributesRequest);
        } catch (InterruptedException ignored) {
        }
        Map<QueueAttributeName, String> attributes = queueAttributesResponse.attributes();

        return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)) +
                Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
    }


    ///////////////////////

    public enum Label {
        Manager,
        Worker
    }
}
