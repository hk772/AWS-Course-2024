package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AWSApp {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 2;
    private static String region = "us-west-2";
//    private static String region = "us-east-1";
    public static String bucketName = "my-bucket-mevuzarot-ass2-asd";
    private static String jarName = "ass3.jar";
//    private static String jarName = "ass3local.jar";
    public static boolean isLocal = false;
    public static boolean useCustomNgrams = false;
    public enum Percentage {
        onePercent,
        tenPercent,
        fullCorpus
    }
    public static Percentage corpusPercentage = Percentage.onePercent; // swap corpus percentage here (1% / 10% / 100%)
    public static final int NUM_CORPUS_FILES = 99;

    public static String baseURL = "s3://" + bucketName;
    public static String ngram_prefix = "http://commondatastorage.googleapis.com/books/syntactic-ngrams/eng/biarcs.{"; //s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    public static String ngram_suffix = "}-of-99.txt"; // "}-of-99.gz"; // + "XX" in between
    // for i in $(seq -w 0 98); do   curl "http://commondatastorage.googleapis.com/books/syntactic-ngrams/eng/biarcs.${i}-of-99.gz" | gunzip | aws s3 cp - s3://my-bucket-mevuzarot-ass3-asd/input/biarcs.${i}-of-99.txt; done

    public static final String Corpus1Percent = AWSApp.baseURL + "/input/biarcs.0-of-99.txt";


    public static void main(String[]args){
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Connecting to aws");
//        System.out.println("ACCESS_KEY: " + credentialsProvider.getCredentials().getAWSAccessKeyId());
//        System.out.println("SECRET_KEY: " + credentialsProvider.getCredentials().getAWSSecretKey());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        System.out.println("list cluster");
        System.out.println(emr.listClusters());


//        // Step 0
//        HadoopJarStepConfig step0 = new HadoopJarStepConfig()
//                .withJar("s3://" + bucketName + "/" + jarName)
//                .withMainClass("org.example.TotalWordCount");
//
//        StepConfig stepConfig0 = new StepConfig()
//                .withName("org.example.TotalWordCount")
//                .withHadoopJarStep(step0)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("org.example.Job1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("org.example.Job1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("org.example.Job2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("org.example.Job2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("org.example.Job3");

        StepConfig stepConfig3 = new StepConfig()
                .withName("org.example.Job3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("3.4.0")
//                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(region + "a"));

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Map reduce project")
                .withInstances(instances)
//                .withSteps(stepConfig0)
                .withSteps(stepConfig1, stepConfig2, stepConfig3)
//                .withSteps(stepConfig3, stepConfig4)
                .withLogUri("s3://" + bucketName + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
