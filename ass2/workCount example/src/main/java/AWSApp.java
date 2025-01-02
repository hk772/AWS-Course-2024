import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class AWSApp {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    public static int numberOfInstances = 3;
    public static String region = "us-west-2";
//    public static String region = "us-east-1";
    public static String bucketName = "my-bucket-mevuzarot-2024-ass2";
    private static String jarName = "Ass2.jar";
    public static boolean use_demo_3gram = false;

    public static String baseURL = "s3a://" + bucketName;
    public static String s3_3gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";


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

        // Step 0
        HadoopJarStepConfig step0 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("Job0");

        StepConfig stepConfig0 = new StepConfig()
                .withName("Job0")
                .withHadoopJarStep(step0)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("Job1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Job1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("Job2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Job2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("Job3");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Job3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 4
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/" + jarName)
                .withMainClass("Job4");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Job4")
                .withHadoopJarStep(step4)
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
                .withSteps(stepConfig0, stepConfig1, stepConfig2, stepConfig3, stepConfig4)
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
