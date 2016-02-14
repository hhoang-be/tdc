package com.crawler.common;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.RebootInstancesRequest;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class AdministrationTasks {
//	static private String accessKey = "AKIAIAGBUP4ZO6G343CA";
//	static private String secretKey = "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";
	static private long WAIT_FOR_TRANSITION_INTERVAL = 20000;
	List<Instance> activeInstances;
	
	static public String accessKey;				//public key to access amazon ws
	static public String secretKey;				//secret key to access amazon ws
	static public String keyLocation;			//location of key file to access amazon ws
	static public String downloaderPath;		//location of downloader jar file
	static public String managerPath;			//location of manager jar file
	static public String readyBucket;			//name of ready bucket
	static public String doneBucket;			//name of done bucket
	static public String managerToDownloader;	//name of SQS from manager to downloader
	static public String applicationToManager;	//name of SQS from application to manager
	static public int downloadSpeed;			//URL download speed. Number of URL per second
	static public int numberOfDownloaders;		//number of downloaders
	static public int queueLength;				//default length of SQS
	
	public AdministrationTasks() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Load configuration parameters into attributes of this class
	 * @param: path to configuration file
	 * @return:
	 */
	public void loadConfiguration(String filePath) {
		File xml = new File(filePath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xml);
			doc.getDocumentElement().normalize();
			NodeList nodes = doc.getElementsByTagName("runningparameters");
			for (int i = 0; i < nodes.getLength(); i++) {
				Node node = nodes.item(i);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					AdministrationTasks.accessKey = getTagValue("accesskey", element);
					AdministrationTasks.secretKey = getTagValue("secretkey", element);
					AdministrationTasks.keyLocation = getTagValue("keyfile", element);
					AdministrationTasks.readyBucket = getTagValue("readybucket", element);
					AdministrationTasks.doneBucket = getTagValue("donebucket", element);
					AdministrationTasks.managerToDownloader = getTagValue("managertodownloader", element);
					AdministrationTasks.applicationToManager = getTagValue("applicationtomanager", element);
					AdministrationTasks.downloaderPath = getTagValue("downloader", element);
					AdministrationTasks.managerPath = getTagValue("manager", element);
					AdministrationTasks.queueLength = Integer.parseInt(getTagValue("queuelength", element));
					AdministrationTasks.numberOfDownloaders = Integer.parseInt(getTagValue("numberofdownloader", element));
					AdministrationTasks.downloadSpeed = Integer.parseInt(getTagValue("downloadspeed", element));
				}
			}
		} catch (Exception e){
			e.printStackTrace();
			System.out.println("XML file format is not correct. Please check.");
			
		}
	}
	
	/**
	 * Get value of XML element
	 * @param: tag, element
	 * @return: element value
	 */
	private static String getTagValue(String sTag, Element eElement) {
		NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
		Node nValue = (Node) nlList.item(0);
		return nValue.getNodeValue();
	}
	
	/**
	 * Create a number of new EC2 instances
	 * @param: AmazonEC2 object, number of instances needed
	 * @return: a list containing all instances created.
	 */
	public List<Instance> createEC2Instance(AmazonEC2 ec2, int noOfInstances) {
		try {
			AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
			AmazonEC2Client amazonEC2Client = new AmazonEC2Client(myCredentials);
			RunInstancesRequest runInstancesRequest =
													new RunInstancesRequest();
													runInstancesRequest.withImageId("ami-1624987f")
													.withInstanceType("t1.micro")
													.withMinCount(noOfInstances)
													.withMaxCount(noOfInstances)
													.withKeyName("tdcproject")
													.withSecurityGroups("quick-start-1");
			RunInstancesResult runInstances = amazonEC2Client.runInstances(runInstancesRequest);
			List<Instance> instances = runInstances.getReservation().getInstances();
			return instances;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	/*
	 * Start an existing instance
	 * @param: instance ID, amazonEC2 object
	 * @return: state of instance after this method
	 */
	public String startInstance(final String instanceId, AmazonEC2 ec2) throws AmazonServiceException,
			AmazonClientException, InterruptedException {
		// Stop the instance
		StartInstancesRequest startRequest = new StartInstancesRequest().withInstanceIds(instanceId);
		StartInstancesResult startResult = ec2.startInstances(startRequest);
		List<InstanceStateChange> stateChangeList = startResult.getStartingInstances();
		System.out.println("Starting instance '" + instanceId + "':");

		// Wait for the instance to be stopped
		return waitForTransitionCompletion(stateChangeList, "running", ec2, instanceId);
	}

	/**
	 * Start an existing instance
	 * @param: instance ID, amazonEC2 object
	 * @return: state of instance after this method
	 */
	
	public String stopInstance(final String instanceId, final Boolean forceStop, AmazonEC2 ec2) 
				throws AmazonServiceException, AmazonClientException, InterruptedException {
		// Stop the instance
		StopInstancesRequest stopRequest = new StopInstancesRequest().withInstanceIds(instanceId).withForce(forceStop);
		StopInstancesResult startResult = ec2.stopInstances(stopRequest);
		List<InstanceStateChange> stateChangeList = startResult.getStoppingInstances();
		System.out.println("Stopping instance '" + instanceId + "':");

		// Wait for the instance to be stopped
		return waitForTransitionCompletion(stateChangeList, "stopped", ec2,	instanceId);
	}

	/**
	 * Reboot an instance
	 * @param: instance ID, amazonEC2 object
	 * @return
	 */
	public void rebootInstance(final String instanceId, AmazonEC2 ec2) 
					throws AmazonServiceException,	AmazonClientException {
		// Reboot the instance
		RebootInstancesRequest rebootRequest = new RebootInstancesRequest().withInstanceIds(instanceId);
		System.out.println("Rebooting instance '" + instanceId + "'");
		ec2.rebootInstances(rebootRequest);
	}

	/**
	 * This function is just to wait until instance change to desired state
	 * @param: list of instance states, desired state, amazonEC2 object and instance ID
	 * @return: current state of instance
	 */
	public final String waitForTransitionCompletion(List<InstanceStateChange> stateChangeList,
				final String desiredState, AmazonEC2 instancebuilder, String instanceId)
				throws InterruptedException {
		Boolean transitionCompleted = false;
		InstanceStateChange stateChange = stateChangeList.get(0);
		String previousState = stateChange.getPreviousState().getName();
		String currentState = stateChange.getCurrentState().getName();
		while (!transitionCompleted) {
			try {
				Instance instance = AdministrationTasks.describeInstance(instancebuilder, instanceId);
				currentState = instance.getState().getName();
				if (previousState.equals(currentState)) {
					System.out.println("... '" + instanceId + "' is still in state " + currentState + " ...");
				} else {
					System.out.println("... '" + instanceId + "' entered state " + currentState + " ...");
					instance.getStateTransitionReason();
				}
				previousState = currentState;

				if (currentState.equals(desiredState)) {
					transitionCompleted = true;
				}
			} catch (AmazonServiceException ase) {
				System.out.println("Failed to describe instance '" + instanceId + "'!");
			}

			// Sleep for WAIT_FOR_TRANSITION_INTERVAL seconds until transition
			// has completed.
			if (!transitionCompleted) {
				Thread.sleep(WAIT_FOR_TRANSITION_INTERVAL);
			}
		}
		return currentState;
	}
	
	/*
	 * Looking for an instance with given ID
	 * @param: amazonEC2 objec, instance ID
	 * @return: Instance with given ID
	 */
	public static Instance describeInstance(AmazonEC2 instancebuilder, String instanceId) 
				throws AmazonServiceException, AmazonClientException {
		DescribeInstancesRequest describeRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
		DescribeInstancesResult result = instancebuilder.describeInstances(describeRequest);
		for (Reservation reservation : result.getReservations()) {
			for (Instance instance : reservation.getInstances()) {
				if (instanceId.equals(instance.getInstanceId())) {
					return instance;
				}
			}
		}
		return null;
	}
	

	/*
	 * Looking for an instance with given ID
	 * @param: instance ID
	 * @return: Instance with given ID
	 */

	public Instance getInstances(String instanceID) {
		AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
		AmazonEC2Client amazonEC2Client = new AmazonEC2Client(myCredentials);
		DescribeInstancesRequest describeRequest = new DescribeInstancesRequest();
		DescribeInstancesResult result = amazonEC2Client.describeInstances(describeRequest);
		for (Reservation reservation : result.getReservations()) {
			for (Instance instance : reservation.getInstances()) {
				if (instanceID.equals(instance.getInstanceId())) {
					return instance;
				}
			}
		}
		return null;
	}
	
	/*
	 * Create a new SQS, if that queue name exists, return that existing SQS
	 * @param: AmazonSQSClient, queue name
	 * @return: URL to queue
	 */
	public String createQueue(AmazonSQSClient sqs, String queueName) {
		for (String tempQueueURL : sqs.listQueues().getQueueUrls()) {
			if (tempQueueURL.endsWith(queueName)) {
				System.out.println("Queue " + queueName + "already exists.");
				return tempQueueURL;
			}
		}
		CreateQueueRequest queueRequest = new CreateQueueRequest().withQueueName(queueName);
		String myQueueURL = sqs.createQueue(queueRequest).getQueueUrl();
		return myQueueURL;
	}
	
	/*
	 * Create a bucket. If that bucke exits then return name of that existing bucket
	 * @param: AmazonS3client object, bucket name
	 * @return: bucket object
	 */
	public Bucket createBucket(AmazonS3Client s3, String bucketName) {
		for (Bucket tempBucket : s3.listBuckets()) {
			if (bucketName.equals(tempBucket.getName())) {
				return tempBucket;
			}
		}
		CreateBucketRequest bucketRequest = new CreateBucketRequest(bucketName);
		Bucket createdBucket = s3.createBucket(bucketRequest);
		return createdBucket;
	}
	
	/*
	 * Remove bucket
	 * @parame: AmazonS3client object, bucket name
	 * @return
	 */
	public void removeBucket(AmazonS3Client s3, String bucketName) {
		for (Bucket tempBucket : s3.listBuckets()) {
			if (bucketName.equals(tempBucket.getName())) {
				s3.deleteBucket(bucketName);
				return;
			}
		}
	}	
	
	/*
	 * Execute a shell command at remote computer over ssh protocol
	 * @param: EC2 instance where command is executed, command and path to key file (.pem file)
	 * @return
	 */
	public void executeCommandOnInstance(Instance instance, String command, String keyFileLocation) {
//		String privateDNSName = instance.getPrivateDnsName();
		String publicDNSName = instance.getPublicDnsName();

			String fullCommand = "ssh -i " + keyFileLocation + " ec2-user@" + publicDNSName + " " + command;
			Process p;
			try {
				p = Runtime.getRuntime().exec(fullCommand);
				p.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
	}
	
	
	public void deployCrawler() throws Exception {
/*		AmazonEC2 ec2 = CommonMethods.getEC2ClientObject();
		AmazonS3Client s3 = CommonMethods.getS3ClientObject();
		AmazonSQSClient sqs = CommonMethods.getSQSClientObject();
		activeInstances = createEC2Instance(ec2, 1);
		for (Instance instance : activeInstances) {
			startInstance(instance.getInstanceId(), ec2);
			System.out.println("Hello: " + instance.getInstanceId());
			
		}
		 startInstance("i-64f45c1a", ec2);
		 Instance in = getInstances("i-64f45c1a");
				 executeCommandOnInstance(in, "mkdir hello", "/home/hieuht/Downloads/tdcproject.pem");
		createBucket(s3, "tdcready2");
		createBucket(s3, "tdcdone2");
		createQueue(sqs, "ManagerToDownloader");
		createQueue(sqs, "ApplicationToManager");*/
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AdministrationTasks admin = new AdministrationTasks();
		admin.loadConfiguration("/home/hieuht/Eclipse/workspaceJEE/tdc/res/config.xml");
//		try {
//			admin.deployCrawler();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
