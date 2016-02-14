package com.crawler.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class CommonMethods {

	static public String configFilePath = "/home/hieuht/EclipseIDE/workspaceJEE/tdc/res/config.xml";

	 static public String S3ReadyKeys = "https://queue.amazonaws.com/624269346392/S3ReadyKeys";
	 static public String ManagerToDownloader = "https://sqs.us-east-1.amazonaws.com/165392776648/ManagerToDownloader";
	 static public String ApplicationToManager = "https://sqs.us-east-1.amazonaws.com/165392776648/ApplicationToManager";
	 static public String PolicyConfiguration = "https://sqs.us-east-1.amazonaws.com/165392776648/PolicyConfiguration";
	 static public String ReadyBucket = "tdcready2";
	 static public String DoneBucket = "tdcdone2";
	 static public String HadoopBucket = "tdchadoop";
	 static public long POLLING_INTERVAL = 10000;
	 static public int queueLength = 0;
	 static public int downloadSpeed;
	 static public long TOTAL_PAGES = 10000;
	 static public double PAGE_RANK_THRESHOLD = 0.001;
	 static public long PAGE_RANK_MAX_LOOP = 10;
	 static private String accessKey = "AKIAIAGBUP4ZO6G343CA";
	 static private String secretKey = "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";
	
	static public ArrayList<String> countryFilters = new ArrayList<String>();
	static public ArrayList<String> fileTypeFilters = new ArrayList<String>();
	static public ArrayList<URL> initialSeedURLS = new ArrayList<URL>();

	public CommonMethods() {
		// TODO Auto-generated constructor stub
	}
	
	
	/**
	 * Write a number to file
	 */
	static public void writeNumberToFile(int number, String filePath) {
		try {
			// replace helloworld.txt with the name of the file
			BufferedWriter out = new BufferedWriter(new FileWriter(filePath));
			// Write out the specified string to the file
			out.write(Integer.toString(number));
			// flushes and closes the stream
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * REad a number from file
	 */
	static public int readNumberFromFile(String filePath) {
		try {
			String read;
			int returnValue;
			// open a bufferedReader to file helloworld.txt
			BufferedReader in = new BufferedReader(new FileReader(filePath));

			// read a line from helloworld.txt and save into a string
			read = in.readLine();
			if (read != null) {
				read = read.trim();
				returnValue = Integer.parseInt(read);
			} else {
				returnValue = 1;
			}
			
			// safely close the BufferedReader after use
			in.close();
			//remove content of the file
			FileOutputStream writer = new FileOutputStream(filePath);
			writer.write((new String()).getBytes());
			writer.close();
			return returnValue;
		} catch (IOException e) {
			System.out.println("There was a problem:" + e);
			return 1;
		}
	}
	
	
	public void removeAllObjectsInS3(String bucketName) {
		AmazonS3Client s3client = CommonMethods.getS3ClientObject();
		ObjectListing objListing = s3client.listObjects(bucketName);
		int i = 0;
		for (S3ObjectSummary objSummary : objListing.getObjectSummaries()) {
			i++;
			s3client.deleteObject(bucketName, objSummary.getKey());
			System.out.println("Removed " + Integer.toString(i));
		}
		
	}

	
	/**
	 * Load configuration for crawler from config file.
	 */
	public void loadConfiguration(String filePath) {
		File xml = new File(filePath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		String filterString;
		try {
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xml);
			doc.getDocumentElement().normalize();
			NodeList nodes = doc.getElementsByTagName("runningparameters");
			for (int i = 0; i < nodes.getLength(); i++) {
				Node node = nodes.item(i);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					CommonMethods.accessKey = getTagValue("accesskey", element);
					CommonMethods.secretKey = getTagValue("secretkey", element);
					CommonMethods.ReadyBucket = getTagValue("readybucketname", element);
					CommonMethods.DoneBucket = getTagValue("donebucketname", element);
					CommonMethods.ManagerToDownloader = getTagValue("managertodownloader", element);
					CommonMethods.ApplicationToManager = getTagValue("applicationtomanager", element);
					CommonMethods.queueLength = Integer.parseInt(getTagValue("queuelength", element));
					CommonMethods.downloadSpeed = Integer.parseInt(getTagValue("downloadspeed", element));
					CommonMethods.PAGE_RANK_THRESHOLD = Double.parseDouble(getTagValue("pagerankthreshold", element));
					CommonMethods.TOTAL_PAGES = Long.parseLong(getTagValue("totalpages", element));
					CommonMethods.PAGE_RANK_MAX_LOOP = Integer.parseInt(getTagValue("pagerankmaxloop", element));
					
					NodeList FilterNodes = element.getElementsByTagName("urlcountry");	//filter for country
					for (int j = 0; j < FilterNodes.getLength(); j++) {
						Node subNode = FilterNodes.item(j);
						if (subNode.getNodeType() == Node.ELEMENT_NODE) {
							filterString = subNode.getTextContent().trim();
							countryFilters.add(filterString);
						}
					}
					
					FilterNodes = element.getElementsByTagName("urlfiletype");			//filter for file type
					for (int j = 0; j < FilterNodes.getLength(); j++) {
						Node subNode = FilterNodes.item(j);
						if (subNode.getNodeType() == Node.ELEMENT_NODE) {
							filterString = subNode.getTextContent().trim();
							fileTypeFilters.add(filterString);
						}
					}
					
					FilterNodes = element.getElementsByTagName("url");			//filter for file type
					for (int j = 0; j < FilterNodes.getLength(); j++) {
						Node subNode = FilterNodes.item(j);
						if (subNode.getNodeType() == Node.ELEMENT_NODE) {
							filterString = subNode.getTextContent().trim();
							initialSeedURLS.add(new URL(filterString));
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("XML file format is not correct. Please check.");
		}
	}

	/**
	 * Get value of XML element
	 * 
	 * @param: tag, element
	 * @return: element value
	 */
	private static String getTagValue(String sTag, Element eElement) {
		NodeList nlList = eElement.getElementsByTagName(sTag).item(0)
				.getChildNodes();
		Node nValue = (Node) nlList.item(0);
		return nValue.getNodeValue().trim();
	}

	static public AmazonEC2Client getEC2ClientObject() {
		AWSCredentials myCredentials = new BasicAWSCredentials(accessKey,
				secretKey);
		AmazonEC2Client client = new AmazonEC2Client(myCredentials);
		return client;
	}

	static public AmazonS3Client getS3ClientObject() {
		AWSCredentials myCredentials = new BasicAWSCredentials(accessKey,
				secretKey);
		AmazonS3Client client = new AmazonS3Client(myCredentials);
		return client;
	}

	static public AmazonSQSClient getSQSClientObject() {
		AWSCredentials myCredentials = new BasicAWSCredentials(accessKey,
				secretKey);
		AmazonSQSClient client = new AmazonSQSClient(myCredentials);
		return client;
	}

	static public AmazonDynamoDBClient getDynamoDBObject() {
		AWSCredentials myCredentials = new BasicAWSCredentials(accessKey, secretKey);
//		Set the maximum retries when write to DynamoDB
//		ClientConfiguration config = new ClientConfiguration();
//		config.setMaxErrorRetry(0);
//		AmazonDynamoDBClient client = new AmazonDynamoDBClient(myCredentials, config);
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(myCredentials);
		return client;
	}

	static public void sendMessageToSQS(AmazonSQSClient _sqsClient,
			String _sqsQueueURL, ArrayList<URL> _sqsListOfURLs) {
		if (_sqsListOfURLs != null) {
			for (URL _url : _sqsListOfURLs) {
				_sqsClient.sendMessage(new SendMessageRequest().withQueueUrl(
						_sqsQueueURL).withMessageBody(_url.toString()));
				System.out.println(_url.toString());
			}
			
		} else {
			System.out.println("<sendMessageFromSQS> client SQS is null.");
		}
	}

	
	static public ArrayList<URL> receiveMessageFromSQS(
			AmazonSQSClient _sqsClient, String _sqsQueueURL,
			int _numberOfMessages) {
		if (_sqsClient != null) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					_sqsQueueURL);
			ArrayList<URL> returnURLList = new ArrayList<URL>();
			receiveMessageRequest.setMaxNumberOfMessages(_numberOfMessages);
			List<Message> messages = _sqsClient.receiveMessage(
					receiveMessageRequest).getMessages();
			for (Message message : messages) {
				String receiptHandler = message.getReceiptHandle();
				String messageBody = message.getBody();
				try {
					URL tempURL = new URL(messageBody);
					// Add new URL to return list
					returnURLList.add(tempURL);
					// remove it from queue
					_sqsClient.deleteMessage(new DeleteMessageRequest()
							.withQueueUrl(_sqsQueueURL).withReceiptHandle(
									receiptHandler));
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					System.out.println(messageBody + " is not a valid URL");
					e.printStackTrace();
				}
			}
			return returnURLList;
		} else {
			System.out.println("<receiveMessageFromSQS> client SQS is null.");
			return null;
		}
	}

	static public String receiveStringFromSQS(AmazonSQSClient _sqsClient,
			String _sqsQueueURL, int _numberOfMessages) {
		String messageBody = null;
		if (_sqsClient != null) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					_sqsQueueURL);
			receiveMessageRequest.setMaxNumberOfMessages(_numberOfMessages);
			List<Message> messages = _sqsClient.receiveMessage(
					receiveMessageRequest).getMessages();
			for (Message message : messages) {
				String receiptHandler = message.getReceiptHandle();
				messageBody = message.getBody();
				// remove it from queue
				_sqsClient.deleteMessage(new DeleteMessageRequest()
						.withQueueUrl(_sqsQueueURL).withReceiptHandle(
								receiptHandler));
			}
			return messageBody;
		} else {
			return null;
		}
	}
	
	
	static public String receiveStringFromSQSNoRemove(AmazonSQSClient _sqsClient,
			String _sqsQueueURL, int _numberOfMessages) {
		String messageBody = null;
		if (_sqsClient != null) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(_sqsQueueURL);
			receiveMessageRequest.setMaxNumberOfMessages(_numberOfMessages);
			List<Message> messages = _sqsClient.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				messageBody = message.getBody();
			}
			return messageBody;
		} else {
			return null;
		}
	}
	
	
	public void clearSQSQueue(AmazonSQSClient _sqsClient,String _sqsQueueURL, int _numberOfMessages) {
		if (_sqsClient != null) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(_sqsQueueURL);
			receiveMessageRequest.setMaxNumberOfMessages(_numberOfMessages);
			List<Message> messages = _sqsClient.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				String receiptHandler = message.getReceiptHandle();
				// remove it from queue
				_sqsClient.deleteMessage(new DeleteMessageRequest().withQueueUrl(_sqsQueueURL).withReceiptHandle(receiptHandler));
			}
		}
	}

	public static void main(String[] args) {
		CommonMethods test = new CommonMethods();
		test.loadConfiguration(CommonMethods.configFilePath);
//		AmazonSQSClient sqsClient = CommonMethods.getSQSClientObject();
//		sqsClient.deleteMessageBatch(new DeleteMessageBatchRequest().withQueueUrl(CommonMethods.ManagerToDownloader));
//		test.removeAllObjectsInS3("tdcdone2");
		test.removeAllObjectsInS3("tdcready2");
//		test.extractPageTitle("");
	}
	
	
	public void extractPageTitle(String url) {
		String regex = "(.*)(\\<title\\>)(([^\\>\\<]+))(\\</title\\>)(.*)";
		String title = "";
		String line = "";
		Pattern patternTitle = Pattern.compile(regex);
		Matcher matcherTitle;
		AmazonS3Client s3Client = CommonMethods.getS3ClientObject();
		S3Object object = s3Client.getObject(new GetObjectRequest(CommonMethods.DoneBucket, "http:!!!!actualite.fr.be.msn.com!!finance!!"));
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent(), "UTF-8"));
			while ((line = reader.readLine()) != null) {
				matcherTitle = patternTitle.matcher(line);
				if (matcherTitle.find()) {
					title = matcherTitle.group(3);
					System.out.println(title);
					break;
				}
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ====================================================
	 * 
	 * @param _sqsQueueURL
	 *            S3ReadyKeys URL
	 * @param _keys
	 *            keys corresponding to the files in TDCReady
	 * 
	 *            static public void sendMessageToSQS_S3ReadyKeys(String
	 *            _sqsQueueURL, ArrayList<String> _keys) { AmazonSQSClient
	 *            _sqsClient = CommonMethods.getSQSClientObject(); if (_keys !=
	 *            null) { for (String _key : _keys) { _sqsClient.sendMessage(new
	 *            SendMessageRequest() .withQueueUrl(_sqsQueueURL)
	 *            .withMessageBody(_key)); }
	 * 
	 *            } }=======================================================
	 */

	/**
	 * =====================================================
	 * 
	 * @param _sqsClient
	 *            SQS client
	 * @param _sqsQueueURL
	 *            Amazon S3readyKeys SQS queue
	 * @param _numberOfMessages
	 *            number of messages.
	 * @return keys of the files inside TDCReady bucket static public
	 *         ArrayList<String>
	 *         receiveMessageFromSQS_S3ReadyKeys(AmazonSQSClient _sqsClient,
	 *         String _sqsQueueURL, int _numberOfMessages) { if (_sqsClient !=
	 *         null) { ReceiveMessageRequest receiveMessageRequest = new
	 *         ReceiveMessageRequest(_sqsQueueURL); ArrayList<String>
	 *         returnFileKeys = new ArrayList<String>();
	 *         receiveMessageRequest.setMaxNumberOfMessages(_numberOfMessages);
	 *         List<Message> messages =
	 *         _sqsClient.receiveMessage(receiveMessageRequest).getMessages();
	 *         for (Message message : messages) { String receiptHandler =
	 *         message.getReceiptHandle(); String messageBody =
	 *         message.getBody(); String tempKey = new String(messageBody);
	 *         //Add new Key to return list returnFileKeys.add(tempKey);
	 *         //remove it from queue _sqsClient.deleteMessage( new
	 *         DeleteMessageRequest() .withQueueUrl(_sqsQueueURL)
	 *         .withReceiptHandle(receiptHandler)); } return returnFileKeys; }
	 *         else { System.out.println(
	 *         "<receiveMessageFromSQS> client SQS is null (Method: receiveMessageFromSQS_S3ReadyKeys)."
	 *         ); return null; }
	 *         }=======================================================
	 */
}
