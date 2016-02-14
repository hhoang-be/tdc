package com.crawler.application;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.crawler.common.CommonMethods;

public class TestFramework {
	
	
	
	public TestFramework()
	{
		 listBuckets();
		 listSQS_queues();
		 listSQS_ApplicationToManager();
		 listS3_TDCReady();
		 listS3_TDCDone();
		 //===listS3ReadyKeys_SQS();
		 //===recreate_S3ReadyKeys_SQS();
	}
	
	
	//list all buckets in your s3
	public void listBuckets()
	{
		System.out.println("Listing buckets:");
		AmazonS3Client s3client = CommonMethods.getS3ClientObject();
        for (Bucket bucket : s3client.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }
        System.out.println();
        System.out.println();
	}
	
	
	//list all queues
	public void listSQS_queues()
	{
		// List queues
		AmazonSQSClient sqsclient = CommonMethods.getSQSClientObject();
        System.out.println("Listing all queues in your account:");
        for (String queueUrl : sqsclient.listQueues().getQueueUrls()) {
            System.out.println(" - QueueUrl: " + queueUrl);
        }
        System.out.println();
        System.out.println();
	}
	
	
	//list the URLs within ApplicationToManager SQS
	public void listSQS_ApplicationToManager()
	{
		System.out.println("Listing ApplicationToManager SQS:");
		AmazonSQSClient sqsclient = CommonMethods.getSQSClientObject();
		ReceiveMessageRequest _rec_msg_req = new ReceiveMessageRequest(CommonMethods.ApplicationToManager);//"https://queue.amazonaws.com/624269346392/DownloadQueue");
		_rec_msg_req.setVisibilityTimeout(0);
		List<Message> _messages = (List<Message>) sqsclient.receiveMessage(_rec_msg_req).getMessages();
		for(Message _msg : _messages)
		{
			System.out.println(" - " + _msg.getBody());
		}
		System.out.println();
		System.out.println();
	}
	
	//listing the items inside TDCReady
	public void listS3_TDCReady()
	{
		AmazonS3Client s3client = CommonMethods.getS3ClientObject();
		System.out.println("Listing objects in TDCReady:");
        ObjectListing objectListing = s3client.listObjects(new ListObjectsRequest()
                .withBucketName(CommonMethods.ReadyBucket));
        ArrayList<String> _keys = new ArrayList<String>();
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() + ")");
            //_keys.add(objectSummary.getKey());
        }
        //CommonMethods.sendMessageToSQS_S3ReadyKeys(CommonMethods.S3ReadyKeys, _keys);
        System.out.println();
        System.out.println();
	}
	
	
	//listing the items inside TDCDone
	public void listS3_TDCDone()
	{
		AmazonS3Client s3client = CommonMethods.getS3ClientObject();
		System.out.println("Listing objects in TDCDone:");
        ObjectListing objectListing = s3client.listObjects(new ListObjectsRequest()
                .withBucketName(CommonMethods.DoneBucket));
        ArrayList<String> _keys = new ArrayList<String>();
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() + ")");
            //_keys.add(objectSummary.getKey());
        }
        //CommonMethods.sendMessageToSQS_S3ReadyKeys(CommonMethods.S3ReadyKeys, _keys);
        System.out.println();
        System.out.println();
	}
	
	/*=============================================================
	public void listS3ReadyKeys_SQS()
	{
		System.out.println("Listing S3ReadyKeys:");
		AmazonSQSClient sqsclient = CommonMethods.getSQSClientObject();
		ReceiveMessageRequest _rec_msg_req = new ReceiveMessageRequest(CommonMethods.S3ReadyKeys);
		_rec_msg_req.setVisibilityTimeout(0);
		_rec_msg_req.setMaxNumberOfMessages(10);
		List<Message> _messages = (List<Message>) sqsclient.receiveMessage(_rec_msg_req).getMessages();
		for(Message _msg : _messages)
		{
			System.out.println(" - " + _msg.getBody());
			delete_message_S3ReadyKeys_SQS(sqsclient, _msg.getReceiptHandle(), "https://queue.amazonaws.com/624269346392/S3ReadyKeys");
		}
		System.out.println();
		System.out.println();
	}
	
	public void delete_message_S3ReadyKeys_SQS(AmazonSQSClient sqsclient, String receiptHandler, String URL)
	{
		sqsclient.deleteMessage(new DeleteMessageRequest(URL, receiptHandler));
	}
	
	
	//delete all messages
	public void recreate_S3ReadyKeys_SQS()
	{
		// Delete a queue
		AmazonSQSClient sqsclient = CommonMethods.getSQSClientObject();
        System.out.println("Deleting the test queue.\n");
        sqsclient.deleteQueue(new DeleteQueueRequest("https://queue.amazonaws.com/624269346392/S3ReadyKeys"));
		
        try {
			Thread.sleep(120000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		System.out.println("Creating a new SQS queue called MyQueue.\n");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("S3ReadyKeys");
        String myQueueUrl = sqsclient.createQueue(createQueueRequest).getQueueUrl();
        System.out.println(myQueueUrl);
	}
	==================================================*/
	
	

}
