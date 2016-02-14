package com.crawler.downloader;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.crawler.common.CommonMethods;


public class Downloader implements Runnable {
	
	private String bucketName;								//S3 bucket name
	//================private String keysQueueURL;			//URL to keys queue
	private String queueURLFromManager;						//URL for queue from Manager to Downloader
	private int numberOfConnections;						//size of thread pool
	private ExecutorService pool;							//Thread pool to download webpage
	private int maxNumberOfMessages;						//maximum number of messages per fetch from SQS
	private AmazonSQSClient clientSQS = null;				//Amazon client of SQS
	private AmazonS3Client clientS3 = null;					//Amazon client for S3
	private boolean serviceIsRunning;
	private int downloadSpeed;								//download speed: pages per second

	
	public Downloader() {
		// TODO Auto-generated constructor stub
	}
	
	
	Downloader(String _bucketName, String _queuURLFromManager, int _maxNoOfMessages, int _downloadSpeed) {
		this.bucketName = _bucketName;
		//==============this.keysQueueURL = _queueURLKeysS3;
		serviceIsRunning = true;
		this.queueURLFromManager = _queuURLFromManager;
		this.maxNumberOfMessages = _maxNoOfMessages;
		this.downloadSpeed = _downloadSpeed;
		this.clientSQS = CommonMethods.getSQSClientObject();
		this.clientS3 = CommonMethods.getS3ClientObject();
		this.numberOfConnections = 1;
	}
	
	
	/**
	 * Set download speed in pages per second 
	 * @param: speed
	 * @return: void
	 */
	
	public void setDownloadSpeed(int _downloadSpeed) {
		this.downloadSpeed = _downloadSpeed;
	}
	
	/**
	 * Wait until all threads finish and shut down thread pool.
	 * Do not allow any new thread after call this function.
	 * @param:
	 * @return:
	 */
	public void shutDownThreadPool() {
		if ((pool != null) && (!pool.isShutdown())) {
			pool.shutdown();
		}
	}

	
	
	public void processURLsFromSQS() throws Exception {
		if (this.numberOfConnections > 0) {
			while (true) {
				pool = Executors.newFixedThreadPool(numberOfConnections);
				Set<Future<Integer>> set = new HashSet<Future<Integer>>();
				for (int i = 1; i <= numberOfConnections; i++) {
					Callable<Integer> callable = new DownloaderInstance();
					if (((DownloaderInstance)callable).getNumberOfURLinQueue() > 0) {
						Future<Integer> future = pool.submit(callable);
						set.add(future);
					}
				}
				int i = 0;
				double speedInThread = 0;	//pages per second in each thread
				int realSpeed = 0;			//pages per second
				/*Compute average download speed of all threads*/
				for (Future<Integer> future : set) {
					speedInThread = future.get();
					realSpeed = (int)(realSpeed*i + speedInThread)/(i+1);
					i++;
				}
				/*If download speed is lower than specified then increase number of connection*/
				if ((realSpeed < downloadSpeed) && (realSpeed != 0)) {
					this.numberOfConnections = (int) (this.numberOfConnections * downloadSpeed/realSpeed);
				}
			} 
		} 
	}

	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (serviceIsRunning) {
			try {
				processURLsFromSQS();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	class DownloaderInstance implements Callable {
		
		private ArrayList<URL> subFeededURLs;
		
		public DownloaderInstance() {
			// TODO Auto-generated constructor stub
			subFeededURLs = CommonMethods.receiveMessageFromSQS(clientSQS, queueURLFromManager, maxNumberOfMessages);
		}
		
		public int getNumberOfURLinQueue() {
			if (subFeededURLs != null) {
				return subFeededURLs.size();
			} else {
				return -1;
			}
		}
	
		
		private synchronized int DownloadURLToS3() {
			URL currentDownloadURL;
			try {
				if (subFeededURLs != null) {
					long startTime = System.currentTimeMillis();
					for (int i = 0; i < subFeededURLs.size(); i++) {
						currentDownloadURL = subFeededURLs.get(i);
						//create unique key for files stored in S3 bucket
						String key = currentDownloadURL.toString().replaceAll("/", "!!");
						InputStream inputStream = currentDownloadURL.openStream();
						/*
						 * In this case, we don't specify the content length of input stream to S3, so it will be stored in main memory
						 * an could result in memory overload. In order to avoid this, we can first store web page to a file in local 
						 * disk and then upload that file to S3
						 */
						PutObjectRequest request = new PutObjectRequest(bucketName, key, inputStream, new ObjectMetadata());
						clientS3.putObject(request);		// put downloaded file to S3
						//===========clientSQS.sendMessage(new SendMessageRequest()			// put key to SQS
						//===========						.withQueueUrl(this.queueURL)
						//===========						.withMessageBody(key));
						inputStream.close();
					}
					long endTime = System.currentTimeMillis();
					if (subFeededURLs.size() != 0) {
						return (int) ((subFeededURLs.size())/(endTime - startTime)*1000);	//return download speed of this thread. Page/second
					} else {				//there is no URL in queue
						return 0;
					}
				}
			} catch (FileNotFoundException e1) {
				System.out.println("Webpage is unavailable.");
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			return 0;
		}

		@Override
		public Integer call() throws Exception {
			// TODO Auto-generated method stub
			return Integer.valueOf(DownloadURLToS3());
		}
	}

}
