package com.crawler.application;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.crawler.application.parser.HtmlLink;
import com.crawler.common.CommonMethods;

/**
 * 
 * @author Emad Heydari beni
 *
 * Checking the S3 Ready Bucket and do pull the files
 * After finishing move them to Done Bucket
 * and extracting the URLs within HTML file
 * then, save them in memory and put unvisited URLs
 * in the DownloadQueue
 */
public class Thread_S3_Parsing implements Runnable{

	
	//================================
	String _bucketReadyName;
	String _bucketDoneName;
	String _SQS_S3ReadyKeys_URL;
	String _SQS_unvisited_URLS; //download url
	ArrayList<String> _memory_Keys_list;
	Parsing _parsing;
	private AmazonSQSClient _clientSQS = null;
	//================================
	
	/**
	 * 
	 * @param bucketReadyName TDCReady
	 * @param bucketDoneName TDCDone
	 * @param SQS_Univisited_URLs ApplicationToManager SQS
	 */
	public Thread_S3_Parsing(String bucketReadyName, String bucketDoneName
			, String SQS_Univisited_URLs) {
		this._bucketDoneName = bucketDoneName;
		this._bucketReadyName = bucketReadyName;
		//===============this._SQS_S3ReadyKeys_URL = SQS_S3ReadyKeys_URL;
		this._SQS_unvisited_URLS = SQS_Univisited_URLs;
		this._clientSQS = CommonMethods.getSQSClientObject();
		
		//parsing initialization
		_parsing = new Parsing();
	}
	
	/**
	 * 1. Read file from READYbucket and put it to DONEbucket
	 * 2. Parse the links within file and save the links into memory (previously visited links dropped)
	 * 3. put links into the SQS queue.
	 */
	@Override
	public void run() {
		while(true)
		{
			/*
			 * retrieving keys from TDCReady in do-while loop.
			 *   Gets the list of object summaries describing the
			 *   objects stored in the S3 bucket. Listings for
			 *   large buckets can be truncated for performance
			 *   reasons. Always check the ObjectListing.isTruncated()
			 *   method to see if the returned listing is complete or
			 *   if additional calls are needed to get more results.
			 *   Callers might need to make additional calls to
			 *   AmazonS3.listNextBatchOfObjects(ObjectListing)
			 *   to get additional results.
			 */
			AmazonS3Client s3client = CommonMethods.getS3ClientObject();
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(CommonMethods.ReadyBucket);
			ObjectListing objectListing;
			
			do{
				objectListing = s3client.listObjects(listObjectsRequest);
				
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    String key = objectSummary.getKey();
                    
                    //read file from TDCReady
                    //Then, move it to TDCDone
                    S3Object __FileObject = readFileFromS3_readyBucket_moveTheFileTo_doneBucket(key);
                    //reading the file content
                    //Parsing the links
                    //save links to the memory (previously visited links dropped)
                    //send new unvisited-links to ApplicationToManager SQS
                    if(__FileObject != null)
        			{
        				S3ObjectInputStream __objectContentStream = __FileObject.getObjectContent();
        				String __fileData = getFileData((InputStream)__objectContentStream);
        				
        				if(__fileData != null)
        				{
        					List<HtmlLink> __links_all_relatives = _parsing.parse_extract_URLs(__fileData);
        					String Key_replacedSlashes = key;
        					List<HtmlLink> __links_all = relateiveToAbsuluteURL(__links_all_relatives, Key_replacedSlashes.replaceAll("!!", "/"));
        					if(__links_all.size() != 0 || __links_all!=null)
        					{
        						ArrayList<URL> __links_univisited = _parsing.saveExraceted_URLs(__links_all);
        						//send links to SQS
        						CommonMethods.sendMessageToSQS(_clientSQS , _SQS_unvisited_URLS , __links_univisited);
//        						System.out.println("["+ __links_univisited.size() + "] has been added to ApplicationToManager.");
        					}
        					else
        						System.out.println("No links found inside the HTML file. (or found previously visited-URLs)");
        				}
        				else
        					System.out.println("File data is null.");
        			}
        			else
        				System.out.println("No file in TDCReady");
                }
				
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			}while(objectListing.isTruncated());
				
			
		}
	}
	
	
	/**
	 * =====1. read a key from the SQS queue S3ReadyKeys and 
	 * =====	  add this key to array list in memory for future use.
	 * 2. with that key, download the file from the TDCReady
	 * 3. copy the file from ReadyBucket to DoneBucket.
	 * 4. delete the file from TDCReady 
	 * (if there is no file NULL will be returned.)
	 * 
	 * 
	 * @return downloaded file
	 */
	public S3Object readFileFromS3_readyBucket_moveTheFileTo_doneBucket(String Key)
	{
		
		S3Object __FileObject = null;
		//==========read a key from S3ReadyKeys and
		//add this key to array list in memory for future use.
		//====================ArrayList<String> __keys = CommonMethods.receiveMessageFromSQS_S3ReadyKeys(_clientSQS,_SQS_S3ReadyKeys_URL, 1);
		if(Key == null) return null;
		//_memory_Keys_list.add(Key);
		
		//download the fileObject from the TDCReady using that key
		AmazonS3Client clientS3 = CommonMethods.getS3ClientObject();
		GetObjectRequest __getObjectReq = new GetObjectRequest(_bucketReadyName, Key);
		__FileObject= clientS3.getObject(__getObjectReq);
		
		//move object to the other bucket
		clientS3.copyObject(_bucketReadyName,Key, _bucketDoneName,Key);
		
		//file deleted from TDCReady
		clientS3.deleteObject(_bucketReadyName, Key);
		
		return __FileObject;
	}
	
	
	
	/**
	 * 
	 * @param content InputStream
	 * @return File content as String type
	 */
	public String getFileData(InputStream content) {
        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(content));
        StringBuilder sb = new StringBuilder();
        try {
            String line = rdr.readLine();
            while (line != null) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(line);
                line = rdr.readLine();
            }
            return sb.toString();
        }
        catch (IOException e) {
//            throw new RuntimeException(e);
        	return null;

        }
    }
	
	
	
	
	/**
	 *  convert relative URLs to absolute URL using main URL
	 * 
	 * @param links relative Links
	 * @param mainURL
	 * @return absolute URL version of the relative links
	 */
	private List<HtmlLink> relateiveToAbsuluteURL(List<HtmlLink> links, String mainURL)
	{
		//relative to absolute
		try {
			List<HtmlLink> absolutes = new ArrayList<HtmlLink>();
			URL main = new URL(mainURL);
			
			Iterator<HtmlLink> it = links.iterator();
			while(it.hasNext())
			{
				HtmlLink relativeLink = it.next();
				
				//--System.out.println(main.toString());
				//--System.out.println(relativeLink.link);
				try{
					URL relative= new URL(main,relativeLink.link);
					if(relativeLink.link.startsWith("http"))
						absolutes.add(new HtmlLink(relativeLink.link, ""));
					else
						absolutes.add(new HtmlLink(relative.toString(), ""));
				}catch (MalformedURLException e) {
				}
				
			}
			
			return absolutes;
	        
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


}
