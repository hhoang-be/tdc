package com.crawler.manager;

import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.crawler.common.CommonMethods;


public class CrawlerManager implements Runnable {
	
	private AmazonSQSClient clientSQS = null;
	private String queueURLToDownloader;	//URL of queue from Manager to Downloader
	private ArrayList<URL> feededURLs;		//initial URLs
	private ArrayList<Pattern> countryPatterns;	//list of pattern to filter fetched URLs
	private ArrayList<Pattern> fileTypePatterns;	//list of pattern to filter fetched URLs
	private String queueURLFromApp;			//URL of queue from Application to Manager
	private int maxNoOfMessages;			//maximum number of messages can be fetched in one call
	private boolean serviceRunning;			
	
	
	CrawlerManager() {
		// TODO Auto-generated constructor stub
	}
	
	
	CrawlerManager(String _queueURLFromApp, String _queueURLToDownloader, int _maxNoOfMessages) {
		serviceRunning = true;
		this.queueURLToDownloader = _queueURLToDownloader;
		this.queueURLFromApp = _queueURLFromApp;
		this.maxNoOfMessages = _maxNoOfMessages;
		clientSQS = CommonMethods.getSQSClientObject();
		receivePolicyFromConfigFile();

	}

	
	public synchronized void setMaxNoOfMessages(int _maxNoOfMessages) {
		this.maxNoOfMessages = _maxNoOfMessages;
	}

	/**
	 * Received URL from SQS to process before pushing to downloader
	 */
	public synchronized void receiveURLsFromApplication() {
		feededURLs = CommonMethods.receiveMessageFromSQS(clientSQS, queueURLFromApp, maxNoOfMessages);
	}
	
	/**
	 *  Apply selection policy to list of URLs and push the results to 
	 *  queue from Manager to Downloader
	 *  @param:
	 *  @return:
	 */
	public synchronized void pushURLsToDownloader() {
		if (feededURLs != null) {
			ArrayList<URL> filteredURLList = new ArrayList<URL>();
			for (int i = 0; i < feededURLs.size(); i++) {
				if (applyPolicyToURLs(feededURLs.get(i))) {	
					filteredURLList.add(feededURLs.get(i));
					System.out.println(feededURLs.get(i).toString());
				}
			}
			CommonMethods.sendMessageToSQS(clientSQS, queueURLToDownloader, filteredURLList);
		}
	}
	
	
	/**
	 * Push a list of initial URLs to queue. This list is used as seed URLs for crawler
	 * @param: list of URLs
	 * @return:
	 */
	public void pushSeedURLToDownloader(ArrayList<URL> _seedURLs) {
		if (_seedURLs != null) {
			CommonMethods.sendMessageToSQS(clientSQS, queueURLToDownloader, _seedURLs);
		}
	}
	
	
	/**
	 * Apply selection policy to a URL. Selection policy is represented by
	 * a number of regular expressions
	 * @param: URL needed to check
	 * @return: true if URL passes and false otherwise
	 */
	public boolean applyPolicyToURLs(URL _url) {
		String strURL = _url.toString();
		if (this.countryPatterns.size() >= 1) {		//no rule for country
			for (Pattern pattern : this.countryPatterns) {
				Matcher matcher = pattern.matcher(strURL);
				if (matcher.matches()) {
					if (fileTypePatterns.size() >= 1) {
						for (Pattern fileTmpPatter : this.fileTypePatterns) {
							Matcher fileMatcher = fileTmpPatter.matcher(strURL);
							if (fileMatcher.matches()) {
								return true;
							}
						}
					} else {
						return true;
					}
				}
			}
		} else {
			if (fileTypePatterns.size() >= 1) {
				for (Pattern fileTmpPatter : this.fileTypePatterns) {
					Matcher fileMatcher = fileTmpPatter.matcher(strURL);
					if (fileMatcher.matches()) {
						return true;
					}
				}
			} else {
				return true;
			}
	
		}
		return false;
	}
	
	/**
	 * receive policy from configuration file
	 */
	public void receivePolicyFromConfigFile() {
		countryPatterns = new ArrayList<Pattern>();
		fileTypePatterns = new ArrayList<Pattern>();
		Pattern tmpPattern;
		String policyString;
		ArrayList<String> countryFilters = CommonMethods.countryFilters;
		ArrayList<String> fileTypeFilters = CommonMethods.fileTypeFilters;
		
		for (int i = 0; i < countryFilters.size(); i++) {
			policyString = "([A-Za-z]{3,9})://(www\\.|)([_a-zA-Z0-9]+)(\\." + countryFilters.get(i) + ")(.+)";
			tmpPattern = Pattern.compile(policyString);
			countryPatterns.add(tmpPattern);
		}
		
		for (int i = 0; i < fileTypeFilters.size(); i++) {
			policyString = "([A-Za-z]{3,9})://(www\\.)([_a-zA-Z0-9]+)(\\.)([a-zA-Z]{2,4})(.+)(\\." + fileTypeFilters.get(i) + ")(.*)";
			tmpPattern = Pattern.compile(policyString);
			fileTypePatterns.add(tmpPattern);
		}
	}

	/**
	 * Receive policy directly from web interface
	 */
    public void receivePolicyFromQueue() {
        String abbreString = CommonMethods.receiveStringFromSQSNoRemove(clientSQS, CommonMethods.PolicyConfiguration, 10);
        String policyString;
        if (abbreString != null) {
                policyString = "([A-Za-z]{3,9})://(www\\.|)([_a-zA-Z0-9]+)(\\" + abbreString + ")(.+)"; 
                Pattern tempPattern = Pattern.compile(policyString);
                countryPatterns.add(tempPattern);
        }
}
	
	/**
	 * Some useful regular expression for
	 * 1. Only html webpage: "([A-Za-z]{3,9})://(www\\.)([_a-zA-Z0-9]+)(\\.)([a-zA-Z]{2,4})(.+)(\\.html)(.*)"
	 * 2. Only  from .be: "([A-Za-z]{3,9})://(www\\.|)([_a-zA-Z0-9]+)(\\.be)(.+)(\\.html)(.*)"
	 * 3. Only  .be: "([A-Za-z]{3,9})://(www\\.|)([_a-zA-Z0-9]+)(\\.be)(.+)"
	 */


	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (serviceRunning) {
			receiveURLsFromApplication();
			receivePolicyFromQueue();
			pushURLsToDownloader();
		}
	}
}