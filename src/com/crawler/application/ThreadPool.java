package com.crawler.application;

import com.crawler.common.CommonMethods;

public class ThreadPool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/**
		 * Set up configuration for Application
		 */
		CommonMethods config = new CommonMethods();
		config.loadConfiguration(args[0]);
		
		System.out.println("Crawler Application is running...");
		Thread parsingThread = new Thread(new Thread_S3_Parsing(CommonMethods.ReadyBucket,CommonMethods.DoneBucket,CommonMethods.ApplicationToManager));
		parsingThread.start();
	
		
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			parsingThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Crawler Application is stopped.");
		
		//TestFramework test = new TestFramework();
	}

}
