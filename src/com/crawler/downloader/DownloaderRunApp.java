package com.crawler.downloader;

import com.crawler.common.CommonMethods;

public class DownloaderRunApp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/**
		 * Set up configuration for Downloader
		 */
		CommonMethods config = new CommonMethods();
		config.loadConfiguration(args[0]);
		Downloader downloader = new Downloader(CommonMethods.ReadyBucket,
												CommonMethods.ManagerToDownloader,
												4,
												1);
		Thread crawler = new Thread(downloader);
		System.out.println("Downloader is running...");
		crawler.start();
        Object keepAlive = new Object();
        synchronized(keepAlive) {
        	try {
        		keepAlive.wait();
        	} catch(InterruptedException e) {
        		// do nothing  
        	}
        }
        System.out.println("Crawler Downloader is halted.");
	}
}
