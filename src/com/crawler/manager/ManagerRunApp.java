package com.crawler.manager;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import com.crawler.common.CommonMethods;

public class ManagerRunApp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/**
		 * Set up configuration for Manager
		 */
		CommonMethods config = new CommonMethods();
		config.loadConfiguration(args[0]);
		// TODO Auto-generated method stub
        // stay around forever
		CrawlerManager manager = new CrawlerManager(CommonMethods.ApplicationToManager,
													CommonMethods.ManagerToDownloader,
													10);
		manager.pushSeedURLToDownloader(CommonMethods.initialSeedURLS);
		Thread crawler = new Thread(manager);
		crawler.start();
		System.out.println("Crawler Manager is running...");
        Object keepAlive = new Object();
        synchronized(keepAlive) {
        	try {
        		keepAlive.wait();
        	} catch(InterruptedException e) {
        		// do nothing  
        	}
        }
        System.out.println("Crawler Manager is halted.");
	}
}
