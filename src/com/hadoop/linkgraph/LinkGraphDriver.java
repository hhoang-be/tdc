/**
 * Example application for Hadoop: Driver
 * Topics in Distributed Computing
 * @author Ruben Van den Bossche
 * http://hadoop.apache.org/common/docs/r1.0.3/mapred_tutorial.html
 * 
 * Run:
 * hadoop jar Wordcount.jar be.ac.ua.comp.hadoop.WordCountDriver <input path> <output path>
 */

package com.hadoop.linkgraph;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.crawler.common.CommonMethods;

/**
 * Driver class containing the main method
 * @author rvdbossc
 */
public class LinkGraphDriver {

	/**
	 * @param args input and output file location (starting with hdfs://localhost for HDFS locations)
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		CommonMethods config = new CommonMethods();
		config.loadConfiguration(args[0]);
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(LinkGraphDriver.class);
		
		conf.setJobName("LinkGraph");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(LinkGraphMapper.class);
		conf.setReducerClass(LinkGraphReducer.class);
		
		
		client.setConf(conf);

	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}

}
