package com.hadoop.pagerank;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import com.crawler.common.CommonMethods;


public class PageRankDriver {
	int pagerankStop;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputPath = "";
		PageRankDriver driver = new PageRankDriver();
		String outputPath = args[1] + "_0";
		driver.runFirstJob(new Path(args[0]), new Path(outputPath));
		int i = 0;
		while (i <= CommonMethods.PAGE_RANK_MAX_LOOP) {
			i++;
			inputPath = outputPath;
			outputPath = args[1] + "_" + Integer.toString(i);
			RunningJob loopJob = driver.runLoopJob(new Path(inputPath), new Path(outputPath));
			try {
				if (loopJob.getCounters().findCounter("PAGECOUNTERGROUP", "PAGECOUNTER").getValue() < 3) {
					break;
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		outputPath = args[1] + "_final";
		driver.runCleanUpJob(new Path(inputPath), new Path(outputPath));
	}

	
	public RunningJob runLoopJob(Path inputPath, Path outputPath) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(PageRankDriver.class);
		conf.setJobName("PageRank" + System.currentTimeMillis());
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setMapperClass(SecondPageRankMapper.class);
		conf.setReducerClass(SecondPageRankReducer.class);
		client.setConf(conf);
	    try {
	      return JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	      return null;
	    }
	}
	
	
	public void runFirstJob(Path inputPath, Path outputPath) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(PageRankDriver.class);
		conf.setJobName("PageRank" + System.currentTimeMillis());
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);
		client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}
	
	
	public void runCleanUpJob(Path inputPath, Path outputPath) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(PageRankDriver.class);
		conf.setJobName("FinalPageRank" + System.currentTimeMillis());
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setMapperClass(SecondPageRankMapper.class);
		conf.setReducerClass(FinalPageRankReducer.class);
		client.setConf(conf);
	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}
}	
