package com.hadoop.pagerank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.crawler.common.CommonMethods;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> {
	
	static public String FILE_LINE_REG_EX = "(http://)([^\\<\\,]+),(\\<)([\\d]+)([(\\,)((http://)[^\\,]*)]+)(\\>)(\\,\\<)([\\d]+)([(\\,)(http://)([^\\,]*)]+)(\\>)";
	static public String HTML_SEPARATOR_REG_EX = "(\\,)([^\\,]*)";
	
	private Pattern patternLineRegEx;
	private Pattern patternHtmlSeparator;

	public PageRankMapper() {
		// TODO Auto-generated constructor stub
		patternLineRegEx = Pattern.compile(FILE_LINE_REG_EX);
		patternHtmlSeparator = Pattern.compile(HTML_SEPARATOR_REG_EX);
	}
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		/**	text format in data file is as follow
		/*	baseHtlm,<noOfInHtmls,inHtmls>,<noOfOutHtmls,outHtmls>
		 * */
		String line = value.toString().trim();	//remove all whitespace or tab
		Matcher matcherLineRegEx = patternLineRegEx.matcher(line);
		if (matcherLineRegEx.matches()) {
			String newValue = "";
			String keyValue = "";
			String baseHtml = matcherLineRegEx.group(1) + matcherLineRegEx.group(2);
			String inHtmls = matcherLineRegEx.group(5);			 
			String outHtmls = matcherLineRegEx.group(9);
			int noOfInHtmls = Integer.parseInt(matcherLineRegEx.group(4));
			int noOfOutHtmls = Integer.parseInt(matcherLineRegEx.group(8));
			Matcher matcherHtmlSeparator;
			if (noOfInHtmls != 0) {
				matcherHtmlSeparator = patternHtmlSeparator.matcher(inHtmls);	//list of incoming Htlms to base Html
				while (matcherHtmlSeparator.find()) {
					/*output text format is as follow*/
					/*<incoming html><out><base html><page rank><number of outgoing Htmls from base html><pagerank of incoming html>*/
					keyValue = matcherHtmlSeparator.group(2);
					newValue = "<out>" + "<" + baseHtml + ">" + "<" + Float.toString(1/CommonMethods.TOTAL_PAGES) + ">" + "<" + Integer.toString(noOfOutHtmls) + "><" + Float.toString(1/CommonMethods.TOTAL_PAGES) + ">";
//					newValue = "<out>" + "<" + baseHtml + ">" + "<" + Float.toString(1/CommonMethods.TOTAL_PAGES) + ">" + "<" + Integer.toString(noOfOutHtmls) + ">";
					output.collect(new Text(keyValue), new Text(newValue));
				}
			}
			if (noOfOutHtmls != 0) {
				matcherHtmlSeparator = patternHtmlSeparator.matcher(outHtmls);	//list of outgoing Htlms to base Html
				while (matcherHtmlSeparator.find()) {
					/*<outgoing html><in><base html><page rank><number of outgoing html from base html><pagerank of  outgoing html>*/
					keyValue = matcherHtmlSeparator.group(2);
					newValue = "<in>" + "<" + baseHtml + ">" + "<" + Double.toString(1/CommonMethods.TOTAL_PAGES) + ">" + "<" + Integer.toString(noOfOutHtmls) + "><" + Float.toString(1/CommonMethods.TOTAL_PAGES) + ">";
//					newValue = "<in>" + "<" + baseHtml + ">" + "<" + Double.toString(1/CommonMethods.TOTAL_PAGES) + ">" + "<" + Integer.toString(noOfOutHtmls) + ">";
					output.collect(new Text(keyValue), new Text(newValue));
				}
			}
		}
	}
}
