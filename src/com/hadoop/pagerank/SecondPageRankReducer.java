package com.hadoop.pagerank;

import java.io.IOException;
import com.crawler.common.CommonMethods;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SecondPageRankReducer  extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {
	
	static public String INFORMATION_EXTRACT = "(\\<)(out|in)(\\>)(\\<)(http://)([^\\>\\<]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>\\<)([\\d]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>)";
	private Pattern pattern;
	private Matcher matcher;
	private ArrayList<String> outgoingHtmls;
	private ArrayList<String> incomingHtmls;
	private long totalPages = 10000;
	
	public SecondPageRankReducer() {
		// TODO Auto-generated constructor stub
		pattern = Pattern.compile(INFORMATION_EXTRACT);
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		String lastPartOfValue = populateData(values, reporter);
		String tempString = "";
		for (int i = 0; i < outgoingHtmls.size(); i++) {
			tempString = "<in><" + key.toString() + ">" + lastPartOfValue;
			output.collect(new Text(outgoingHtmls.get(i)), new Text(tempString));
		}
		
		for (int i = 0; i < incomingHtmls.size(); i++) {
			tempString = "<out><" + key.toString() + ">" + lastPartOfValue;
			output.collect(new Text(incomingHtmls.get(i)), new Text(tempString));
		}
	}
	
	
	private String populateData(Iterator<Text> _values, Reporter reporter) {
		/*a list of <key html><out|in><value html)><pageRank><noOfOutgoingHtml>*/
		double pageRank = 0;
		double oldPageRank = 0;
		int noOfOutgoingHtml = 0;
		outgoingHtmls  = new ArrayList<String>();
		incomingHtmls = new ArrayList<String>();
		while (_values.hasNext()) {
			Text _value = _values.next();
			String _strValue = _value.toString();
			matcher = pattern.matcher(_strValue);
			if (matcher.matches()) {
				oldPageRank = Double.parseDouble(matcher.group(14));
				if ((matcher.group(2)).equals("out")) {			//if this is outgoing html of key page
					noOfOutgoingHtml++;
					outgoingHtmls.add(matcher.group(5) + matcher.group(6));
				} else if ((matcher.group(2)).equals("in")) {	//this is incoming html of key page
					int tempNoOfOut = Integer.parseInt(matcher.group(11));
					incomingHtmls.add(matcher.group(5) + matcher.group(6));
					if (tempNoOfOut != 0) {
						pageRank = pageRank + (Double.parseDouble(matcher.group(9)))/tempNoOfOut;
					} else {
						pageRank = pageRank + (Double.parseDouble(matcher.group(9)))/CommonMethods.TOTAL_PAGES;
					}
				}
			}
		}
		pageRank = 0.85*pageRank + (1 - 0.85)/CommonMethods.TOTAL_PAGES;
		if (Math.pow((pageRank - oldPageRank), 2) > Math.pow(CommonMethods.PAGE_RANK_THRESHOLD, 2)) {	//there is one computed pagerank not satisfy requirement
			reporter.incrCounter("PAGECOUNTERGROUP", "PAGECOUNTER", 1);
		} else {	//otherwise, pagerank remains unchanged
			pageRank = oldPageRank;
		}
		return "<" + Double.toString(oldPageRank) + "><" + Integer.toString(noOfOutgoingHtml) + "><" + Double.toString(pageRank) + ">";
	}
}
