package com.hadoop.pagerank;

import java.io.IOException;
import com.crawler.common.CommonMethods;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.crawler.common.CommonMethods;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer  extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {
	
	static public String INFORMATION_EXTRACT = "(\\<)(out|in)(\\>)(\\<)(http://)([^\\>\\<]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>\\<)([\\d]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>)";
	private Pattern pattern;
	private Matcher matcher;
	private ArrayList<String> outgoingHtmls;
	private ArrayList<String> incomingHtmls;
	
	public PageRankReducer() {
		// TODO Auto-generated constructor stub
		pattern = Pattern.compile(INFORMATION_EXTRACT);
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		String lastPartOfValue = populateData(values);
		String tempString;
		for (int i = 0; i < outgoingHtmls.size(); i++) {
			tempString = "<in><" + key.toString() + ">" + lastPartOfValue;
			output.collect(new Text(outgoingHtmls.get(i)), new Text(tempString));
		}
		
		for (int i = 0; i < incomingHtmls.size(); i++) {
			tempString = "<out><" + key.toString() + ">" + lastPartOfValue;
			output.collect(new Text(incomingHtmls.get(i)), new Text(tempString));
		}
	}
	
	private String populateData(Iterator<Text> _values) {
		/*a list of <key html><out|in><value html)><pageRank><noOfOutgoingHtml><pagerank of key html><page rank of key html>*/
		double pageRank = 0;
		int noOfOutgoingHtml = 0;
		incomingHtmls =  new ArrayList<String>();
		outgoingHtmls = new ArrayList<String>();
		while (_values.hasNext()) {
			Text _value = _values.next();
			String _strValue = _value.toString();
			matcher = pattern.matcher(_strValue);
			if (matcher.matches()) {
				if ((matcher.group(2)).equals("out")) {							//if this is outgoing html of key page
					noOfOutgoingHtml++;
					outgoingHtmls.add(matcher.group(5) + matcher.group(6));
				} else {														//this is incoming html of key page
					int tempNoOfOut = Integer.parseInt(matcher.group(11));		//no of outgoing html
					incomingHtmls.add(matcher.group(5) + matcher.group(6));		//old page rank of value html
					if (tempNoOfOut != 0) { 	//if number of incoming html !=0 then using rk(pj)/|pj|
						pageRank = pageRank + (Double.parseDouble(matcher.group(9)))/tempNoOfOut;	
					} else {					//otherwise using rk(pj)/total pages
						pageRank = pageRank + (Double.parseDouble(matcher.group(9)))/CommonMethods.TOTAL_PAGES;
					}
				}
			}
		}
		pageRank = 0.85*pageRank + (1 - 0.85)/CommonMethods.TOTAL_PAGES;		//final formula to compute pagerank
		return "<" + Double.toString(pageRank) + "><" + Integer.toString(noOfOutgoingHtml) + "><" + (Double.parseDouble(matcher.group(14))) + ">";
	}
}
