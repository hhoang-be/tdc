package com.hadoop.pagerank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SecondPageRankMapper  extends MapReduceBase 
		implements Mapper<LongWritable, Text, Text, Text> {
	
	/*<key html><out|in><value html)><pageRank><noOfOutgoingHtml>*/
	static public String INFORMATION_EXTRACT = "(http://)([^\\<]+)(\\<)(out|in)(\\>)(\\<)(http://)([^\\>\\<]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>\\<)([\\d]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>)";
	private Pattern pattern = Pattern.compile(INFORMATION_EXTRACT);
	private Matcher matcher;
	
	public SecondPageRankMapper() {
		
	}

	@Override
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		matcher = pattern.matcher(value.toString());
		if (matcher.matches()) {
			String keyURL = matcher.group(1) + matcher.group(2);
			String valueURL = "";
			for (int i = 3; i <= matcher.groupCount(); i++) {
				valueURL = valueURL + matcher.group(i);
			}
			output.collect(new Text(keyURL), new Text(valueURL));
		}
	}
}
