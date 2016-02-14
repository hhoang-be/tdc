package com.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.crawler.common.CommonMethods;

@SuppressWarnings("deprecation")
public class FinalPageRankReducer  extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {
	
	static public String INFORMATION_EXTRACT = "(\\<)(out|in)(\\>)(\\<)(http://)([^\\>\\<]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>\\<)([\\d]+)(\\>)(\\<)([\\d|\\.|E|\\-]+)(\\>)";
	private Pattern pattern;
	private Matcher matcher;
	private ArrayList<String> outgoingHtmls;
	private ArrayList<String> incomingHtmls;
	private long totalPages = 10000;
	private AmazonDynamoDBClient client;
	String regex = "(.*)(\\<title\\>)(([^\\>\\<]+))(\\</title\\>)(.*)";
	Pattern patternTitle;;
	Matcher matcherTitle;
	AmazonS3Client s3Client;

	
	public FinalPageRankReducer() {
		// TODO Auto-generated constructor stub
		pattern = Pattern.compile(INFORMATION_EXTRACT);
		patternTitle = Pattern.compile(regex);
		client = CommonMethods.getDynamoDBObject();
		s3Client = CommonMethods.getS3ClientObject();
		patternTitle = Pattern.compile(regex);
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		String lastPartOfValue = populateData(key, values);
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
	
	private String populateData(Text key, Iterator<Text> _values) {
		/*a list of <key html><out|in><value html)><pageRank><noOfOutgoingHtml>*/
		double pageRank = 0;
		int noOfOutgoingHtml = 0;
		outgoingHtmls  = new ArrayList<String>();
		incomingHtmls = new ArrayList<String>();
		Map<String, AttributeValue> items = new HashMap<String, AttributeValue>();
		PutItemRequest itemRequest;
		while (_values.hasNext()) {
			Text _value = _values.next();
			String _strValue = _value.toString();
			matcher = pattern.matcher(_strValue);
			if (matcher.matches()) {
				if ((matcher.group(2)).equals("out")) {			//if this is outgoing html of key page
					noOfOutgoingHtml++;
					outgoingHtmls.add(matcher.group(5) + matcher.group(6));
				} else if ((matcher.group(2)).equals("in")) {	//this is incoming html of key page
					int tempNoOfOut = Integer.parseInt(matcher.group(11));
					incomingHtmls.add(matcher.group(5) + matcher.group(6));
					if (tempNoOfOut != 0) {
						pageRank = pageRank + (Double.parseDouble(matcher.group(9)))/tempNoOfOut;
					} else {
						pageRank = pageRank + (Double.parseDouble(matcher.group(9)))/totalPages;
					}
				}
			}
		}
		pageRank = 0.85*pageRank + (1 - 0.85)/CommonMethods.TOTAL_PAGES;
		String formedURL = key.toString().replace("!!", "/");
//		String title = extractPageTitle(formedURL.trim());
		/*Put item to DynamoDB*/
		try {
			if (key.toString().length() < 2000) {		//maximum size limit of hashkey is 2024 bytes
				items.put("URL", new AttributeValue().withS(key.toString().trim()));
				items.put("PageRank", new AttributeValue().withN(Double.toString(pageRank)));
//				items.put("Title", new AttributeValue().withS(title));
				itemRequest = new PutItemRequest().withTableName("PageInfo").withItem(items);
				client.putItem(itemRequest);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "<" + Double.toString(pageRank) + "><" + Integer.toString(noOfOutgoingHtml) + ">";
	}
	
	public String extractPageTitle(String urlString) {
		String title;
		String line;
		URL url;
		try {
			url = new URL(urlString);
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
			while ((line = reader.readLine()) != null) {
				matcherTitle = patternTitle.matcher(line);
				if (matcherTitle.find()) {
					title = matcherTitle.group(3);
					return title;
				}
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return urlString;
	}
}
