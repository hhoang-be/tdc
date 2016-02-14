/**
 * Example application for Hadoop: Reducer
 * Topics in Distributed Computing
 * @author Ruben Van den Bossche
 * http://hadoop.apache.org/common/docs/r1.0.3/mapred_tutorial.html
 */

package com.hadoop.invertedindex;

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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.crawler.common.CommonMethods;

public class InvertedIndexHamletReducer extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	private AmazonDynamoDBClient client;
	String regex = "(.*)(\\<title\\>)(([^\\>\\<]+))(\\</title\\>)(.*)";
	Pattern patternTitle;;
	Matcher matcherTitle;
	
	public InvertedIndexHamletReducer() {
		client = CommonMethods.getDynamoDBObject();
		patternTitle = Pattern.compile(regex);
	}
	
	/**
	 * Reduce method for word count
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		PutItemRequest itemRequest;
		/*
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}*/
		
		ArrayList<String> files = new ArrayList<String>();
		ArrayList<Long> occurs = new ArrayList<Long>();
		Map<String, AttributeValue> items;// = new HashMap<String, AttributeValue>();
		
		makeFilesAndNumberOfOccurence(values,files,occurs);
		
		int index = 0;
		StringBuilder toReturn = new StringBuilder();
		toReturn.append(";");
		String formedURL;
		String title;
	    while (index < files.size()){
	        toReturn.append(files.get(index) +"," + occurs.get(index).toString() + ";");
	        formedURL = files.get(index).replaceAll("!!", "/");
//	        title = extractPageTitle(formedURL);
//	        
//	        /*Put item into DynamoDB*/
	        items = new HashMap<String, AttributeValue>();
	        items.put("Key", new AttributeValue().withS(key.toString()));
	        items.put("Location", new AttributeValue().withS(formedURL));
//	        items.put("Title", new AttributeValue().withS(title));
	        itemRequest = new PutItemRequest().withTableName("InvertedIndex").withItem(items);
	        client.putItem(itemRequest);
	        index++;
	    }
		
		output.collect(key, new Text(toReturn.toString()));
		
		/*
		 * output style:
		 * 
		 * appeared	 ;file3.txt,1;
		 * appetite	 ;file3.txt,2;file3.txt,1;
		 * 
		 * 
		 * 
		 */
	}
	
	/**
	 * retrieve the files and number of occurence in each of them in
	 * "files" and "occurs" ArrayLists
	 * @param values
	 * @param occurs 
	 * @param files 
	 */
	public void makeFilesAndNumberOfOccurence(Iterator<Text> values, ArrayList<String> files, ArrayList<Long> occurs)
	{
		while(values.hasNext())
		{
			Text Value = values.next();
			//check if exist or not
			Iterator<String> it = files.iterator();
			int index = 0;
			while(it.hasNext())
			{
				String filename = it.next();
				if(filename.equals(Value.toString()))
				{
					long increasedCounter = occurs.get(index).longValue();
					occurs.set(index, new Long(++increasedCounter));
					break;
				}
				index++;
			}
			//not in files
			files.add(Value.toString());
			occurs.add(new Long(1));
		}
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
