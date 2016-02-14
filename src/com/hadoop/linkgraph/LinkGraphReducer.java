/**
 * Example application for Hadoop: Reducer
 * Topics in Distributed Computing
 * @author Ruben Van den Bossche
 * http://hadoop.apache.org/common/docs/r1.0.3/mapred_tutorial.html
 */

package com.hadoop.linkgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class LinkGraphReducer extends MapReduceBase 
		implements Reducer<Text, Text, Text, Text> {

	

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		/*
		 * Values[index] =>  Value style
		 * Value[0] 	 =>  in,link1
		 * Value[1]		 =>  in,link2
		 * Value[2]		 =>  out,link3
		 * Value[3]		 =>  in,link1
		 * ...
		 */
		
		ArrayList<String> incoming = new ArrayList<String>();
		ArrayList<String> outgoing = new ArrayList<String>();
		
		fill_incoming_outgoing(values,incoming,outgoing);
		
		StringBuilder toReturn = new StringBuilder();
		toReturn.append(",<");
		
		toReturn.append(String.valueOf(incoming.size()) + ",");
		for(int i=0; i<incoming.size(); i++)
		{
			toReturn.append(incoming.get(i));
			if(incoming.size()-i != 1)
				toReturn.append(",");
		}
		toReturn.append(">,<");
		toReturn.append(String.valueOf(outgoing.size()) + ",");
		for(int i=0; i<outgoing.size(); i++)
		{
			toReturn.append(outgoing.get(i));
			if(outgoing.size()-i != 1)
				toReturn.append(",");
		}
		toReturn.append(">");
		
		output.collect(key, new Text(toReturn.toString()));
		
		/**
		 * output style:
		 * 
		 * LINK,<#,incoming>,<#,outgoing>
		 * 
		 * link1,<10,link2,link3,...>,<4,link2,link5,link6,...>
		 * link2,<4,link3,link1,...>,<3,link1,link3,...>
		 * ...
		 * 
		 */
	}

	
	/**
	 * Filling incoming and outgoing arrays. (Duplicates will be removed)
	 * 
	 * @param values
	 * @param incoming
	 * @param outgoing
	 */
	private void fill_incoming_outgoing(Iterator<Text> values,
			ArrayList<String> incoming, ArrayList<String> outgoing) {

		/*
		 * Values[index] =>  Value style
		 * Value[0] 	 =>  in,link1
		 * Value[1]		 =>  in,link2
		 * Value[2]		 =>  out,link3
		 * Value[3]		 =>  in,link1
		 * ...
		 */
		
		while(values.hasNext())
		{
			Text value = values.next();
			StringTokenizer Tokens = new StringTokenizer(value.toString(), ",");
			String type = Tokens.nextToken();
			String link = Tokens.nextToken();
			
			if(type.equalsIgnoreCase("in"))
			{
				if(!exists(link, incoming))
					incoming.add(link);
			}
			else if(type.equalsIgnoreCase("out"))
			{
				if(!exists(link, outgoing))
					outgoing.add(link);
			}
			
		}
		
	}
	
	/**
	 * To avoid duplicate link in the list.
	 * 
	 * @param link
	 * @param links
	 * @return
	 */
	private boolean exists(String link, ArrayList<String> links)
	{
		Iterator<String> it = links.iterator();
		while(it.hasNext())
		{
			String currLink = it.next();
			if(link.equalsIgnoreCase(currLink))
				return true;
		}
		return false;
	}
	

}
