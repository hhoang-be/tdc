/**
 * Example application for Hadoop: Mapper
 * Topics in Distributed Computing
 * @author Ruben Van den Bossche
 * http://hadoop.apache.org/common/docs/r1.0.3/mapred_tutorial.html
 */

package com.hadoop.linkgraph;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.log4j.helpers.AbsoluteTimeDateFormat;

public class LinkGraphMapper extends MapReduceBase 
		implements Mapper<LongWritable, Text, Text, Text> {
	
	private final static Text location = new Text();
	private final static Text outgoing_Text = new Text();
	private final static Text incoming_Text = new Text();
	HTMLLinkExtractor linkExtractor = new HTMLLinkExtractor();
	//========================================
	//BEGIN
	//========================================
	public LinkGraphMapper()
	{

	}
	//========================================
	//END
	//========================================
	
	/**
	 * Map method for word count
	 */
	@Override
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		//===========================================
		//BEGIN
		//===========================================
		
		
		FileSplit filesplit = (FileSplit)reporter.getInputSplit();
		String filename = filesplit.getPath().getName();
		filename = filename.replaceAll("!!", "/");
		location.set(filename);
		
		Vector<HTMLLinkExtractor.HtmlLink> links = linkExtractor.grabHTMLLinks(line);
		
		ArrayList<URL> absLinks = convert_relativeToAbsoluteURL(filename, links);
		
		Iterator<URL> it = absLinks.iterator();
		StringBuilder outgoing = new StringBuilder();
		outgoing.append("out,");
		StringBuilder incoming = new StringBuilder();
		incoming.append("in,");
		while(it.hasNext())
		{
			URL link = it.next();
			
			//outgoing
			outgoing.append(link.toString());
			outgoing_Text.set(outgoing.toString());
			output.collect(location, outgoing_Text);
			
			//outgoing
			incoming.append(location);
			incoming_Text.set(link.toString());
			output.collect(incoming_Text, new Text(incoming.toString()));
		}
		

		/*
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			word.set(token);
			output.collect(word, location);
		}*/
		//============================================
		//END
		//============================================
	}
	
	
	
	/**
	 * Convert relative links to absolute links
	 * 
	 * @param baseLink
	 * @param relativeLink
	 * @return
	 */
	public ArrayList<URL> convert_relativeToAbsoluteURL(String baseLink, 
			Vector<HTMLLinkExtractor.HtmlLink> relativeLinks)
	{
		ArrayList<URL> absLinks = new ArrayList<URL>();
		
		Iterator<HTMLLinkExtractor.HtmlLink> it = relativeLinks.iterator();
		while(it.hasNext())
		{
			HTMLLinkExtractor.HtmlLink htmllink = it.next();
			URL absoluteURL;
			try {
				absoluteURL = new URL(new URL(baseLink), htmllink.getLink());
				absLinks.add(absoluteURL);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} 
			
		}
		
		return absLinks;
	}

}
