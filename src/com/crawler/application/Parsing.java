package com.crawler.application;



import com.crawler.application.parser.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author Emad Heydari Bei
 *
 */
public class Parsing {
	
	
	//=======================================
	List<String> URLs_memory;
	//=======================================
	
	
	public Parsing()
	{
		 URLs_memory = new ArrayList<String>();
	}
	
	
	/**
	 * Parsing and extracting links
	 * @return list of links
	 */
	public List<HtmlLink> parse_extract_URLs(String HTMLcontent)
	{
		HTMLLinkExtrator htmlLinkExtrator = new HTMLLinkExtrator();
		List<HtmlLink> links = htmlLinkExtrator.grabHTMLLinks(HTMLcontent);
		return links;
	}
	
	
	
	/**
	 * Save extracted URLs in Memory (ArrayList) and 
	 * check to avoid any repeated URLs.
	 * 
	 * @param links
	 * @return list of unvisited links (previous visited links dropped)
	 */
	public ArrayList<URL> saveExraceted_URLs(List<HtmlLink> links)
	{ 
		ArrayList<URL> links_repeateds_dropped = new ArrayList<URL>();
		Iterator<HtmlLink> it = links.iterator();
		while(it.hasNext())
		{
			HtmlLink link = it.next();
			if(!existsInMemory(link.link))
			{
				URLs_memory.add(link.link);
				try {
					links_repeateds_dropped.add(new URL(link.link));
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return links_repeateds_dropped;
	}
	
	/**
	 * 
	 * @param link this link to be not in the memory
	 * @return
	 */
	private boolean existsInMemory(String link)
	{
		Iterator<String> it = URLs_memory.iterator();
		while(it.hasNext())
		{
			String tmpLink = it.next();
			if(tmpLink.equals(link))
			{
				//System.out.println(link);
				return true;
			}
			
		}
		return false;
	}

}