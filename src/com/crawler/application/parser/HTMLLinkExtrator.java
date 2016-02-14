package com.crawler.application.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
/**
 * 
 * @author Emad Heydari Beni
 *
 */
public class HTMLLinkExtrator{
 
	  private Pattern patternTag, patternLink;
	  private Matcher matcherTag, matcherLink;
 
	  private static final String HTML_A_TAG_PATTERN = 
                      "(?i)<a([^>]+)>(.+?)</a>";
 
	  private static final String HTML_A_HREF_TAG_PATTERN = 
                      "\\s*(?i)href\\s*=\\s*(\"([^\"]*\")|'[^']*'|([^'\">\\s]+))";
 
	  public HTMLLinkExtrator(){
		  patternTag = Pattern.compile(HTML_A_TAG_PATTERN);
		  patternLink = Pattern.compile(HTML_A_HREF_TAG_PATTERN);
	  }
 
	  /**
	   * Validate html with regular expression
	   * @param html html content for validation
	   * @return Vector links and link text
	   */
	  public List<HtmlLink> grabHTMLLinks(final String html){
 
		  ArrayList<HtmlLink> result = new ArrayList<HtmlLink>();
 
		  matcherTag = patternTag.matcher(html);
 
		  while(matcherTag.find()){
 
			  String href = matcherTag.group(1); //href
			  String linkText = matcherTag.group(2); //link text
 
			  matcherLink = patternLink.matcher(href);
 
			  while(matcherLink.find()){
 
				  String link = matcherLink.group(1); //link
				  //System.out.println(link);
				  if(link.startsWith("\".") || link.startsWith("\"h") || link.startsWith("\"/"))
					  result.add(new HtmlLink(link.substring(1, link.length()-1), linkText));
 
			  }
 
		  }
 
		  return result;
 
	  }
 
}
