package com.petty.etl.parser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.constant.Constants;

public class DoubanBookParser {
	
	/*
	 * url pattern
	 * http://book.douban.com/annotation/10964578/
	 */
	public static JSONObject parseAnnotation(String html_body){
		JSONObject result = new JSONObject();
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		//result.put("title", title);
		
		//describe
		Element describe_el = html.select("#link-report").first();
		if(describe_el!=null){
			describe = describe_el.text();
		}
		//result.put("describe", describe);
		
		
		Elements reply_content_docs = html.select("#comments > div.bd > div > div.comment-item");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
		//result.put("reply", reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://book.douban.com/subject/1023045/discussion/18849148/
	 */
	public static JSONObject parseDiscussion(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		//result.put("title", title);
		
		//describe
		Element describe_el = html.select("#link-report > span:nth-child(4)").first();
		if(describe_el!=null){
			describe = describe_el.text();
		}
		//result.put("describe", describe);
		
		Elements reply_content_docs = html.select("#comments > table.wr.comment-item > tbody.content > tr > td > span:nth-child(2)");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
		//result.put("reply", reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://book.douban.com/subject/10739341/comments/
	 */
	public static JSONObject parseComments(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		//result.put("title", title);
		
		//describe
		//result.put("describe", describe);
		
		Elements reply_content_docs = html.select("#comment-list-wrapper > div > ul > li > p");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
		//result.put("reply", reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://book.douban.com/subject/1000353/reviews
	 */
	public static JSONObject parseSubjectReview(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		//result.put("title", title);
		
		//describe
		//result.put("describe", describe);
		
		Elements reply_content_docs = html.select("#content > div.grid-16-8.clearfix > div.article > div.ctsh > div.tlst > div.clst > div.review-short > span");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
		//result.put("reply", reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://book.douban.com/review/1000007/
	 */
	public static JSONObject parseReview(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		//result.put("title", title);
		
		//describe
		Element describe_el = html.select("#link-report").first();
		if(describe_el!=null){
			describe = describe_el.text();
		}
		//result.put("describe", describe);
		
		Elements reply_content_docs = html.select("#content > div > div.article > div > div.piir > div.comments > div > div > div.bd > p");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
		//result.put("reply", reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
}
