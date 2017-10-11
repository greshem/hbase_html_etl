package com.petty.etl.parser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.constant.Constants;

public class DoubanMovieParser {
	
	/*
	 * url pattern
	 * http://movie.douban.com/subject/25754848/comments
	 */
	public static JSONObject parseComment(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
				
		Elements reply_content_docs = html.select("#comments > div > div.comment > p");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
//		result.put(JsonObjectKeys.TITLE, title);
//		result.put(JsonObjectKeys.QUESTION, title);		
//		result.put(JsonObjectKeys.DESCRIPTION, describe);
//		result.put(JsonObjectKeys.ANSWERS, reply_content_list);
		
		return result;
	}
	
	
	
	/*
	 * url pattern
	 * http://movie.douban.com/subject/1292329/discussion/1019998/
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
		
		//describe
		Element describe_el = html.select("#link-report > span:nth-child(4) > p").first();
		if(describe_el!=null){
			describe = describe_el.text();
		}		
		
		Elements reply_content_docs = html.select("#comments > div.comment-item > div.content.report-comment > p");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
//		result.put(JsonObjectKeys.TITLE, title);
//		result.put(JsonObjectKeys.QUESTION, title);
//		result.put(JsonObjectKeys.DESCRIPTION, describe);		
//		result.put(JsonObjectKeys.ANSWERS, reply_content_list);
		
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://movie.douban.com/subject/1889299/tv_discuss
	 */
	public static JSONObject parseTvDiscussion(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("#content > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		
		
		
		Elements reply_content_docs = html.select("#comments > div.comm-mod.comment-item  > div.bd > p > span:nth-child(1)");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
//		result.put(JsonObjectKeys.TITLE, title);
//		result.put(JsonObjectKeys.QUESTION, title);
//		result.put(JsonObjectKeys.DESCRIPTION, describe);
//		result.put(JsonObjectKeys.ANSWERS, reply_content_list);		
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://movie.douban.com/subject/10581289/episode/140/
	 */
	public static JSONObject parseEpisode(String html_body){
		return parseTvDiscussion(html_body);
	}
	
	
	/*
	 * url pattern
	 * http://movie.douban.com/subject/1293399/reviews
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
		
		
		
		Elements reply_content_docs = html.select("#content > div > div.article > div > div.review > div.review-bd > div.review-short > span");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
//		result.put(JsonObjectKeys.TITLE, title);
//		result.put(JsonObjectKeys.QUESTION, title);
//		result.put(JsonObjectKeys.DESCRIPTION, describe);
//		result.put(JsonObjectKeys.ANSWERS, reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://movie.douban.com/review/1033924/
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
				
		//describe
		Element describe_el = html.select("#link-report > div:nth-child(1)").first();
		if(describe_el!=null){
			describe = describe_el.text();
		}
			
		Elements reply_content_docs = html.select("#comments > div.comment-item > div.content.report-comment > p");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			String reply_content = reply_doc.text();
			if(reply_content!=null && !reply_content.trim().equals("")){
				reply_content_list.add(reply_content);
			}
		}
		
//		result.put(JsonObjectKeys.TITLE, title);
//		result.put(JsonObjectKeys.QUESTION, title);
//		result.put(JsonObjectKeys.DESCRIPTION, describe);
//		result.put(JsonObjectKeys.ANSWERS, reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		
		return result;
	}
	
	
}
