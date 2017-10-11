package com.petty.etl.parser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.constant.Constants;

public class DoubanParser {
	
	
	/*
	 * url pattern
	 * http://www.douban.com/group/topic/10002436/?author=1
	 */
	public static JSONObject groupParse(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element content = html.select("#content").first();
		if(content != null){
			Element title_el = content.select("h1").first();
			if(title_el!=null){
				title = title_el.text();
			}
		}
		//result.put("title", title);
		
		Element topic_content = html.select("#link-report > div").first();
		if(topic_content != null){
			describe = topic_content.text();
		} else {		
			topic_content = html.select(".inactive").first();
			if (topic_content != null) {
				topic_content = html.select(".inactive").first().select("div").last();
				if (topic_content != null) {
					describe = topic_content.text();
				}
			}
			
			topic_content = html.select("#link-report").first();
			if (topic_content != null) {
				describe += " "+topic_content.text();
			}
		}
		//result.put("describe", describe);
		
		Elements comments = html.select("#comments");
		Elements reply_docs = comments.select("div.reply-doc.content");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_docs){
			
			Element reply_content = reply_doc.getElementsByTag("p").first();
			if(reply_content != null){
				if(reply_content.text().length() <= 5000){
					reply_content_list.add(reply_content.text());
				}
			}
		}
		//result.put("reply", reply_content_list);
		
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
	/*
	 * url pattern
	 * http://www.douban.com/note/10006741/
	 */
	public static JSONObject noteParse(String html_body){
		JSONObject result = new JSONObject();
		
		String title = "";
		String describe = "";
		
		org.jsoup.nodes.Document html = Jsoup.parse(html_body);
		
		//parse title and describe
		Element title_el = html.select("div.note-header.note-header-container > h1").first();
		if(title_el!=null){
			title = title_el.text();
		}
		//result.put("title", title);
		
		Element topic_content = html.select("#link-report").first();
		if(topic_content != null){
			describe = topic_content.text();
		}
		//result.put("describe", describe);
		
		Elements reply_content_docs = html.select("#comments > div.comment-item > div.content.report-comment > p");
		
		JSONArray reply_content_list = new JSONArray();
		
		for(Element reply_doc:reply_content_docs){
			if(reply_doc != null){
				reply_content_list.add(reply_doc.text());
			}
		}
		//result.put("reply", reply_content_list);
		Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		return result;
	}
	
	
}
