package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.CalendarUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.parser.DoubanBookParser;
import com.petty.etl.parser.DoubanMovieParser;
import com.petty.etl.parser.DoubanParser;

public class DoubanExtractor extends BaseExtractor {

	public List<JSONObject> extract(String data) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		JSONObject srcData = JSONObject.fromObject(data);
		String html = srcData.getString("html_body");		
		String srcurl = srcData.getString("url");						
		
		if (html == null) {
			return result;
		}
		
		JSONObject date = JSONObject.fromObject(srcData.getString(Constants.UPDATETIME)); 
		long update_time = 0l;
		if (String.valueOf(date.get("$date")).contains("T")) {
			String tt = String.valueOf(date.get("$date")).replace("T", " ").replace("Z", "");
			update_time = CalendarUtil.Datetime2Unix(tt, "yyyy-MM-dd HH:mm:ss.SSS");
		} else {
			update_time = date.getLong("$date");
		}
		
		//Parse html get question and answer list
		JSONObject qa = parse(html, srcurl);
		if (qa == null) {
			return result;
		}
		
		//Get Title
		String title = qa.getString(Constants.TITLE);
		
		//Get description
		String description = qa.getString(Constants.DESCRIPTION);
		
		//Get tags in the html		
		JSONArray tags = getTagList(html);		
		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, srcurl);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, update_time);
		result.add(qa);				
		
		//Add all reply2reply to result
		List<JSONObject> r2r = getReplyToReply(html, title, tags, description, srcurl, null, update_time);
		result.addAll(r2r);
		
		return result;
	}
	
	private JSONObject parse(String html, String srcurl) {
		JSONObject result = null;
		if(subjectDocument(srcurl)) {
			return parseSubject(html);
		}
		
		if(srcurl.startsWith("http://www.douban.com/group/topic/")){
			result = DoubanParser.groupParse(html);
		}else if(srcurl.startsWith("http://www.douban.com/note/")){
			result = DoubanParser.noteParse(html);
		}else if(srcurl.startsWith("http://book.douban.com/subject")){
			if(srcurl.contains("comment")){
				result = DoubanBookParser.parseComments(html);
			}else if(srcurl.contains("discussion")){
				result = DoubanBookParser.parseDiscussion(html);
			}else if(srcurl.contains("review")){
				result = DoubanBookParser.parseReview(html);
			}else{
				return null;
			}
		}else if(srcurl.startsWith("http://book.douban.com/review")){
			result = DoubanBookParser.parseReview(html);
		}else if(srcurl.startsWith("http://book.douban.com/annotation")){
			result = DoubanBookParser.parseAnnotation(html);
		}else if(srcurl.startsWith("http://movie.douban.com/subject")){
			if(srcurl.contains("comment")){
				result = DoubanMovieParser.parseComment(html);
			}else if(srcurl.contains("discussion")){
				result = DoubanMovieParser.parseDiscussion(html);
			}else if(srcurl.contains("tv_discuss")){
				result = DoubanMovieParser.parseTvDiscussion(html);
			}else if(srcurl.contains("episode")){
				result = DoubanMovieParser.parseEpisode(html);
			}else if(srcurl.contains("reviews")){
				result = DoubanMovieParser.parseSubjectReview(html);
			}else{
				return null;
			}
		}else if(srcurl.startsWith("http://movie.douban.com/review")){
			result = DoubanMovieParser.parseReview(html);
		}else{
			return null;
		}
				
		return result;
	}
	
	private boolean subjectDocument(String srcurl) {
		return Pattern.matches("^.+\\.douban\\.com/subject/\\d+/$", srcurl);
	}
	
	private JSONObject parseSubject(String html) {		
		JSONObject result = new JSONObject();
		//check whether has tags, if has get tags
		JSONArray tags = getTagList(html);
		if (tags.size() <= 0) {
			return null;
		}
		
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		String title = "";
		String description = "";
		JSONArray answers = new JSONArray();
		
		//get title <title>
		Element eTitle = htmlDoc.select("title").first();
		title = cleanTitle(eTitle.text());		
		
		//get description <div class="indent" id="link-report"> <span class="all hidden"> <p>
		Element link_report = htmlDoc.select("#link-report").first();
		if (link_report != null) {
			Element eDes;
			
			if ((eDes= link_report.select(".all").first()) != null) {
				Element eIntro = eDes.select("div.intro").first();
				
				if (eIntro != null) {
					//book.douban.com/subject/26576861/
					description = eIntro.text();
				} else {
					//music.douban.com/subject/26321569/
					description = eDes.text();
				}
			} else {
				if ((eDes = link_report.select("div.intro").first()) != null) {					
					//book.douban.com/subject/26451393/
					description = eDes.text();
					
				} else {
					eDes = link_report.select("span[property=v:summary]").first();
					if (eDes != null) {
						description = eDes.text();
					}
				}
			}				
		}
		
		//get comments <li class="comment-item">
		Elements comments = htmlDoc.select(".comment-item");
		for (Element comment:comments) {
			Element eAnswer = comment.select("p").first();
			if (eAnswer != null) {
				answers.add(eAnswer.text());
			}
		}
		
		Constants.parserResultBuilder(result, title, title, description, answers);
		
		return result;
	}
	
	private String cleanTitle(String title) {
		if (title.contains(" (豆瓣)")) {
			return title.replace(" (豆瓣)", "");
		}
		return title;
	}
	
	private List<JSONObject> getReplyToReply(String html, String title, JSONArray tags, String description, String srcurl, String category, long update_time) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
    	Elements reply_doc = htmlDoc.select("div.reply-doc.content");
    	
    	//Extract all quote and its reply
    	for(Element rd : reply_doc) {
    		JSONObject result = new JSONObject();
    		
        	String question = "";
        	JSONArray content_answers = new JSONArray();
        	
        	Element reply_quote = rd.select("div.reply-quote").first();
        	if (reply_quote == null) {
        		continue;
        	}
    		Element quote = reply_quote.select("span.all").first();   		
    		Element quoteReply = rd.select("p").first(); 
    		if(quoteReply != null){
        		question = quote.text();
        		content_answers.add(quoteReply.text());
        		
        		result.put(Constants.TITLE, title);
        		result.put(Constants.QUESTION, question);
        		result.put(Constants.ANSWERS, content_answers);
        		result.put(Constants.TAGS, tags);
        		result.put(Constants.DESCRIPTION, description);
        		result.put(Constants.URL, srcurl);
        		result.put(Constants.CATEGORY, category);
        		result.put(Constants.DOCUMENTSOURCE, getDataSource());
        		result.put(Constants.UPDATETIME, update_time);
        		
        		resultList.add(result); 
    		}  		
    	}
    	
		return resultList;
	}
	
	public JSONArray getTagList(String html) {
		JSONArray result = new JSONArray();
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		Element tagElement = null;
		if ((tagElement = htmlDoc.select("div#db-tags-section").first())!=null) {
			;
		} else if ((tagElement = htmlDoc.select("div.tags-body").first())!=null) {
			;
		} 
		
		if (tagElement == null) {
			return result;
		}
				
		Elements tags = tagElement.select("a");						
		for (Element tag:tags) {
			result.add(tag.text());				
		}		
		
		return result;
	}
	
	protected int getDataSource() {
		dataSource = DOUBAN;
		return dataSource;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String category,
			HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		
		if (htmlBody == null) {
			return result;
		}
		long update_time = Date.parse(updateTime);
		
		//Parse html get question and answer list
		JSONObject qa = parse(htmlBody, url);
		if (qa == null) {
			return result;
		}
		
		//Get Title
		String title = qa.getString(Constants.TITLE);
		
		//Get description
		String description = qa.getString(Constants.DESCRIPTION);
		
		//Get tags in the html		
		JSONArray tags = getTagList(htmlBody);		
		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, url);
		qa.put(Constants.CATEGORY, category);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, update_time);
		result.add(qa);				
		
		//Add all reply2reply to result
		List<JSONObject> r2r = getReplyToReply(htmlBody, title, tags, description, url, category, update_time);
		result.addAll(r2r);
		
		return result;
	}
}
