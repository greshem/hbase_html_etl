package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class ShuolianaiExtractor extends BaseExtractor {
	private String html;

	// in result
	private long update_time;
	private String srcurl;
	private String title="";
	private String description;
	private JSONArray tags;

	@Override
	public List<JSONObject> extract(String data) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag,
			HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		html = htmlBody;
		srcurl = url;
		tags = new JSONArray();

		if (html == null) {
			return null;
		}

		update_time = 0L;

		// Parse html get question and answer list
		JSONObject qa = parse();
		if (qa == null) {
			return null;
		}

		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, srcurl);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, update_time);
		qa.put(Constants.CATEGORY, "lianai");

		result.add(qa);
		
		//Add all reply2reply to result
		List<JSONObject> r2r = getReplyToReply();
		result.addAll(r2r);

		return result;
	}
	
	public JSONObject parse() {
		if (html == null || html.trim().equals("")) {
			return null;
		}
		JSONObject result = new JSONObject();
		JSONArray reply_content_list = new JSONArray();

		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		Element elTitle = htmlDoc.select(".tviewsfirst_name").first();
		if (elTitle == null) {
			return null;			
		}
		
		Elements elATitles = elTitle.select("a");
		if (elATitles == null) {
			return null;
		}
		
		for (Element elA:elATitles) {
			if (elATitles.indexOf(elA) == 0) {
				continue;
			} else {
				title = elA.text();
			}
		}
		
		Elements replies = htmlDoc.select("div.t_fsz");
		for (Element reply:replies) {
			Element elContent = reply.select(".t_f").first();
			
			if (elContent == null) {
				continue;
			}
			
			if (replies.indexOf(reply) == 0) {
				description = elContent.text();
			} else {
				reply_content_list.add(elContent.text());
			}
		}
		
		try {
			Constants.parserResultBuilder(result, title, title, description, reply_content_list);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	private List<JSONObject> getReplyToReply() {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		String question="";
		String answer="";
		
		Elements replies = htmlDoc.select("div.t_fsz");
		for (Element reply:replies) {
			JSONObject result = new JSONObject();
			JSONArray reply_content_list = new JSONArray();
			Element elQuestion = reply.select("blockquote").first();
			
			if (elQuestion == null) {
				continue;
			}			
			
			question = elQuestion.text();
			
			
			Element elAnswer = reply.select(".t_f").first();
			if (elAnswer == null) {
				continue;
			}
			
			answer = elAnswer.text();
			answer = answer.replace(question, "").trim();
			reply_content_list.add(answer);
			System.out.println("elAnswer:"+answer);
			
			String r = "\\S+\\s+发表于\\s+\\d+-\\d+-\\d+\\s+\\d+:\\S+\\s+";
			question = question.replaceAll(r, "");
			System.out.println("elQuestion:"+question);
			
			result.put(Constants.TITLE, title);
    		result.put(Constants.QUESTION, question);
    		result.put(Constants.ANSWERS, reply_content_list);
    		result.put(Constants.TAGS, tags);
    		result.put(Constants.DESCRIPTION, description);
    		result.put(Constants.URL, srcurl);
    		result.put(Constants.DOCUMENTSOURCE, getDataSource());
    		result.put(Constants.UPDATETIME, update_time);
    		result.put(Constants.CATEGORY, "lianai");
    		
    		resultList.add(result);
		}		
		
		return resultList;
	}

	@Override
	protected int getDataSource() {
		dataSource = SHUOLIANAI;
		return dataSource;
	}

}
