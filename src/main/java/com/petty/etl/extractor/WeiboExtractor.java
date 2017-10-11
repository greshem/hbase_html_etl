package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import com.petty.etl.constant.Constants;


public class WeiboExtractor extends BaseExtractor {
	//source in result
	
	private String html;
	
	
	//in result
	private long update_time;
	private String srcurl;

	private String description;
	private JSONArray tags;
	private JSONArray answers;
	

	public List<JSONObject> extract(String data) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		
		description = "";
		tags = new JSONArray();
		answers = new JSONArray();
		
		return result;
	}
	
	private JSONObject parse() {
		JSONObject result = null;
		String question = getQuestionFromHTML(html);
		Constants.parserResultBuilder(result, question, question, description, answers);
				
		return result;
	}
	
	public String getQuestionFromHTML(String html) {
		String question = "";
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		Element subject = htmlDoc.select("div#M_").first();
		Element q = subject.select("span.ctt").first();
		
		question = q.text();
		
		return question;
	}
	
	protected int getDataSource() {
		dataSource = WEIBO;
		return dataSource;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		return null;
	}
}
