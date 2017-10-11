package com.petty.etl.parser;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class WangyiSportNewsParser {
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		String html = FileUtil.readFileToString("/Users/greshem/codes/myprojects/corpusetl/test.html");
		List<JSONObject> obs = parser(html, 111, "");
		for(JSONObject ob: obs){
			System.out.println(Constants.TITLE + ": \n" + ob.get(Constants.TITLE) + "\n");
			System.out.println(Constants.CONTENT + ": \n" + ob.get(Constants.CONTENT) + "\n");
			System.out.println(Constants.TAGS + ": \n" + ob.get(Constants.TAGS).toString() + "\n");
			System.out.println("posttime" + ": \n" + ob.get("posttime").toString() + "\n");
			System.out.println(Constants.SOURCE + ": \n" + ob.get(Constants.SOURCE).toString() + "\n");
			System.out.println("==================================================");
		}
	}
	
	/*
	 * url pattern
	 * http://sports.163.com/14/1028/22/A9M6C0LB00051CD5.html
	 */
	public static List<JSONObject> parser(String html_body, int source, String url) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		JSONObject result = new JSONObject();
		Document html = Jsoup.parse(html_body);
		if(html == null){
			return resultList;
		}
		
		// 抓去网页的标题
		// 尝试用ep-content-main去抓取
		Element tilte = html.select(".ep-content-main > h1").first();
		if(tilte == null){
			// 尝试用atc_title去抓取
			tilte = html.select(".atc_title > h1").first();
		}
		if(tilte == null){
			// 尝试用post_content_main去抓取
			tilte = html.select(".post_content_main > h1").first();
		}
		if(tilte == null){
			return resultList;
		}
		result.put(Constants.TITLE, tilte.text());
		
		// 抓取新闻发布的时间，转成Unix时间
		Element timeElement = html.select(".ep-time-soure.cDGray").first();
		String postTime = "";
		if(timeElement == null){
			timeElement = html.select(".post_time_source").first();
		}
		if(timeElement != null){
			String[] timeArray = timeElement.text().split("　");
			for(int i=0; i<timeArray.length; i++){
				if(timeArray[i].contains("来源")){
					break;
				}
				postTime += timeArray[i];
			}
		}
//		long postUnixtime = CalendarUtil.Datetime2Unix(postTime, "yyyy年MM月dd日HH:mm");
		result.put("posttime", postTime);
		
		// 抓取新闻的正文内容
		StringBuilder contentBuiler = new StringBuilder();
		Elements contents = html.select(".post_text > p");
		if(contents == null || contents.size() == 0){
			contents = html.select(".atc_bd > div > p");
		}
		if(contents == null || contents.size() == 0){
			contents = html.select(".end-text > p");
		}
		for(int i=1; i<contents.size(); i++){
			contentBuiler.append(contents.get(i).text().trim());
		}
		result.put(Constants.CONTENT, contentBuiler.toString().replace("　", " ").replaceAll(" +", " ").trim());
		
		// 抓取关键字
		JSONArray tagArray = new JSONArray();
		Elements keywords = html.select(".ep-keywords-main > div > span > a");
		// 如果抓不到相应的class，就去head的meta信息中去找
		if(keywords == null || keywords.size() == 0){
			Elements metas = html.head().getElementsByAttributeValue("name", "keywords");
			if(metas.size() >= 1){
				Element keyWordMeta = metas.get(0);
				String keyWords = keyWordMeta.attr("content");
				if(keyWords != null){
					String[] keyWordsArray = keyWords.split(",");
					for(int i=0; i<keyWordsArray.length; i++){
						// 如果是体彩类的新闻，就过滤掉
						if("竞彩".equalsIgnoreCase(keyWordsArray[i]) || "彩票".equalsIgnoreCase(keyWordsArray[i])){
							return resultList;
						}
						tagArray.add(keyWordsArray[i]);
					}
				}
			}
		}else{
			for(Element keyword: keywords){
				// 如果是体彩类的新闻，就过滤掉
				if("竞彩".equalsIgnoreCase(keyword.text()) || "彩票".equalsIgnoreCase(keyword.text())){
					return resultList;
				}
				tagArray.add(keyword.text());
			}
		}
		result.put(Constants.TAGS, tagArray);
		
		result.put(Constants.SOURCE, source);
		result.put(Constants.URL, url);
		
		if(!result.isEmpty()){
			resultList.add(result);
		}
		return resultList;
	}
}
