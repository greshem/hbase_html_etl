package com.petty.etl.parser;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SogouWenwenParser {
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		String html = FileUtil.readFileToString("/Users/greshem/codes/myprojects/corpusetl/test.html");
		List<JSONObject> obs = parser(html, 116, "http://wenwen.sogou.com/z/q431066335.htm?sw=%E4%BB%BB%E4%BD%95%E6%95%B0%E4%B9%98%E4%BB%A50%E7%AD%89%E4%BA%8E%E9%9B%B6%E5%90%97&ch=new.w.search.6&");
		for(JSONObject ob: obs){
			if(ob.has("related_question")){
				System.out.println("related_question" + ": \n" + ob.get("related_question") + "\n");
			}else{
				System.out.println(Constants.TITLE + ": \n" + ob.get(Constants.TITLE) + "\n");
				System.out.println(Constants.QUESTION + ": \n" + ob.get(Constants.QUESTION) + "\n");
				JSONArray answerArray = ob.getJSONArray(Constants.ANSWERS);
				System.out.println(Constants.ANSWERS);
				for(int i=0; i<answerArray.size(); i++){
					System.out.println(answerArray.getString(i));
				}
				System.out.println();
				System.out.println(Constants.TAGS + ": \n" + ob.get(Constants.TAGS).toString() + "\n");
				System.out.println(Constants.SOURCE + ": \n" + ob.get(Constants.SOURCE).toString() + "\n");
				System.out.println(Constants.URL + ": \n" + ob.get(Constants.URL).toString() + "\n");
			}
			System.out.println("==================================================");
		}
		System.out.println(obs.size());
	}
	
	/*
	 * url pattern
	 * http://wenwen.sogou.com/s/?w=%E5%85%AC%E8%9A%8A%E5%AD%90%E4%B8%BA%E4%BB%80%E4%B9%88%E4%B8%8D%E5%90%B8%E8%A1%80
	 * http://wenwen.sogou.com/z/q211897425.htm?sw=%E5%85%AC%E8%9A%8A%E5%AD%90%E4%B8%BA%E4%BB%80%E4%B9%88%E4%B8%8D%E5%90%B8%E8%A1%80&ch=new.w.search.8&
	 */
	public static List<JSONObject> parser(String html_body, int source, String url) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		Document html = Jsoup.parse(html_body);
		if(html == null){
			return resultList;
		}
		
		// 抓取原始的搜索问题
		String tmpURL = url;
		try {
			tmpURL = URLDecoder.decode(url, "utf-8");
		}catch(UnsupportedEncodingException e) {
			e.printStackTrace();
		}catch(IllegalArgumentException e) {
			System.out.println("Exception URL:\t" + url);
		} 
		String rawquery = getRawQuery(tmpURL);
		if(url.startsWith("http://wenwen.sogou.com/s/")){
			Elements elements = html.select(".result-item");
			for(int i=0; i<elements.size(); i++){
				Element item = elements.get(i);
				
				Element questionEle = item.select(".result-title.sIt_title").first();
				if(questionEle == null){
					continue;
				}
				String question = questionEle.text();
				
				Element answerEle = item.select(".result-summary").first();
				if(answerEle == null){
					continue;
				}
				String answer = answerEle.text();
				if(answer.endsWith("...")){
					int lastSentence = answer.lastIndexOf("。");
					if(lastSentence != -1){
						answer = answer.substring(0, answer.lastIndexOf("。")+1);
					}
				}
				
				Element tagEle = item.select(".result-info.sIt_info > a > span").first();
				if(tagEle == null){
					continue;
				}
				String tag = tagEle.text();
				
				JSONObject qaObject = new JSONObject();
				qaObject.put(Constants.TITLE, cleanString(rawquery));
				qaObject.put(Constants.QUESTION, cleanString(question));
				JSONArray answerArray = new JSONArray();
				JSONObject answerOb = new JSONObject();
				answerOb.put(Constants.CONTENT, cleanString(answer));
				answerOb.put(Constants.SELECT, 0);
				answerArray.add(answerOb);
				qaObject.put(Constants.ANSWERS, answerArray);
				JSONArray tagArray = new JSONArray();
				tagArray.add(tag);
				qaObject.put(Constants.TAGS, tagArray);
				qaObject.put(Constants.URL, tmpURL);
				qaObject.put(Constants.INCREFLAG, 1);
				qaObject.put(Constants.SOURCE, source);
				resultList.add(qaObject);
			}

			// 查找相关搜索
			Elements relatedSearch = html.select(".interrelated_search > table > tbody > tr > td > a");
			JSONArray relatedArray = new JSONArray();
			for(int i=0; i<relatedSearch.size(); i++){
				relatedArray.add(relatedSearch.get(i).text());
			}
			if(relatedArray.size() > 0){
				JSONObject relatedOb = new JSONObject();
				relatedOb.put("related_question", relatedArray);
				resultList.add(relatedOb);
			}
		}else if(url.startsWith("http://wenwen.sogou.com/z/")){
			Element titleElement = html.select(".question-tit").first();
			if(titleElement == null){
				return resultList;
			}
			Element rewardElement = titleElement.select(".ico-reward").first();
			if(rewardElement != null){
				rewardElement.remove();
			}
			String title = cleanString(titleElement.text());
			
			// 解析answer
			JSONArray answerArray = new JSONArray();
			// 满意答案
			Elements topAnswers = html.select(".question-main.satisfaction-answer");
			if(topAnswers != null){
				for(int j=0; j<topAnswers.size(); j++){
					Element topAnswer = topAnswers.get(j);
					JSONObject topAnswerOb = getAnswerInfo(topAnswer, url);
					if(!topAnswerOb.isEmpty()){
						answerArray.add(topAnswerOb);
					}
				}
			}
			// 精华答案
			Elements jingHuaAnswers = html.select(".question-main.jinghua-answer");
			if(jingHuaAnswers != null){
				for(int j=0; j<jingHuaAnswers.size(); j++){
					Element jingHuaAnswer = jingHuaAnswers.get(j);
					JSONObject jingHuaAnswerOb = getAnswerInfo(jingHuaAnswer, url);
					if(!jingHuaAnswerOb.isEmpty()){
						answerArray.add(jingHuaAnswerOb);
					}
				}
			}
			// 常规答案
			Elements defaultAnswerElements = html.select(".default-answer");
			for(int i=0; i<defaultAnswerElements.size(); i++){
				Element defaultAnswer = defaultAnswerElements.get(i);
				JSONObject answerOb = getAnswerInfo(defaultAnswer, url);
				if(!answerOb.isEmpty()){
					answerArray.add(answerOb);
				}
			}
			
			// 取得Tag信息
			Elements headElements = html.select(".question-head");
			JSONArray tagArray = new JSONArray();
			if(headElements != null){
				String tag = "";
				for(int j=0; j<headElements.size(); j++){
					Element head = headElements.get(j);
					Elements lineElements = head.select(".line");
					if(lineElements != null){
						for(int p=0; p<lineElements.size(); p++){
							Element line = lineElements.get(p);
							if("|".equalsIgnoreCase(line.text())){
								tag = line.nextElementSibling().text();
								break;
							}
						}
						if(!"".equalsIgnoreCase(tag)){
							break;
						}
					}
				}
				tagArray.add(tag);
			}
			
			JSONObject qaObject = new JSONObject();
			qaObject.put(Constants.TITLE, title);
			qaObject.put(Constants.QUESTION, title);
			qaObject.put(Constants.ANSWERS, answerArray);
			qaObject.put(Constants.TAGS, tagArray);
			qaObject.put(Constants.URL, tmpURL);
			qaObject.put(Constants.INCREFLAG, 1);
			qaObject.put(Constants.SOURCE, source);
			resultList.add(qaObject);
			
			// 查找相关搜索
			Elements relatedSearch = html.select(".similar_keywords.relate_search > ul > li > a");
			JSONArray relatedArray = new JSONArray();
			for(int i=0; i<relatedSearch.size(); i++){
				relatedArray.add(relatedSearch.get(i).text());
			}
			if(relatedArray.size() > 0){
				JSONObject relatedOb = new JSONObject();
				relatedOb.put("related_question", relatedArray);
				resultList.add(relatedOb);
			}
		}
		return resultList;
	}
	
	public static JSONObject getAnswerInfo(Element element, String url){
		JSONObject answerOb = new JSONObject();
		Element answerEle = element.select(".answer-con").first();
		if(answerEle == null){
			return answerOb;
		}
		
		Element replyElement = answerEle.select(".replenish").first();
		if(replyElement != null){
			replyElement.remove();
		}
//		Element supportEle = element.select(".operate-support ").first();
//		Element opposeEle = element.select(".operate-oppose").first();
//		int support=0, oppose=0;
//		if(supportEle != null){
//			try{
//				support = Integer.valueOf(supportEle.text());
//			}catch(NumberFormatException e){
//				System.out.println("url:\t" + url);
//			}
//			support = Integer.valueOf(supportEle.text());
//		}
//		if(opposeEle != null){
//			try{
//				oppose = Integer.valueOf(opposeEle.text());
//			}catch(NumberFormatException e){
//				System.out.println("url:\t" + url);
//			}
//			oppose = Integer.valueOf(opposeEle.text());
//		}
		answerOb.put(Constants.CONTENT, cleanString(answerEle.text()));
		answerOb.put(Constants.SELECT, 0);
//		answerOb.put("support", support);
//		answerOb.put("oppose", oppose);
		return answerOb;
	}
	
	public static String getRawQuery(String url){
		// http://www.bing.com/knows/search?FORM=BKACAI&mkt=zh-cn&q=%E7%82%9C%E7%82%9C
		if(url == null || "".equalsIgnoreCase(url)){
			return "";
		}
		String rawQuery = "";
		String[] array = url.split("\\?");
		if(array.length == 2){
			String params = array[1];
			String[] paramArray = params.split("&");
			for(int i=0; i<paramArray.length; i++){
				String[] keyValue = paramArray[i].split("=");
				if(keyValue.length == 2){
					if("w".equalsIgnoreCase(keyValue[0])){
						rawQuery = keyValue[1];
						break;
					}
				}
			}
		}
		return rawQuery;
	}
	
	public static String cleanString(String rawString){
		String string = rawString.replace("查看更多答案>>", "").replaceAll("\\{\\-+", "\\{-").trim();
		return string;
	}
}
