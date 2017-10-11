package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.CalendarUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TianyaBbsExtractor extends BaseExtractor {
	private String html;
	
	//in result
	private long update_time;
	private String srcurl;
	private JSONArray tags;
	
	String title = "";
	String describe = "";
	
	public List<JSONObject> extract(String data) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		JSONObject srcData = JSONObject.fromObject(data);
		html = srcData.getString("html_body");		
		srcurl = srcData.getString("url");	
		tags = new JSONArray();
		
		if (html == null || html.trim().equals("")) {
			return null;
		}
		
		JSONObject date = JSONObject.fromObject(srcData.getString(Constants.UPDATETIME)); 
		if (String.valueOf(date.get("$date")).contains("T")) {
			String tt = String.valueOf(date.get("$date")).replace("T", " ").replace("Z", "");
			update_time = CalendarUtil.Datetime2Unix(tt, "yyyy-MM-dd HH:mm:ss.SSS");
		} else {
			update_time = date.getLong("$date");
		}
		
		//Parse html get question and answer list
		JSONObject qa = parse();
		if (qa == null) {
			return null;
		}
		
		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, srcurl);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, update_time);
		
		result.add(qa);
		
		List<JSONObject> rr = getReplyToReply(html);
		
		result.addAll(rr);
		
		List<JSONObject> rr1 = getCommentToReply(html);
		
		result.addAll(rr1);
		
		return result;
	}
	
	public JSONObject parse(){
		JSONObject result = new JSONObject();		
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		
		//parse title and describe
		Element title_el = htmlDoc.select("#post_head > h1 > span.s_title").first();
		if(title_el!=null){
			title = title_el.text();
		}
		
		if (title.equals("")) {
			return null;
		}
		
		Element describe_el = htmlDoc.select(".host-item").first();
		boolean hasDescription = false;
		if (describe_el != null) {
			Element content = describe_el.select("div.bbs-content").first();
			if (content != null) {
				describe = removeUselessString(content.text());
				hasDescription = true;
			}
		}
		
		JSONArray replies = new JSONArray();
		Elements answers_el = htmlDoc.select(".atl-item");
		if(answers_el!=null){
			for(int i=0 ; i<answers_el.size() ; i++){
				Element answer_el = answers_el.get(i);
				if (i == 0 && hasDescription) {
					continue;
				}
				
				Element content = answer_el.select("div.bbs-content").first();
				if (content == null) {
					continue;
				}
				
				try {
					replies.add(removeUselessString(content.text()));	
				} catch (Exception e) {
					System.out.println("url is:"+srcurl);
					e.printStackTrace();
				}
			}
		}
		
		Constants.parserResultBuilder(result, title, title, describe, replies);
		return result;
	}
	
	public String removeUselessString(String sentence) {		
		//remove below strings
		//[来自UC浏览器]
		//@海风爱你 19楼 2014-12-16 16:44
		//@zhoubojwjy 2014-12-16 17:30:45
		String result;
		String r1 = "\\[来自\\S+\\]";
		
		result = sentence.replaceAll(r1, "");
		
		char c[] = result.toCharArray();
		for (int i = 0 ; i < c.length; ++i) {
			if (c[i] == '\u3000') {
	            c[i] = ' ';
	        } else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
	            c[i] = (char) (c[i] - 65248);
	        }
		}
		
		result = new String(c).trim();

		String r2 = "@\\S+\\s+(\\d+楼\\s+|楼主\\s+)?\\d+-\\d+-\\d+\\s+\\d+:\\S+\\s+";
		result = result.replaceAll(r2, "");
		
		result = result.replaceAll("\\s+", " ");
		
		//回复第305楼(作者:@huang草 于 2013-09-26 23:10)		
		//String r4 = "回复第\\d+楼\\(作者:\\s*@\\S+\\s+于\\s+\\d+-\\d+-\\d+\\s+\\d+:\\S+\\)";
		String r3 = "回复第\\d+楼\\(作者:\\s*@\\S+.{1,5}于.{1,5}\\d+-\\d+-\\d+.{1,5}\\d+:\\S+\\)";
		result = result.replaceAll(r3, "");
		
		//回复第1441楼(作者:@樊家……
		//String r6 = "回复第\\d+楼\\(作者:\\s*@\\S+\\s*……";
		String r4 = "回复第\\d+楼\\(作者:(\\s*@\\S+)?\\s*……";
		result = result.replaceAll(r4, "");
		
		//回复第121楼, 别老换天涯帐号
		String r5 = "回复第\\d+楼,\\s*";
		result = result.replaceAll(r5, "");
		
		//作者:cagcag 回复日期:2012-01-27 23:45:58 回复
		//作者:cagcag 回复日期:2012-01-27 23:45:58
		//作者:cagcag 回复日期:
		//String r6 = "作者:\\s{0,3}\\S+\\s{0,3}回复日期:\\s{0,3}\\d+-\\d+-\\d+\\s{0,3}\\d+:\\d+:\\d+";
		//String r6 = "作者:(\\s{0,3}\\S+.{0,1})?(时间:)?";
		//result = result.replaceAll(r6, "");
		
		//作者:回复日期:
		//作者:我的菩提 时间: 
		//回复日期：2012-5-20 15:05:00
		//String r7 = "作者:(\\S+.{0,3})?(回复日期:|时间:)?";
		//String r7 = "回复日期:(\\d+-\\d+-\\d+.{1,3}\\d+:\\S+\\s{0,3})?(回复)?";
		//result = result.replaceAll(r7, "");
		
		//2013-08-25 19:14:34
		//String r9 = "\\d+-\\d+-\\d+.{1,3}\\d+:\\d+:\\d+";
		//result = result.replaceAll(r9, "");
		
		String rl = "@\\S+\\s*(\\d+楼\\s+|楼主\\s+)?";
		
		result = result.replaceAll(rl, "");
				
		return result.trim();
	}
	
	public String removeUselessFromQuestion(String sentence) {		
		//remove below strings
		//[来自UC浏览器]
		//@海风爱你 19楼 2014-12-16 16:44
		//@zhoubojwjy 2014-12-16 17:30:45
		String result;
		
		if (sentence.contains("-----")||sentence.contains("—————")
				||sentence.contains("======")||sentence.contains("_____")){
			result = "";
			return result;
		}
		
		//来自:Android客户端
		//来自:QQ浏览器
		//来自:手机版
		//来自Android客户端 | 举报 | 回复		
		String r1 = "来自:(\\S+客户端|\\S+浏览器|手机版)";
		String r2 = "来自\\S+客户端 \\| 举报 \\| 回复";
		r2 = r1+"|"+r2;
		result = sentence.replaceAll(r2, "");
		
		//作者:cagcag 回复日期:2012-01-27 23:45:58 回复
		//作者:cagcag 回复日期:2012-01-27 23:45:58 
		//作者:回复日期:2011-6-29 21:03:00
		//作者:回复日期:2011-8-3 08:24:00 
		//作者:我本善良e 时间:2013-12-29 21:40:58 
		//作者:花园情人时间:2015-01-1816:04:34 
		//作者:吃饱穿暖就好 提交日期:2012-08-13 22:54:13    16# 在这谈车论驾的时
		//作者:兔宅子 提交日期:2009-12-6 12:11:00 访问:954 回复:65
		//作者:中毒小仙 提交日期:2009-12-9 16:30:00 
		//作者:用户名1111 提交日期:2009-7-2		
		//作者:毛彭门外野狐说 发表日期:2010-11-15 14:07:00 回复 
		String r6 = "\\S{0,20}\\s{0,3}\\s{0,3}\\S{2,4}:\\s{0,3}\\d+-\\d+-\\d+\\s{0,3}(\\d+:\\d+:\\d+)?\\s{0,2}(回复)?";
		result = result.replaceAll(r6, "");
		
		//回复第14楼(作者:于 ……
		//回复第595楼(作者: ...
		//回复第162楼(作者:于 2013-08-2……
		//回复第1783楼(作者:于 2013-05……
		//回复第53楼(作者:_暗黑圣堂 于 2012-04-19 22:50)
		//String r7 = "回复第\\d{0,10}楼{0,1}\\({0,1}(作者:\\S*\\s{0,1}(于)?\\s{0,1}(\\d|-)*\\s{0,1}(\\d|:)*\\s{0,1}(\\.|…)*)?\\){0,1}";
		
		String r7 = "回复第\\d{0,10}楼{0,1}\\({0,1}((作者|楼主):\\S*\\s{0,1}(于)?(\\d|-|\\s|:|\\.|…)*)?\\){0,1}";
		result = result.replaceAll(r7, "");
		
		
		//作者:jh201312012014-05-26 21:59:27
		//作者:批量马甲2012-12-2509:44:25
		//作者:空山雪月苍茫2013-08-29 15:56
		//作者: xsy2006
		//作者:玫瑰与悲伤
		String r8 = "作者:\\s{0,1}\\S*\\s{0,1}(\\d|:)*";
		result = result.replaceAll(r8, "");
				
		//猜叔 2011-8-3 8:20:00 
		//2012-09-18 23……
		//2013-11-07 ……
		//201……
		//2014-08-21 13:32
		//......
		//国剧盛典不该是12月30号吗?
		//String r9 = "(\\d|-|:|\\s|…|\\.){1,30}";
		String r9 = "^(\\d|-|:|\\s|…|\\.){1,30}$";
		result = result.replaceAll(r9, "");
		
		//【染头发】
		//「天神右翼」		
		String r10 = "【|】|「|」";
		result = result.replaceAll(r10, "");
		//http://img3.laibafile.cn/getimgXXX/3/3/photo3/2012/2/12/middle/86605293_24265620_middle.jpg......
		String r11 = "http://\\S+";
		result = result.replaceAll(r11, "");
		
		//猜叔 2011-8-3 8:20:00
		//13:01:54 aa
		//String r12 = "\\d+-\\d+-\\d+\\s{0,3}(\\d+:\\d+:\\d+)?";
		String r12 = "\\d+-\\d+-\\d+\\s{1,3}\\d+:\\d+:\\d+\\s{1,3}|\\d+:\\d+:\\d+\\s{1,3}";
		result = result.replaceAll(r12, "");
		
		
		
		//xique32 xique32 当前离线 62379楼 楼主| 发表于 2014-10-27 22:47 | ?云?朵? 发表于 2014-10-27 19:12
		//本帖发自天涯社区手机客户端
		//[发自Android客户端-贝客悦读]
		
		
		//作者:回复日期:2011-6-29 21:03:00
		//作者:回复日期:2011-8-3 08:24:00 猜叔 2011-8-3 8:20:00 
		//作者:我本善良e 时间:2013-12-29 21:40:58 
		//作者:花园情人时间:2015-01-1816:04:34 
		//作者:吃饱穿暖就好 提交日期:2012-08-13 22:54:13    16# 在这谈车论驾的时	
		//作者:兔宅子 提交日期:2009-12-6 12:11:00 访问:954 回复:65
		//作者:中毒小仙 提交日期:2009-12-9 16:30:00 
		//作者:用户名1111 提交日期:2009-7-2
		
		//作者:毛彭门外野狐说 发表日期:2010-11-15 14:07:00 回复 
		//String r10 = "作者:回复日期:\\s{0,3}\\d+-\\d+-\\d+\\s{0,3}\\d+:\\d+:\\d+(\\s{0,2})?";
		//String r11 = "\\S{1,2}:\\s{0,3}\\S+\\s{0,3}(发表日期:|时间:|提交日期:)\\s{0,3}\\d+-\\d+-\\d+\\s{0,3}\\d+:\\d+:\\d+(\\s{0,2})?(回复)?";
		//r11 = r10+"|"+r11;
		//result = result.replaceAll(r11, "");
				
		return result.trim();
	}
	
	public List<JSONObject> getReplyToReply(String html_content) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html_content);
		String couldSplit = ".*------+.*|.*———————+.*|.*=========+.*";
		String splitby = "------+|———————+|=========+";
		
		Elements atl_items = htmlDoc.select(".atl-item");
		for (Element atl_item : atl_items) {
			Element el_content = atl_item.select(".bbs-content").first();
			if (el_content == null) {
				continue;
			}
			
			String content = el_content.text();
			
			if (!Pattern.matches(couldSplit, content)) {
				continue;
			}
			
			String sentences[] = content.split(splitby);

			for (int i = 0; i < sentences.length - 1; ++i) {
				JSONObject result = new JSONObject();
				JSONArray answerList = new JSONArray();
				String question = removeUselessString(sentences[i]);
				String answer = removeUselessString(sentences[i+1]);
				
				//String question = sentences[i];
				//String answer = sentences[i+1];	
				
				answerList.add(answer);
				
				if (question.equals("")) {
					question = title;
				}
				result.put(Constants.TITLE, title);
	    		result.put(Constants.QUESTION, question);
	    		result.put(Constants.ANSWERS, answerList);
	    		result.put(Constants.TAGS, tags);
	    		result.put(Constants.DESCRIPTION, describe);
	    		result.put(Constants.URL, srcurl);
	    		result.put(Constants.DOCUMENTSOURCE, getDataSource());
	    		result.put(Constants.UPDATETIME, update_time);		
	    		
	    		resultList.add(result);
			}
		}
		
		return resultList;
	}
	
	public List<JSONObject> getCommentToReply(String html_content) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();		
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html_content);
		
		//every data element "atl-item"		
    	Elements atl_items = htmlDoc.select(".atl-item");
    	for (Element atl_item : atl_items) {
    		JSONObject result = new JSONObject();
    		String question = "";
    		
    		//"bbs-content" as question in "atl-item"
    		//ir-list -> ul -> li -> ir-content as answer
    		Element ir_list = atl_item.select(".ir-list").first();
    		if (ir_list == null) {
    			continue;
    		}   		
    		
    		Element el_question = atl_item.select(".bbs-content").first();
    		if (el_question == null) {
    			continue;
    		}
    		question = removeUselessString(el_question.text());
    		if (question.equals("")) {
    			question = title;
    		}
    		
    		Elements answers = ir_list.select("li");
    		if (answers == null) {
    			continue;
    		}

    		JSONArray content_answers = new JSONArray();
    		for (Element answer : answers) {    
    			if (answer.select(".ir-content").first() == null) {
    				System.out.println("answer does not have ir-content,url is:"+srcurl);
    				continue;
    			}
    			
    			content_answers.add(removeUselessString(answer.select(".ir-content").first().text()));

    		}
    		
    		result.put(Constants.TITLE, title);
    		result.put(Constants.QUESTION, question);
    		result.put(Constants.ANSWERS, content_answers);
    		result.put(Constants.TAGS, tags);
    		result.put(Constants.DESCRIPTION, describe);
    		result.put(Constants.URL, srcurl);
    		result.put(Constants.DOCUMENTSOURCE, getDataSource());
    		result.put(Constants.UPDATETIME, update_time);

    		resultList.add(result); 
    	}
		
		return resultList;
	}
	
	public JSONObject clearInvalidQuestionContent(String data) {
		JSONObject srcData = JSONObject.fromObject(data);		
		String question = srcData.getString("question");
		
		question = removeUselessFromQuestion(question);
		if (question.equals("")) {
			return null;
		}
		
		JSONObject result = srcData;
		//result.remove(Constants.QUESTION);
		result.put(Constants.QUESTION, question);
		
		return result;
	}
	
	protected int getDataSource() {
		dataSource = TIANYA;
		return dataSource;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		return null;
	}
}
