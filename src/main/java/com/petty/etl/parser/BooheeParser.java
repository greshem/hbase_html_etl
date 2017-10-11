package com.petty.etl.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class BooheeParser {
	
	public static void main(String[] args) {
		String html = FileUtil.readFileToString("/Users/greshem/codes/myprojects/corpusetl/test.html");
		List<JSONObject> obs = parser(html);
		for(JSONObject ob: obs){
			System.out.println(Constants.TITLE + ": \n" + ob.get(Constants.TITLE) + "\n");
			System.out.println(Constants.QUESTION + ": \n" + ob.get(Constants.QUESTION) + "\n");
			System.out.println(Constants.ANSWERS + ":");
			JSONArray array = (JSONArray) ob.get(Constants.ANSWERS);
			for(int i=0;i<array.size(); i++){
				System.out.println("\t" + array.get(i));
			}
			System.out.println(Constants.TAGS + ": \n" + ob.get(Constants.TAGS) + "\n");
			System.out.println(Constants.DESCRIPTION + ": \n" + ob.get(Constants.DESCRIPTION) );
			System.out.println("==================================================");
		}
	}
	
	public static List<JSONObject> parser(String html_body) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		JSONObject result = new JSONObject();
		String htmlText = html_body.replace("&nbsp;", " ");
		Document html = Jsoup.parse(htmlText);
		if(html == null){
			return null;
		}
		
		Element title = html.select(".post-title > h1").first();
		String titleText = "";
        String tagsText = "";
		if(title != null){
			titleText = title.text();
			Pattern pattern = Pattern.compile("(?<=\\()[^\\)]+");
	        Matcher matcher = pattern.matcher(titleText);
	        String pageNum = "";
	        while(matcher.find()){
	        	pageNum = matcher.group();
	        }
	        if(pageNum.startsWith("第") && pageNum.endsWith("页")){
		        titleText = titleText.replace("("+pageNum+")", "");
	        }
	        if(titleText.endsWith("]")){  //分享一下郑多燕的《8周健康减肥计划》[减肥方法,减肥知识]
	        	Pattern patternTag = Pattern.compile("\\[(.*?)\\]");
		        Matcher matcherTag = patternTag.matcher(titleText);
		        while(matcherTag.find()){
		        	tagsText = matcherTag.group();
		        }
		        titleText = titleText.replace(tagsText, "");
	        }
			result.put(Constants.TITLE, titleText);
			result.put(Constants.QUESTION, titleText);
		}else{
			return resultList;
		}
		
		JSONArray tags = new JSONArray();
		if(!"".equalsIgnoreCase(tagsText)){
			String[] tagArray = tagsText.replace("[", "").replace("]", "").split(",");
			for(int i=0; i<tagArray.length; i++){
				String tag = tagArray[i];
				if("原创".equalsIgnoreCase(tag) || "转载".equalsIgnoreCase(tag)){
					continue;
				}
				tags.add(tag);
			}
		}
		result.put(Constants.TAGS, tags);
		
		String descriptionText = "";
		Elements postElements =  html.select(".content.post-view > table");
		if(postElements == null){
			return resultList;
		}

	    JSONArray replayArray = new JSONArray();
		for(int m=0; m<postElements.size(); m++){
			Element post = postElements.get(m);
			Element replyPosinfo = post.select(".post-info").first();
			if(replyPosinfo == null){
				continue;
			}
			
			Element replyPos = replyPosinfo.select(".right-float.post-index > span").first();
			if(replyPos == null){
				continue;
			}
			String replyPosNum = replyPos.text();
			if("1".equalsIgnoreCase(replyPosNum)){ // 1楼被认为是description，其他的是回复
				Element desc = post.select(".contents-show").first();
				if(desc != null){
					descriptionText = desc.text();
				}
			}else{
				Element reply = post.select(".contents-show").first();
				if(reply != null){
					Element blockquote = reply.select("blockquote").first();
					if(blockquote != null){
						List<JSONObject> replyToReply = getReplyToReply(reply, titleText, tags, descriptionText);
						if(replyToReply.size() > 0){
							resultList.addAll(replyToReply);
						}
					}else{
						String replyText = reply.text();
						replayArray.add(replyText);
					}
				}
			}
		}
		result.put(Constants.DESCRIPTION, descriptionText);
		result.put(Constants.ANSWERS, replayArray);
		resultList.add(result);
		return resultList;
	}
	
	public static List<JSONObject> getReplyToReply(Element reply, String title, JSONArray tags, String description){
		List<JSONObject> list = new ArrayList<JSONObject>();
		JSONObject replyOb = new JSONObject();
		JSONArray replayToReplyArray = new JSONArray();
		
		Element blockquote = reply.select("blockquote > div").first();
		Element blockInblock = blockquote.select("blockquote").first();
		String question = "";
		if(blockInblock != null){ //回复中的引用还包含引用
			question = getReplyString(blockquote, true);
		}else{
			question = blockquote.text();
		}
		
		String replyText = getReplyString(reply , false);
		replayToReplyArray.add(replyText);
		replyOb.put(Constants.TITLE, title);
		replyOb.put(Constants.QUESTION, question);
		replyOb.put(Constants.ANSWERS, replayToReplyArray);
		replyOb.put(Constants.TAGS, tags);
		replyOb.put(Constants.DESCRIPTION, description);
		list.add(replyOb);
		return list;
	}

	public static String getReplyString(Element reply, boolean questionFlag){
		StringBuilder builder = new StringBuilder();
		List<Node> childNodes = reply.childNodes();
		for(int i=0; i<childNodes.size(); i++){
			Node childNode = childNodes.get(i);
			if("p".equalsIgnoreCase(childNode.nodeName())){
				List<Node> nodes = childNode.childNodes();
				for(int j=0; j<nodes.size(); j++){
					Node node = nodes.get(j);
					if("#text".equalsIgnoreCase(node.nodeName())){
						builder.append(node.toString());
					}
				}
			}
		}
		return builder.toString().trim();
	}
}
