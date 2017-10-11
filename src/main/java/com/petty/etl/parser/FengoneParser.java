package com.petty.etl.parser;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class FengoneParser {
	
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
		html_body = html_body.replace("<br>", "").replace("&nbsp;", "");
		Document html = Jsoup.parse(html_body);
		if(html == null){
			return null;
		}
		
		Element title = html.select("#thread_subject").first();
		if(title != null){
			result.put(Constants.TITLE, title.text());
			result.put(Constants.QUESTION, title.text());
		}else{
			return resultList;
		}
		
		JSONArray tags = new JSONArray();
		result.put(Constants.TAGS, tags);
		
		
		String descriptionText = "";
		Element postElement =  html.select("#postlist").first();
		if(postElement == null){
			return resultList;
		}
		Elements replyElements =  html.select("td.t_f");
		List<Node> postList = postElement.childNodes();
		int label = 0;

	    JSONArray replayArray = new JSONArray();
		for(int m=0; m<postList.size(); m++){
			Node childNode = postList.get(m);
			if(!"div".equalsIgnoreCase(childNode.nodeName())){
				continue;
			}else if("postlistreply".equalsIgnoreCase(childNode.attr("id"))){
				continue;
			}
			if((replyElements.size() - 1) < label){
				continue;
			}
			Element replyElement = replyElements.get(label);
			List<JSONObject> replyList = new ArrayList<JSONObject>();
			List<Node> childNodes = replyElement.childNodes();
			
			boolean descFlag = false;
			if(label == 0){
				Element desc = html.select("div.pi > strong").first();
				if(desc != null){
					if("楼主".equalsIgnoreCase(desc.text())){
						descFlag = true;
					}
				}
			}
			if(descFlag){// 首页包含description
				descriptionText = descriptionText + getChildString(childNodes).trim();
			}else{
				boolean hasReply = false;
				for(Node node: childNodes){
					if(node.toString().contains("quote")){
						hasReply = true;
						break;
					}
				}
				if(!hasReply){
					String reply = getChildString(childNodes);
					reply = reply.replaceAll("\\{[^}]*\\}", "").trim();
					if(reply != null && !"".equalsIgnoreCase(reply)){
						replayArray.add(reply);
					}
				}else{
					replyList = getReplyToReply(childNodes, title.text(), tags, descriptionText);
				}
				if(replyList != null && replyList.size()>0){
					resultList.addAll(replyList);
				}
			}
			label++;
		}
		result.put(Constants.DESCRIPTION, descriptionText);
		result.put(Constants.ANSWERS, replayArray);
		resultList.add(result);
		return resultList;
	}
	
	public static List<JSONObject> getReplyToReply(List<Node> childNodes, String title, JSONArray tags, String description){
		List<JSONObject> list = new ArrayList<JSONObject>();
		JSONObject replyOb = new JSONObject();
		JSONArray replayToReplyArray = new JSONArray();
		int childNum = childNodes.size();
		int pos = 0;
		boolean hasReplyToReply = false;
		for(int i=0; i<childNum; i++){
			Node childNode = childNodes.get(i);
			boolean foundFlag = false;
			if("div".equalsIgnoreCase(childNode.nodeName()) && "quote".equalsIgnoreCase(childNode.attr("class"))){
				List<Node> nodeList = childNode.childNodes().get(0).childNodes();
				for(int j=0; j<nodeList.size(); j++){
					if("#text".equalsIgnoreCase(nodeList.get(j).nodeName())){
						replyOb.put(Constants.TITLE, title);
						replyOb.put(Constants.QUESTION, nodeList.get(j).toString());
						pos = i + 1;
						foundFlag = true;
						break;
					}
				}
				if(foundFlag){
					break;
				}else{// 如果回复中的回复中，引用的部分是空文字，则直接返回
					return list;
				}
			}
		}
		for(int i=pos; i<childNum; i++){
			if("#text".equalsIgnoreCase(childNodes.get(i).nodeName())){
				String reply = childNodes.get(i).toString().trim();
				reply = reply.replaceAll("\\{[^}]*\\}", "").trim();
				if(reply != null && !"".equalsIgnoreCase(reply)){
					replayToReplyArray.add(reply);
					hasReplyToReply = true;
				}
				
			}
		}
		if(hasReplyToReply){
			replyOb.put(Constants.ANSWERS, replayToReplyArray);
			replyOb.put(Constants.TAGS, tags);
			replyOb.put(Constants.DESCRIPTION, description);
			list.add(replyOb);
		}
		return list;
	}

	public static String getChildString(List<Node> nodes){
		StringBuilder builder = new StringBuilder();
		for(Node n: nodes){
			if("#text".equalsIgnoreCase(n.nodeName())){
				String reply = n.toString().trim();
				reply = reply.replaceAll("\\{[^}]*\\}", "").trim();
				if(reply != null && !"".equalsIgnoreCase(reply)){
					builder.append(reply);
				}
			}
		}
		return builder.toString().trim();
	}
}
