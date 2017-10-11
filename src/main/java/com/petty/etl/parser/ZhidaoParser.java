package com.petty.etl.parser;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class ZhidaoParser {
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		HashMap<String, String> picMap = FileUtil.readFileToMap("/Users/greshem/codes/myprojects/corpusetl/Zhidao_Pic.map", "\t");
		String html = FileUtil.readFileToString("/Users/greshem/codes/myprojects/corpusetl/test.html");
		List<JSONObject> obs = parser(html, picMap);
		for(JSONObject ob: obs){
			System.out.println(Constants.TITLE + ": \n" + ob.get(Constants.TITLE) + "\n");
			System.out.println(Constants.QUESTION + ": \n" + ob.get(Constants.QUESTION).toString() + "\n");
			System.out.println(Constants.ANSWERS + ":");
			JSONArray array = (JSONArray) ob.get(Constants.ANSWERS);
			for(int i=0;i<array.size(); i++){
				String answer = array.get(i).toString();
				System.out.println("\t" + answer);
			}
			System.out.println(Constants.TAGS + ": \n" + ob.get(Constants.TAGS).toString() + "\n");
			System.out.println(Constants.DESCRIPTION + ": \n" + ob.get(Constants.DESCRIPTION) );
			System.out.println("==================================================");
		}
	}
	
	/*
	 * url pattern
	 * http://zhidao.baidu.com/question/1641235985866127540.html
	 */
	public static List<JSONObject> parser(String html_body, HashMap<String, String> picMap) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		JSONObject result = new JSONObject();
		Document html = Jsoup.parse(html_body);
		if(html == null){
			return null;
		}
		
		HashMap<String, String> currentPicMap = new HashMap<String, String>();
		// 先处理html的中图片文件
		Elements imgs = html.select("img");
		for(int i=0; i< imgs.size(); i++){
			Element img = imgs.get(i);
			String url = img.attr("src");
			if(url.contains("api/getdecpic")){
				String urlValue = picMap.get(url);
				if(urlValue == null || "".equalsIgnoreCase(urlValue)){
					System.out.println("没有配置的: " + url);
				}else{
					currentPicMap.put(url, picMap.get(url));
				}
			}
		}
		Set<String> keySet = currentPicMap.keySet();
		for(String key: keySet){
			String htmlImg = "<img class=\"word-replace\" src=\""+ key + "\">";
			html_body = html_body.replace(htmlImg, currentPicMap.get(key));
		}
		html = Jsoup.parse(html_body);
		if(html == null){
			return null;
		}
		
		Element tilte = html.select(".ask-title").first();
		if(tilte != null){
//			System.out.println(tilte.text());
			result.put(Constants.TITLE, tilte.text());
			result.put(Constants.QUESTION, tilte.text());
		}
		
		Elements keywords = html.select("li.word.grid");
		JSONArray tags = new JSONArray();
		if(keywords != null && keywords.size() > 0){
			for(Element keyword: keywords){
				if(!"搜索资料".equalsIgnoreCase(keyword.text())){
					tags.add(keyword.text());
				}
			}
		}
		result.put(Constants.TAGS, tags);
		
		Element describe = html.select(".line.mt-5.q-content").first();
		String descriptionText = "";
		if(describe != null){
			descriptionText = describe.text().replace("图\" class=\"ikqb_img_alink\">", "").trim();
		}
		Element describeSupply = html.select(".line.mt-10.q-supply-content").first();
		if(describeSupply != null){
			descriptionText = descriptionText + "。" + describeSupply.text().replace("图\" class=\"ikqb_img_alink\">", "").trim();
		}
		result.put(Constants.DESCRIPTION, descriptionText);
		

		JSONArray replayArray = new JSONArray();
		// 提问者采纳（best－text）
		Element bestReply = html.select(".best-text.mb-10").first();
		if(bestReply != null){
			replayArray.add(bestReply.text());
		}
		// 网友采纳（recommend－text）
		Element recommendReply = html.select(".recommend-text.mb-10").first();
		if(recommendReply != null){
			replayArray.add(recommendReply.text());
		}
		// 专业回答
		Element profReply = html.select(".quality-content-detail.content").first();
		if(profReply != null){
			replayArray.add(profReply.text());
		}
		// 企业回答
		Element ecReply = html.select(".ec-answer").first();
		if(ecReply != null){
			replayArray.add(ecReply.text());
		}
				
		Elements replies = html.select("span.con");
		if(replies != null && replies.size() > 0){
			for(Element reply: replies){
				replayArray.add(reply.text());
			}
		}
		result.put(Constants.ANSWERS, replayArray);
		resultList.add(result);
		
		Elements replyContents = html.select("div.line.content");
		if(replyContents != null && replyContents.size() > 0){
			for(Element reply: replyContents){
				resultList.addAll(getReplyToReply(reply, tilte.text(), tags, descriptionText));
			}
		}
		return resultList;
	}
	
	public static List<JSONObject> getReplyToReply(Element element, String title, JSONArray tags, String descrition){
		List<JSONObject> list = new ArrayList<JSONObject>();
		List<Node> childNodes = element.childNodes();
		if(childNodes == null || childNodes.size() == 0){
			return null;
		}else{
			for(int i=0; i<childNodes.size(); i++){
				Node node = childNodes.get(i);
				String nodeClass = node.attr("class");
				if(nodeClass == null || "".equalsIgnoreCase(nodeClass)){
					continue;
				}
				JSONObject object = null;
				if("answer-text line".equalsIgnoreCase(nodeClass)){
					Element reply = element.getElementsByClass("con").first();
					String firstReplyType = getFirstReplyType(childNodes, i);
//					System.out.println("firstReplyType: "+ firstReplyType);
					if("zhuiwen".equalsIgnoreCase(firstReplyType)){ // 如果回复是追问，则处理QA pair
						object = parseReply(childNodes, i, reply.text(), "zhuida", title, tags, descrition);
					}else if("zhuida".equalsIgnoreCase(firstReplyType)){ // 如果是追答，应该是对原帖子的回答
						object = parseReply(childNodes, i, title, "zhuiwen", title, tags, descrition);
						if(object != null){
							list.add(object);
						}
						object = parseReply(childNodes, i, reply.text(), "zhuida", title, tags, descrition);
					}
				}else if("replyask line replyask-ask".equalsIgnoreCase(nodeClass)){
					String question = getText(node, "qRA", "div");
					object = parseReply(childNodes, i, question, "zhuiwen", title, tags, descrition);
				}else if("replyask line replyask-ans".equalsIgnoreCase(nodeClass)){
					String question = getText(node, "aRA", "div");
					object = parseReply(childNodes, i, question, "zhuida", title, tags, descrition);
					// 提问者评价应该只会在追答后面
					String firstReplyType = getFirstReplyType(childNodes, i);
//					System.out.println("firstReplyType: "+ firstReplyType);
					if("qthanks".equalsIgnoreCase(firstReplyType)){
						object = parseReply(childNodes, i, question, "qthanks", title, tags, descrition);
					}
				}else if("best-text mb-10".equalsIgnoreCase(nodeClass)){
					String question = getChildStr(node);
//					System.out.println("question: " + question);
					String firstReplyType = getFirstReplyType(childNodes, i);
//					System.out.println("firstReplyType: "+ firstReplyType);
					if("zhuiwen".equalsIgnoreCase(firstReplyType)){ // 如果回复是追问，则处理QA pair
						object = parseReply(childNodes, i, question, "zhuida", title, tags, descrition);
					}else if("zhuida".equalsIgnoreCase(firstReplyType)){ // 如果是追答，应该是对原帖子的回答
						object = parseReply(childNodes, i, title, "zhuiwen", title, tags, descrition);
						if(object != null){
							list.add(object);
						}
						object = parseReply(childNodes, i, question, "zhuida", title, tags, descrition);
					}else{
						object = parseReply(childNodes, i, question, "qthanks", title, tags, descrition);
					}
				}else if("recommend-text mb-10".equalsIgnoreCase(nodeClass)){
					String question = getChildStr(node);
//					System.out.println("question: " + question);
					object = parseReply(childNodes, i, question, "zhuida", title, tags, descrition);
				}
				if(object != null){
					list.add(object);
				}
			}
		}
		return list;
	}
	
	public static String getText(Node subNode, String value, String tagType){
		List<Node> subChildNodes = subNode.childNodes();
		String text = "";
		for(int m=0; m<subChildNodes.size(); m++){
			Node subChildNode = subChildNodes.get(m);
			if(tagType.equalsIgnoreCase(subChildNode.nodeName())){
				if("dd".equalsIgnoreCase(tagType)){
					Node replyTextNode = subChildNode.childNode(1);
					if("pre".equalsIgnoreCase(replyTextNode.nodeName()) 
							&& replyTextNode.hasAttr("accuse") 
							&& value.equalsIgnoreCase(replyTextNode.attr("accuse"))){
						if(replyTextNode.childNodeSize() == 1){
							//System.out.println(replyTextNode.childNode(0).toString());
							text = replyTextNode.childNode(0).toString();
						}
					}
				}else if(subChildNode.hasAttr("accuse") && value.equalsIgnoreCase(subChildNode.attr("accuse"))){
					Node replyTextNode = subChildNode.childNode(1);
					if("pre".equalsIgnoreCase(replyTextNode.nodeName()) 
							&& replyTextNode.hasAttr("accuse") 
							&& value.equalsIgnoreCase(replyTextNode.attr("accuse"))){
						if(replyTextNode.childNodeSize() == 1){
							//System.out.println(replyTextNode.childNode(0).toString());
							text = replyTextNode.childNode(0).toString();
						}
					}
				}
			}
		}
		return text;
	}
	
	public static JSONObject parseReply(List<Node> childNodes, int i, String question, String type, String title, JSONArray tags, String descrition){
		JSONArray replyArray = new JSONArray();
		boolean flag = false;
		String questionClass = "";
		String answerClass = "";
		String answerValue = "";
		String tagType = "";
		
		if("zhuiwen".equalsIgnoreCase(type)){
			questionClass = "replyask line replyask-ask";
			answerClass = "replyask line replyask-ans";
			answerValue = "aRA";
			tagType = "div";
		}else if("zhuida".equalsIgnoreCase(type)){
			questionClass = "replyask line replyask-ans";
			answerClass = "replyask line replyask-ask";
			answerValue = "qRA";
			tagType = "div";
		}else if("qthanks".equalsIgnoreCase(type)){
			questionClass = "best-text mb-10";
			answerClass = "thank line pt-5 pb-5";
			answerValue = "qThanks";
			tagType = "dd";
		}
		
		for(int j=i+1; j < childNodes.size(); j++){
			Node subNode = childNodes.get(j);
			String subNodeClass = subNode.attr("class");
			if(subNodeClass == null || "".equalsIgnoreCase(subNodeClass)){
				continue;
			}
			if(questionClass.equalsIgnoreCase(subNodeClass)){
				if(flag){ // 当针对追问找答案时，如果已经发现了追答，在查找时又发现了追问，则跳出循环；如果还没有发现追答，查找时发现了追问，则跳过追问，继续找追答。
					break;
				}else{
					continue;
				}
			}
			if(answerClass.equalsIgnoreCase(subNodeClass)){
				String textAnswer = getText(subNode, answerValue, tagType);
				if(textAnswer != null && !"".equalsIgnoreCase(textAnswer.trim())){
					replyArray.add(textAnswer);
					flag = true;
				}
			}
		}
		if(flag && replyArray.size() > 0 && question != null && !"".equalsIgnoreCase(question.trim())){
			JSONObject object = new JSONObject();
			object.element(Constants.TITLE, title);
			object.element(Constants.QUESTION, question);
			object.element(Constants.ANSWERS, replyArray);
			object.element(Constants.TAGS, tags);
			object.element(Constants.DESCRIPTION, descrition);
			return object;
		}else{
			return null;
		}
	}
	
	/**
	 * 用来检查本条回复后面是追问还是追答，如果是追问，则本条为追答；如果是追答，则为追问
	 * @param childNodes
	 * @param i
	 * @param tagType: div
	 * @return
	 */
	public static String getFirstReplyType(List<Node> childNodes, int i){
		String replyType = "";
		boolean foundFlag = false;
		for(int j=i+1; j < childNodes.size(); j++){
			Node subNode = childNodes.get(j);
			String subNodeClass = subNode.attr("class");
			if(subNodeClass == null || "".equalsIgnoreCase(subNodeClass)){
				continue;
			}
			List<Node> subChildNodes = subNode.childNodes();
			for(int m=0; m<subChildNodes.size(); m++){
				Node subChildNode = subChildNodes.get(m);
				if("div".equalsIgnoreCase(subChildNode.nodeName()) 
						&& subChildNode.hasAttr("accuse")){
					String classType = subChildNode.attr("accuse");
					if("aRA".equalsIgnoreCase(classType)){
						replyType = "zhuida";
					}else{
						replyType = "zhuiwen";
					}
					foundFlag = true;
					break;
				}else if("dd".equalsIgnoreCase(subChildNode.nodeName())
						&& "grid ml-10".equalsIgnoreCase(subChildNode.attr("class"))){
					replyType = "qthanks";
					foundFlag = true;
					break;
				}
			}
			if(foundFlag){
				break;
			}
		}
		return replyType;
	}
	
	public static String getChildStr(Node node){
		List<Node> nodeList = node.childNodes();
		StringBuilder builder = new StringBuilder();
		for(Node n: nodeList){
			String type = n.nodeName();
			if("a".equalsIgnoreCase(type)){
				if(n.childNodeSize() > 0){
					builder.append(n.childNode(0).toString());
					continue;
				}
			}
			String tempStr = n.toString().trim();
			if(!"<br>".equalsIgnoreCase(tempStr)){
				builder.append(n.toString());
			}
		}
		return builder.toString();
	}

}
