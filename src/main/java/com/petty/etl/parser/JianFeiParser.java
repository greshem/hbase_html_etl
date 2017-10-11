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

public class JianFeiParser {
	
	public static List<JSONObject> parseQA(String html) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		JSONObject qa = new JSONObject();
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		String title = "";
		String question = "";
		String description = "";
		String answer = "";
		JSONArray answerlist = new JSONArray();
		
		Element eTitle = htmlDoc.select("h1").first();
		if (eTitle != null) {
			title = eTitle.text();
		}
		
		Elements eQA = htmlDoc.select("div.b_right");		
		//question
		Element eQ = eQA.get(0);		
		Element eQuestion = eQ.select("p").first();
		if (eQuestion != null) {
			question = eQuestion.text().replace("问题描述:", "");
		}
		
		if (title.equals("")) {
			return null;
		}
		
		if (question.equals("")) {
			question = title;
		}
		
		description = question;
		
		Elements eAs = htmlDoc.select("p.user_answer");
		
		for (Element eA:eAs) {
			//answer
			eA = htmlDoc.select("p.user_answer").first();
			if (eA != null && !eA.text().equals("")) {
				answer = eA.text();
				answerlist.add(answer);
			}
		}
			
		Constants.parserResultBuilder(qa, title, question, description, answerlist);
		result.add(qa);		
				
		return result;
	}
	
	public static List<JSONObject> parseStar(String html) {
		List<JSONObject> result = null;
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		Element wrapc = htmlDoc.select(".wrap_c").first();
		Element focusText = htmlDoc.select(".focusText").first();
		Element bigBox = htmlDoc.select("div#big_box").first();
		Element wrapper = htmlDoc.select(".wrapper").first();

		if (focusText != null) {
			//include <div class="focusText">
			//http://fitness.39.net/special/jfzrx/35/index.html
			result = parseStar1(htmlDoc);
		} else if (bigBox != null) {
			//include <div id="big_box">
			//http://fitness.39.net/special/jfzrx/94/index.html
			result = parseStar2(htmlDoc);
		} else if (wrapc != null) {
			//include <div class="wrap_c">
			//http://fitness.39.net/special/jfzrx/13/index.html
			result = parseStar3(htmlDoc);
		} else if (wrapper != null) { 
			//include <div class="wrapper">
			//http://fitness.39.net/special/jfzrx/68/index.html
			result = parseStar4(htmlDoc);
		} else {
			result = new ArrayList<JSONObject>();
		}
		
		return result;
	}
	
	public static List<JSONObject> parseStar1(org.jsoup.nodes.Document htmlDoc) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String title = "";
		String question = "";
		String description = "";
		String answer = "";
		JSONArray answerlist = new JSONArray();
		
		Element focusText = htmlDoc.select("div.focusText").first();
		
		if (focusText != null) {
			Element eTitle = focusText.select("strong").first();
			if (eTitle != null) {
				title = eTitle.text();
			}
			
			Element eDes = focusText.select("p.aboutInfo").first();
			if (eDes != null) {
				description = eDes.text();
			}
		}
		
		Element ePart1 = htmlDoc.select(".part1_1").first();
		if (ePart1 != null) {
			description = description + "\n" + ePart1.text();
		}
		
		Element eSaybox = htmlDoc.select(".sayBox").first();
		if (eSaybox != null) {
			description = description + "\n" + eSaybox.text();
		}
		
		Element eFansDl = htmlDoc.select("dl.fansDl").first();
		if (eFansDl != null) {
			Elements eQuestions = eFansDl.select("dt");
			Elements eAnswers = eFansDl.select("dd");
			
			if (eQuestions != null && eAnswers !=null && eQuestions.size() == eAnswers.size()) {
				for (int i = 0; i < eQuestions.size(); i++) {
					question = eQuestions.get(i).text();
					answer = eAnswers.get(i).text();
					
					if (!question.equals("") && !answer.equals("")) {
						JSONObject qa = new JSONObject();
						answerlist.add(answer);
						Constants.parserResultBuilder(qa, title, question, description, answerlist);
						result.add(qa);
					}
				}
			}
		}
				
		return result;
	}
	
	public static List<JSONObject> parseStar2(org.jsoup.nodes.Document htmlDoc) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String title = "";
		String question = "";
		String description = "";
		String answer = "";
		JSONArray answerlist = new JSONArray();
		
		Element eMainLeft = htmlDoc.select(".main_left").first();
		if (eMainLeft == null) {
			return result;
		}
		
		Element eTitle = eMainLeft.select("h1").first();
		if (eTitle != null) {
			title = eTitle.text();
		}
		
		Element eDes = eMainLeft.select("p").first();
		if (eDes != null) {
			description = eDes.text();
		}
		
		eDes = eMainLeft.select(".story").first();
		if (eDes != null) {
			description = description + "\n" + eDes.text();
		}
		
		Elements eDeses = eMainLeft.select(".fit_content");
		for (Element elem:eDeses) {
			description = description + "\n" + elem.text();
		}
		
		Element eQA = eMainLeft.select(".questions").first();
		if (eQA == null) {
			return result;
		}
		
		Elements eQAs = eQA.select("li");
		for (Element elem:eQAs) {
			Element eq = elem.select(".wen").first();
			Element ea = elem.select(".da").first();
			
			if (eq == null || ea == null) {
				continue;
			}
			
			question = eq.text();
			answer = ea.text();
			
			if (!question.equals("") && !answer.equals("")) {
				JSONObject qa = new JSONObject();
				answerlist.add(answer);
				Constants.parserResultBuilder(qa, title, question, description, answerlist);
				result.add(qa);
			}
		}
		
		return result;
	}
	
	public static List<JSONObject> parseStar3(org.jsoup.nodes.Document htmlDoc) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String title = "";
		String question = "";
		String description = "";
		String answer = "";
		JSONArray answerlist = new JSONArray();
		
		Elements eWraps = htmlDoc.select(".wrap_c");
		Element eWrap0 = eWraps.first();
		Element eInbox1 = eWrap0.select(".in_box1_c").first();
		if (eInbox1 == null) {
			return result;
		}
		
		Element eTitle = eInbox1.select("h1").first();
		if (eTitle == null) {
			return result;
		}
		
		title = eTitle.text();
		
		Element eDes = eInbox1.select("p").first();
		if (eDes != null) {
			description = eDes.text();
		}
		
		Element eInbox5 = htmlDoc.select(".in_box5_c").first();
		if (eInbox5 == null) {
			return result;
		}
	
		Element eLeft = eInbox5.select(".left").first();
		
		if (eLeft != null) {
			description = description + "\n" + eLeft.text();
		}
		
		Element eRight = eInbox5.select(".right").first();
		if (eRight != null) {
			description = description + "\n" + eRight.text();
		}		
		
		Element eQA = eInbox5.select(".left3").first();
		
		if (eQA == null) {
			return result;
		}
		
		Elements eQAs = eQA.select("p");
		for (Element elem:eQAs) {
			Element eq = elem.select("strong").first();
			Element ea = elem.select("span").first();
			if (eq == null || ea == null) {
				continue;
			}
			
			question = eq.text();
			answer = ea.text();
			if (!question.equals("") && !answer.equals("")) {
				answer = answer.replace("答：", "");
				JSONObject qa = new JSONObject();
				answerlist.add(answer);
				Constants.parserResultBuilder(qa, title, question, description, answerlist);
				result.add(qa);
			}
		}
		
		return result;
	}
	
	public static List<JSONObject> parseStar4(org.jsoup.nodes.Document htmlDoc) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String title = "";
		String question = "";
		String description = "";
		String answer = "";
		JSONArray answerlist = new JSONArray();
		
		Element eTitle = htmlDoc.select("h2").first();
		if (eTitle == null) {
			return result;
		}
		title = eTitle.text();
		
		Elements eDeses = htmlDoc.select(".cBox");
		for (int i = 0; i < 3; i++) {
			Element eDes = eDeses.get(i);
			if (i == 0) {
				description = eDes.text();
			} else {
				description = description + "\n" + eDes.text();
			}
		}
		
		Element eQA = htmlDoc.select(".askList").first();
		if (eQA == null) {
			return result;
		}
		
		Elements eqs = eQA.select(".ask-tit");
		Elements eas = eQA.select(".ans-con");
		if (eqs == null || eas == null) {
			return result;
		}
		
		if (eqs.size() != eas.size()) {
			return result;
		}
		
		for (int i = 0; i < eqs.size(); i++) {
			Element eq = eqs.get(i);
			Element ea = eas.get(i);
			
			question = eq.text();
			answer = ea.text();
			
			if (!question.equals("") && !answer.equals("")) {
				JSONObject qa = new JSONObject();
				answerlist.add(answer);
				Constants.parserResultBuilder(qa, title, question, description, answerlist);
				result.add(qa);
			}
		}		
		
		return result;
	}
	
	public static List<JSONObject> parseBBS(String html) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String title = "";
		String question = "";
		String description = "";
		String answer = "";
		JSONArray answerlist = new JSONArray();
		
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		Element eTitle = htmlDoc.select("h1").first();
		
		if (eTitle == null || eTitle.text().equals("")) {
			return result;			
		}
		
		title = eTitle.text();
		question = title;
		
		Elements eReplies = htmlDoc.select(".pcb");
		Element eDes = eReplies.get(0);
		
		if (eDes != null) {
			description = eDes.text();
		}
		
		for (int i = 1; i < eReplies.size(); i++) {
			answer = eReplies.get(i).text();
			answerlist.add(answer);
		}
		
		JSONObject qa = new JSONObject();
		Constants.parserResultBuilder(qa, title, question, description, answerlist);
		result.add(qa);		
		
		result.addAll(parseBBSReplyToReply(htmlDoc, title, description));
		
		return result;
	}
	
	public static List<JSONObject> parseBBSReplyToReply(org.jsoup.nodes.Document htmlDoc
												, String title, String description) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		Elements eReplies = htmlDoc.select(".pcb");
		
		if (eReplies == null) {
			return result;
		}
		
		for (Element eReply:eReplies) {
			Elements eRRs =  eReply.select(".lous");
			if (eRRs == null || eRRs.size() == 0) {
				continue;
			}
			
			Elements ePs = eReply.select("p");
			List<String> RRs = new ArrayList<String>();
			
			for (Element eP:ePs) {
				RRs.add(eP.text());
			}
			
			Element eTd = eReply.select(".t_f").first();
			if (eTd != null) {
				RRs.add(eTd.ownText());
			}
			
			for (int j = 0; j < RRs.size() - 1; j++) {
				String question = RRs.get(j);
				String answer = RRs.get(j + 1);
				
				JSONObject qa = new JSONObject();
				JSONArray answerlist = new JSONArray();
				answerlist.add(answer);
				Constants.parserResultBuilder(qa, title, question, description, answerlist);
				result.add(qa);		
			}
		} 
		
		return result;
	}
}
