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

public class BingKnowsParser {
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		String html = FileUtil.readFileToString("/Users/greshem/codes/myprojects/corpusetl/test.html");
		List<JSONObject> obs = parser(html, 111, "http://www.bing.com/knows/search?FORM=BKACAI&mkt=zh-cn&q=公蚊子吸血吗");
		for(JSONObject ob: obs){
			if(ob.has("related_question")){
				System.out.println("related_question" + ": \n" + ob.get("related_question") + "\n");
			}else{
				System.out.println(Constants.TITLE + ": \n" + ob.get(Constants.TITLE) + "\n");
				System.out.println(Constants.QUESTION + ": \n" + ob.get(Constants.QUESTION) + "\n");
				System.out.println(Constants.ANSWERS + ": \n" + ob.get(Constants.ANSWERS).toString() + "\n");
				System.out.println(Constants.SOURCE + ": \n" + ob.get(Constants.SOURCE).toString() + "\n");
			}
			System.out.println("==================================================");
		}
		System.out.println(obs.size());
	}
	
	/*
	 * url pattern
	 * http://cn.bing.com/knows/search?q=%E4%BA%BA%E6%9C%89%E6%82%B2%E6%AC%A2%E7%A6%BB%E5%90%88%E7%9A%84%E4%BD%9C%E8%80%85&mkt=zh-cn
	 * http://www.bing.com/knows/search?q=%E5%AE%8B%E4%BB%B2%E5%9F%BA&FORM=BKHPHOT&mkt=zh-cn
	 */
	public static List<JSONObject> parser(String html_body, int source, String url) {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		HashSet<String> questionSet = new HashSet<String>();
		Document html = Jsoup.parse(html_body);
		if(html == null){
			return resultList;
		}
		
		// 抓取原始的搜索问题
		String tmpURL = url;
		try {
			tmpURL = URLDecoder.decode(url, "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} 
		String rawquery = getRawQuery(tmpURL);
		if(!"".equalsIgnoreCase(rawquery)){
			// 抓取返回的信息list
			Elements listElements = html.select(".bk_section");
			if(listElements != null && listElements.size() > 0){
				for(int n=0; n<listElements.size(); n++){
					Element item = listElements.get(n);
					Elements qaList = item.select(".bk_qna_answer.bk_qna_type_answer");
					for(int j=0; j<qaList.size(); j++){
						Element qaPair = qaList.get(j);
						Element qElement = qaPair.select(".bk_qnaanswer_question > h3").first();
						String question = getElementText(qElement);
						
						Element aElement = qaPair.select(".bk_qnaanswer_answercontent").first();
						String answer = getElementText(aElement);
						if(!"".equalsIgnoreCase(question) && question != null 
								&& !"".equalsIgnoreCase(answer) && answer != null){
							JSONObject object = new JSONObject();
							object.put(Constants.QUESTION, question);
							object.put(Constants.ANSWERS, answer);
							resultList.add(object);
						}
					}
					
					Element qnaQuestion = item.select(".bk_qna_question").first(); 
					if(qnaQuestion != null){
						Element h2Question = qnaQuestion.getElementsByTag("h2").first();
						String h2QuestionString = getElementText(h2Question);
						Element qnaAnswerQuestion = item.select(".bk_qnaanswer_question").first(); 
						// 没有“问”标签的才会与上一层的Q组成QA对
						if(qnaAnswerQuestion == null){
							Element qnaAnswer = item.select(".bk_qnaanswer_answercontent").first(); 
							String qnaAnswerString = getElementText(qnaAnswer);
							if(!"".equalsIgnoreCase(qnaAnswerString)){
								JSONObject object = new JSONObject();
								object.put(Constants.QUESTION, h2QuestionString);
								object.put(Constants.ANSWERS, qnaAnswerString);
								resultList.add(object);
							}
						}
					}
					
					Elements answerElements = item.select(".bk_wr_answer");
					if(listElements != null && listElements.size() > 0){
						for(int i=0; i<answerElements.size(); i++){
							Element answerItem = answerElements.get(i);
							
							// 第一种格式
							Elements rcPairs = answerItem.select(".rc_qna_pair");
							for(int m=0; m<rcPairs.size(); m++){
								Element rcPair = rcPairs.get(m);
								Elements qnaPc = rcPair.select(".rc_qna_pc");
								if(qnaPc == null || qnaPc.size() != 2){
									continue;
								}
								JSONObject object = new JSONObject();
								Element qElement = qnaPc.get(0);
								String question = getSubString(qElement.text());
								
								Element aElement = qnaPc.get(1);
								Element expandsElement = aElement.select(".rc_qna_expand").first();
								String answer = "";
								if(expandsElement != null){
									answer = getElementText(expandsElement);
								}else{
									answer = getElementText(aElement);
								}
								object.put(Constants.QUESTION, question);
								object.put(Constants.ANSWERS, answer);
								resultList.add(object);
							}
							
							// 第二种格式
							if(answerItem.children().size() > 0){
								Element h2 = answerItem.child(0);
								if(h2 != null && "h2".equalsIgnoreCase(h2.tagName())){
									String question = getSubString(h2.text().replace(" - 相关话题", "").replace(" - 必应", ""));
									if("正在加载".equalsIgnoreCase(question)){
										continue;
									}
									String answer = "";
									Elements bestAnswers = answerItem.select(".hsPanel > span");
									if(bestAnswers != null  && bestAnswers.size() >= 2){
										answer = getElementText(bestAnswers.get(1));
									}
									if(!"".equalsIgnoreCase(answer)){
										JSONObject object = new JSONObject();
										object.put(Constants.QUESTION, question);
										object.put(Constants.ANSWERS, answer);
										resultList.add(object);
		
										Elements questionList = answerItem.select(".b_vList > li > a");
										for(int l=0; l<questionList.size(); l++){
											String expandedQ = getSubString(questionList.get(l).text());
											if(expandedQ.length() <= 30){
												questionSet.add(expandedQ);
											}
										}
									}else{
										Elements msnTable = answerItem.select(".msn_table > tbody > tr");
										for(int l=0; l<msnTable.size(); l++){
											Element tr = msnTable.get(l);
											Elements elementList = tr.getElementsByTag("li");
											if(elementList.size() == 0){
												elementList = tr.getElementsByTag("td");
											}
											question = getElementText(elementList.get(0));
											if(elementList.size() == 2){
												answer = getElementText(elementList.get(1));
												if(!"".equalsIgnoreCase(answer)){
													JSONObject object = new JSONObject();
													object.put(Constants.QUESTION, question);
													object.put(Constants.ANSWERS, answer);
													resultList.add(object);
												}
											}else if(elementList.size() == 1){
												if(question.length() <= 30 && !"".equalsIgnoreCase(question)){
													questionSet.add(question);
												}
											}
										}
									}
								}
							}
							
							// 第三种格式
							Elements msnElements = answerItem.select(".msnv2_component_padding.msnv2_ctn_overflow_hidden"); 
							for(int m=0; m<msnElements.size(); m++){
								Element msnQElement = msnElements.get(m);
								Element msnQ = msnQElement.select(".b_vList > li > a > strong").first();
								if(msnQ == null){
									m++;
									continue;
								}
								String msnQString  = getSubString(msnQ.text());
								if(m+1 <= msnElements.size() ){
									Element msnAElement = msnElements.get(m+1);
									Elements msnAList = msnAElement.select(".b_vList > li");
									if(msnAList == null){
										m++;
										continue;
									}
									String msnAString = "";
									for(int p=0; p<msnAList.size(); p++){
										Element msn = msnAList.get(p);
										if("答".equalsIgnoreCase(msn.text())){
											continue;
										}
										Elements aElements = msn.getElementsByTag("a");
										for(Element aElement : aElements){
											aElement.remove();
										}
										msnAString  = getSubString(msn.text());
										break;
									}
									JSONObject object = new JSONObject();
									object.put(Constants.QUESTION, msnQString);
									object.put(Constants.ANSWERS, msnAString);
									resultList.add(object);
								}
								m++;
							}
							
							Elements hiddenElements = answerItem.select(".msnv2_component_padding.msnv2_expan_clear_margin.msnv2_ctn_overflow_hidden");
							for(int m=0; m<hiddenElements.size(); m++){
								Element hiddenElement = hiddenElements.get(m);
								Element hiddenQ = hiddenElement.select(".b_vList > li > strong").first();
								if(hiddenQ == null){
									continue;
								}
								String hiddenQString = getSubString(hiddenQ.text());
								if(hiddenQString.length() <= 30 && !"".equalsIgnoreCase(hiddenQString)){
									questionSet.add(hiddenQString);
								}
							}
						}
					}
				}
			}
		}
		
		List<JSONObject> returnList = new ArrayList<JSONObject>();
		for(int i=0; i<resultList.size(); i++){
			JSONObject object = resultList.get(i);
			object.put(Constants.TITLE, rawquery);
			String answerString = object.getString(Constants.ANSWERS);
			JSONObject answerOb = new JSONObject();
			answerOb.put(Constants.CONTENT, answerString);
			answerOb.put(Constants.SELECT, 0);
			JSONArray answerArray = new JSONArray();
			answerArray.add(answerOb);
			object.put(Constants.ANSWERS, answerArray);
			object.put(Constants.SOURCE, source);
			object.put(Constants.URL, tmpURL);
			object.put(Constants.INCREFLAG, 1);
			returnList.add(object);
		}
		
		// 如果html中没有问题／答案这种pair队， 可能此搜索词不适合做QA， 其相关搜索词也不需要再list
		if(resultList.size() != 0){
			// 取得相关搜索的问题
			Elements similarQ = html.select(".bk_qs > ul > li");
			for(int i=0; i<similarQ.size(); i++){
				String question = similarQ.get(i).text().trim();
				if(question.length() <= 30 && !"".equalsIgnoreCase(question)){
					questionSet.add(question);
				}
			}
			JSONObject similarOb = new JSONObject();
			JSONArray similarQArray = new JSONArray();
			for(String q: questionSet){
				similarQArray.add(q);
			}
			if(similarQArray.size() > 0){
				similarOb.put("related_question", similarQArray);
				returnList.add(similarOb);
			}
		}
		return returnList;
	}
	
	public static String getSubString(String rawString){
		String stopWord = "...";
		String tempString = rawString.replace("…详情", "").replace("求采纳", "")
				.replace("查看原帖", "").replace("采纳哦", "").replace("已发请查收", "").replace("编辑本段", "").trim();
		int index = tempString.indexOf(stopWord);
		if(index == -1){
			return tempString;
		}else{
			return tempString.substring(0, tempString.indexOf(stopWord));
		}
	}
	
	public static String getElementText(Element element){
		if(element != null){
			return getSubString(element.text()).replaceAll("\\{\\-+", "\\{-");
		}else{
			return "";
		}
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
					if("q".equalsIgnoreCase(keyValue[0])){
						rawQuery = keyValue[1];
						break;
					}
				}
			}
		}
		return rawQuery;
	}
}
