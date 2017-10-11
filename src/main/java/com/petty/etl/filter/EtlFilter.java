package com.petty.etl.filter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.petty.etl.commonUtils.HttpUtils;
import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class EtlFilter extends BaseFilter {

	@Override
	public JSONObject filter(String data, Pattern pd, String url, HashSet<String> symbolSet, boolean replaceFlag) {

		List<String> validList = new ArrayList<String>();
		List<String> invalidList = new ArrayList<String>();
		JSONObject srcData = JSONObject.fromObject(data);
		JSONObject validResData = srcData;
		JSONObject invalidData = srcData;
		JSONObject finalData = new JSONObject();
		
		String question = srcData.getString(Constants.QUESTION);
		String qRemoveSymbol = question;
		if(replaceFlag){ // 如果是true， 表示需要先replace掉标点符号再匹配； 否则表示需要匹配 url，邮箱地址等，不需要先replace标点
			qRemoveSymbol = SymbolUtil.removeSymbol(question, symbolSet);
		}
		boolean questionFlag = false;
		if(url != null && !"".equalsIgnoreCase(url)){
			questionFlag = checkNERStr(url, qRemoveSymbol);
		}else{
			questionFlag = isFilterNeeded(pd, qRemoveSymbol);
		}
		if(questionFlag){
			finalData.put("invalid", invalidData);
			// valid的数据， 就是answer变成空list
			validResData.put(Constants.ANSWERS, validList.toArray());
			finalData.put("valid", validResData);
		}else{
			JSONArray answers = srcData.getJSONArray(Constants.ANSWERS);
			for (int i = 0; i < answers.size(); i++) {
				JSONObject answerOb = answers.getJSONObject(i);
				String answer = answerOb.getString(Constants.CONTENT);
				String aRemoveSymbol = answer;
				if(replaceFlag){ // 如果是true， 表示需要先replace掉标点符号再匹配； 否则表示需要匹配 url，邮箱地址等，不需要先replace标点
					aRemoveSymbol = SymbolUtil.removeSymbol(answer, symbolSet);
				}
				if (answer.length() < 80 && answer.length() > 0) {
					boolean answerFlag = false;
					if(url != null && !"".equalsIgnoreCase(url)){
						answerFlag = checkNERStr(url ,aRemoveSymbol);
					}else{
						answerFlag = isFilterNeeded(pd, aRemoveSymbol);
					}
					if(answerFlag){ 
						invalidList.add(answerOb.toString());
					} else {
						validList.add(answerOb.toString());
					}
				}
			}
			validResData.put(Constants.ANSWERS, validList.toArray());
			finalData.put("valid", validResData);
			invalidData.put(Constants.ANSWERS, invalidList.toArray());
			finalData.put("invalid", invalidData);
		}
		return finalData;
	}

	public JSONObject filterNewRule(String data, Pattern pd, String url, HashSet<String> symbolSet, boolean replaceFlag) {

		List<String> validResultList = new ArrayList<String>();
		List<String> invalidResultList = new ArrayList<String>();
		JSONObject srcData = JSONObject.fromObject(data);
		JSONObject validResData = srcData;
		if(validResData.has(Constants.INCREFLAG)){
			validResData.remove(Constants.INCREFLAG);
		}
		JSONObject invalidResData = srcData;
		if(invalidResData.has(Constants.INCREFLAG)){
			invalidResData.remove(Constants.INCREFLAG);
		}
		JSONObject finalData = new JSONObject();
		String question = srcData.getString(Constants.QUESTION);
		String qRemoveSymbol = question;
		if(replaceFlag){ // 如果是true， 表示需要先replace掉标点符号再匹配； 否则表示需要匹配 url，邮箱地址等，不需要先replace标点
			qRemoveSymbol = SymbolUtil.removeSymbol(question, symbolSet);
		}
		boolean questionFlag = false;
		if(url != null && !"".equalsIgnoreCase(url)){
			questionFlag = checkNERStr(url, qRemoveSymbol);
		}else{
			questionFlag = isFilterNeeded(pd, qRemoveSymbol);
		}
		if(questionFlag){
			finalData.put("remove", 1);
			// 如果Q需要被过滤， 则所有的该条record直接被删掉
			finalData.put("invalid", invalidResData);
			// valid的数据， 就是answer变成空list
			validResData.put(Constants.ANSWERS, validResultList.toArray());
			finalData.put("valid", validResData);
		}else{
			JSONArray answers = srcData.getJSONArray(Constants.ANSWERS);
			for (int i = 0; i < answers.size(); i++) {
				JSONObject answerOb = answers.getJSONObject(i);
				String answer = answerOb.getString(Constants.CONTENT).trim();
				String aRemoveSymbol = answer;
				if(replaceFlag){ // 如果是true， 表示需要先replace掉标点符号再匹配； 否则表示需要匹配 url，邮箱地址等，不需要先replace标点
					aRemoveSymbol = SymbolUtil.removeSymbol(answer, symbolSet);
				}
				boolean answerFlag = false;
				if(url != null && !"".equalsIgnoreCase(url)){
					answerFlag = checkNERStr(url ,aRemoveSymbol);
				}else{
					answerFlag = isFilterNeeded(pd, aRemoveSymbol);
				}
				if(answerFlag) { 
					invalidResultList.add(answerOb.toString());
				}else{
					validResultList.add(answerOb.toString());
				}
			}
			// 如果invalidResultList的个数与answers原本的个数一样，则表示所有的answer都被filter掉了， 需要从Solr中remove
			// 如果有效数据集的answer个数小于answers原本的个数, 则表示有部分answer被filter，需要更新Solr
			if(answers.size() > 0 && (answers.size() == invalidResultList.size())){
				finalData.put("remove", 1);
			}else if(validResultList.size() > 0 && (validResultList.size() < answers.size())){
				finalData.put("update", 1);
			}
			validResData.put(Constants.ANSWERS, validResultList.toArray());
			finalData.put("valid", validResData);

			invalidResData.put(Constants.ANSWERS, invalidResultList.toArray());
			finalData.put("invalid", invalidResData);
		}
		return finalData;
	}
	
	public ArrayList<String> getTopic(String text, Pattern pattern) {

		ArrayList<String> qaList = new ArrayList<String>();
		JSONObject srcData = JSONObject.fromObject(text);
		String question = srcData.getString(Constants.QUESTION);
		if (isFilterNeeded(pattern, question)) {
			JSONArray answers = srcData.getJSONArray(Constants.ANSWERS);
			for(int i=0; i<answers.size(); i++){
				qaList.add(question + "\t" + answers.getString(i));
			}
		} 
		return qaList;
	}
	
	public String readFile(String file) {
		StringBuilder content = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String s = null;

			while ((s = br.readLine()) != null) {
				content.append(s).append("\n");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return content.toString();
	}

	public Pattern compileDeleteRegexPattern(URI[] uriArray) {
		String filterString = "";
		String regex = "";

		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();

			// uncomment this line for local testing/debugging mode
			// String filename = uriPath.toString();
			if (filename.toLowerCase().contains("deletetotal.txt")) {
				filterString = filterString + readFile(filename);
				break;
			} else {
				filterString = "placeholder";
			}
		}
		if (filterString.length() > 1) {
			filterString = filterString.substring(0, filterString.length() - 1);
		}
		regex = "(" + filterString.replaceAll("\n", "|") + ")";
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}

	public Pattern compileReplaceRegexPattern(URI[] uriArray) {
		String filterString = "";
		String regex = "";
		String endRegex = "[.。!！…~〜\n \t\f\r]*";
		String nEndRegex = "[^.。!！…~〜\n \t\f\r]*";
		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			
			// uncomment this line for local testing/debugging mode
			// String filename = uriPath.toString();
			if (filename.toLowerCase().contains("replacepart.txt")) {
				filterString = filterString + readFile(filename);
				break;
			} else {
				filterString = "placeholder";
			}
		}
		if (filterString != null && filterString.length() > 1) {
			filterString = filterString.substring(0, filterString.length() - 1);
			// try to match the whole sentence that contains the
			// KEYWORDS
			regex = "(" + nEndRegex + filterString.replaceAll("\n", nEndRegex + endRegex + "|" + nEndRegex) + nEndRegex
					+ endRegex + ")";
		}
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}
	

	public Pattern compilePattern(URI[] uriArray) {
		StringBuilder filterBuilder = new StringBuilder();
		String regex = "";
		String filename = "";
		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			filename = uriPath.getName().toString();
			//filename = uriPath.toString();
			if(filename.toLowerCase().contains("symbol")){
				continue;
			}
			filterBuilder.append(readFile(filename));
			if (filterBuilder.length() > 1) {
				String filterString = filterBuilder.toString().substring(0, filterBuilder.toString().length() - 1);
				StringBuilder builder = new StringBuilder();
				if(filename.toLowerCase().contains("relations.txt")){
					builder.append("我" + filterString.replaceAll("\n", "|我")).append("|");
					builder.append("我大" + filterString.replaceAll("\n", "|我大")).append("|");
					builder.append("我小" + filterString.replaceAll("\n", "|我小")).append("|");
					builder.append("我老" + filterString.replaceAll("\n", "|我老")).append("|");
					builder.append("我好" + filterString.replaceAll("\n", "|我好"));
					regex = "(" + builder.toString() + ")";
				}else{
					regex = "(" + filterString.replaceAll("\n", "|") + ")";
				}
			}
		}
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}
	
	public Pattern compileTopicPattern(URI[] uriArray) {
		String filterString = "";
		String regex = "";

		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			filterString = filterString + readFile(filename);
		}
		if (filterString.length() > 1) {
			filterString = filterString.substring(0, filterString.length() - 1);
		}
		regex = "(" + filterString.replaceAll("\n", "|") + ")";
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}
	
	public Boolean isFilterNeeded(Pattern p, String input) {
		// uncomment the following block for local testing/debugging mode
//		 Matcher m = p.matcher(input);
//		 while (m.find()) {
//			 System.out.println(m.group());
//		 }
		return p.matcher(input).find();
	}
	
	public static boolean checkNERStr(String url, String text){
		boolean flag = false;
		String response = HttpUtils.callService(url, text, "t");
		try{
			JSONObject responseOb = JSONObject.fromObject(response);
	        if(responseOb.has("persons") && responseOb.getJSONArray("persons").size() > 0){
	      //  	System.out.println(responseOb);
	        	flag = true;
	        }
	        return flag;
		}catch(Exception e){
			System.out.println( "text: " + text);
			return false;
		}
	}
}
