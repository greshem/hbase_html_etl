package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.petty.etl.commonUtils.CalendarUtil;
import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


public class WeiboJsonExtractor extends BaseExtractor {
	
	public static void main(String[] args) {
		String html = FileUtil.readFileToString("/Users/greshem/Downloads/container.txt");
		String url = "http://m.weibo.cn/page/json?containerid=1005051193491727_-_WEIBO_SECOND_PROFILE_WEIBO&page=1";
//		String html = FileUtil.readFileToString("/Users/greshem/Documents/test.js");
//		String url = "http://m.weibo.cn/single/rcList?format=cards&id=3934577720414197&type=comment&hot=0&page=2";
		WeiboJsonExtractor ex = new WeiboJsonExtractor();
		System.out.println("21321");
		List<JSONObject> obs = ex.extractHbaseData(html, "", url, null, null);
		for(JSONObject ob: obs){
			System.out.println(Constants.QUESTION + ": \n" + ob.get(Constants.QUESTION) + "\n");
			System.out.println(Constants.ANSWERS + ":");
			JSONArray array = (JSONArray) ob.get(Constants.ANSWERS);
			if(array != null){
				for(int i=0;i<array.size(); i++){
					System.out.println("\t" + array.get(i));
				}
			}
			System.out.println(Constants.DESCRIPTION + ": \n" + ob.get(Constants.DESCRIPTION) );
			System.out.println("id: \n" + ob.get("id") );
			System.out.println("==================================================");
		}
//		
//		getUnixTime("2015-03-01 10:25", "Wed Mar 23 17:03:08 2016");
		
		System.out.println(isFirstPage("http://m.weibo.cn/single/rcList?format=cards&id=3927236056539928&type=comment&hot=0&page=2"));
	}
	
	public List<JSONObject> extract(String data) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		return result;
	}
	
	protected int getDataSource() {
		dataSource = WEIBO;
		return dataSource;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		JSONArray tagArray = new JSONArray();
		if(tag != null && !"".equalsIgnoreCase(tag)){
			tagArray.add(tag);
		}
		if(url != null && htmlBody != null && !"".equalsIgnoreCase(htmlBody)){
			if(url.startsWith("http://m.weibo.cn/page/json")){
				JSONObject htmlObject = JSONObject.fromObject(htmlBody);
				if(htmlObject.has("cards")){
					JSONArray cards = htmlObject.getJSONArray("cards");
					JSONObject cardsInfo = cards.getJSONObject(0);
					if(!cardsInfo.has("card_group")){
						return result;
					}
					JSONArray posts = cardsInfo.getJSONArray("card_group");
					for(int i=0; i<posts.size(); i++){
						String text = "";
						String id = "";
						JSONObject object = new JSONObject();
						JSONObject post = posts.getJSONObject(i);
						if(post.has("mblog")){
							JSONObject mblog = post.getJSONObject("mblog");
							if(mblog.has("text")){
								text = mblog.getString("text");
								text = SymbolUtil.ToDBC(text);
							}
							if(mblog.has("id")){
								id = mblog.getString("id");
							}
							text = removeForfardText(text);
							
							if("".equalsIgnoreCase(text)){
								continue;
							}
							
//							long time = 0L;
//							if(mblog.has("created_timestamp")){
//								time = mblog.getLong("created_timestamp");
//							}
							
							object.put(Constants.TITLE, "");
							object.put(Constants.QUESTION, text);
							object.put(Constants.DESCRIPTION, text);
							object.put(Constants.ANSWERS, new JSONArray());
							object.put(Constants.TAGS, tagArray);
							object.put(Constants.ID, id);
							object.put(Constants.SOURCE, WEIBO);
							object.put(Constants.URL, url);
							object.put(Constants.INCREFLAG, 1);
//							object.put("created", time);
							
							result.add(object);
						}
					}
				}
			}else if(url.startsWith("http://m.weibo.cn/single/rcList")){
				JSONArray responseArray = JSONArray.fromObject(htmlBody);
				if(responseArray != null && responseArray.size() < 1){
					return result;
				}
				JSONObject response = new JSONObject();
				if(isFirstPage(url)){
					if(responseArray.size() > 1){
						response = responseArray.getJSONObject(1);
					}else{
						return result;
					}
				}else{
					response = responseArray.getJSONObject(0);
				}
				if(response.has("card_group")){
					JSONArray cardArray = response.getJSONArray("card_group");
					for(int i=0; i<cardArray.size(); i++){
						String question = "";
						String answer = "";
						String likeCnt = "";
						String commentId = "";
						JSONObject object = new JSONObject();
						JSONObject card = cardArray.getJSONObject(i);
						JSONArray answerArray = new JSONArray();
						if(card.has("text")){
							answer = card.getString("text");
							answer = SymbolUtil.ToDBC(answer);
							answer = removeForfardText(answer);
						}
						if(card.has("like_counts")){
							likeCnt = card.getString("like_counts");
						}
						if(card.has("reply_text")){
							question = card.getString("reply_text");
							question = SymbolUtil.ToDBC(question);
							question = removeForfardText(question);
						}
						if(card.has("url")){
							commentId = getCommendsId(card.getString("url"));
						}
						

//						String time = "";
//						if(card.has("created_at")){
//							time = card.getString("created_at");
//						}
//						long replyTime = 0L;
//						if(time!=null && !"".equalsIgnoreCase(time)){ // 03-01 23:49  今天 10:25
//							replyTime = getUnixTime(time, updateTime);
//						}
						
						
						if(!"".equalsIgnoreCase(answer)){
							JSONObject answerOb = new JSONObject();
							answerOb.put("content", answer);
							answerOb.put("likecount", likeCnt);
//							answerOb.put("created", replyTime);
							answerArray.add(answerOb);
						}
						
						if(answerArray.size() == 0){
							continue;
						}
						
						object.put(Constants.TITLE, "");
						object.put(Constants.QUESTION, question);
						object.put(Constants.DESCRIPTION, question);
						object.put(Constants.ANSWERS, answerArray);
						object.put(Constants.TAGS, tagArray);
						object.put(Constants.ID, commentId);
						object.put(Constants.SOURCE, WEIBO);
						object.put(Constants.URL, url);
						object.put(Constants.INCREFLAG, 1);
						
						result.add(object);
					}
				}
				
			}
		}
		return result;
	}
	
	public static String removeEmoticon(String text, String tag){
		Pattern pattern = Pattern.compile("(<" + tag + ".*?/" + tag + ">)");
        Matcher matcher = pattern.matcher(text);
        String afterRemove = text;
        String matches = "";
        while(matcher.find()){
        	matches = matcher.group();
        	afterRemove = afterRemove.replace(matches, "");
        }
		return afterRemove;
	}
	
	public static String removeForfardText(String text){
		String afterRemove = text;
		afterRemove = removeEmoticon(afterRemove, "i");  // 删除表情符号
		afterRemove = removeEmoticon(afterRemove, "a").replace(" ", ""); // 删除@超链接
		afterRemove = afterRemove.replace("回复:", "").replace("回覆:", "").replace("转发微博", "");
		int indexPos = afterRemove.indexOf("//:");
		if(indexPos != -1){
			afterRemove = afterRemove.substring(0, indexPos);
		}
		return afterRemove.replace(":", "").trim();
	}
	
	public static String getCommendsId(String url){
		String commentId = "";
		String[] array = url.split("\\?");
		StringBuilder parameter = new StringBuilder();
		if(array.length > 1){
			for(int i=1; i<array.length; i++){
				parameter.append(array[i]).append("?");
			}
		}
		String param = parameter.toString().substring(0, parameter.toString().length() - 1);
		String[] paramArray = param.split("&");
		for(int i=0; i<paramArray.length; i++){
			String paramString = paramArray[i];
			String[] keyValue = paramString.split("=");
			if(keyValue.length == 2 && "id".equalsIgnoreCase(keyValue[0])){
				commentId = keyValue[1];
			}
		}
		return commentId;
	}
	
	public static boolean isFirstPage(String url){
		boolean firstPage = false;
		
		if(url != null && !"".equals(url)){
			String[] array = url.split("\\?");
			if(array.length == 2){
				String params = array[1];
				String[] paramArray = params.split("&");
				for(int i=0; i<paramArray.length; i++){
					String param = paramArray[i];
					String[] keyValue = param.split("=");
					if(keyValue.length == 2){
						if("page".equalsIgnoreCase(keyValue[0])
							&& "1".equalsIgnoreCase(keyValue[1])){
							firstPage = true;
							break;
						}
					}
				}
			}
		}
		
		return firstPage;
	}
	
	public static long getUnixTime(String time, String updateTime){
		long realUnixTime = 0L;
		String[] array = time.split(" ");
		if(array.length == 2){
			long update_time = Date.parse(updateTime);
			String updateStrTime = CalendarUtil.Unix2Datetime(update_time, "yyyy-MM-dd HH:mm:ss");
			String[] updateArray = updateStrTime.split(" ");
			String realTime = "";
			if("今天".equalsIgnoreCase(array[0])){ // time: 今天 10:25
				realTime = updateArray[0] + " " + array[1];
			}else if(array[0].length() == 5){ // time: 03-01 10:25
				String year = updateArray[0].split("-")[0];
				realTime = year + "-" + time;
			}else{ // time: 2015-03-01 10:28
				realTime = time;
			}
//			System.out.println(realTime);
			realUnixTime = CalendarUtil.Datetime2Unix(realTime, "yyyy-MM-dd HH:mm");
//			System.out.println(realUnixTime);
		}
		return realUnixTime;
	}
}
