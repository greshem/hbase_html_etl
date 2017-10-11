package com.petty.etl.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class WeiboJsonMerge1Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String description = "";
		String url = "";
		String source = "";
		String rawQuestion = "";
//		String uuid = "";
		JSONArray answers = new JSONArray();
		
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
		HashMap<String, String> answerUUIDMap = new HashMap<String, String>();
		
		List<JSONObject> list = new ArrayList<JSONObject>();
		Set<String> tagSet = new HashSet<String>();
		boolean containIncreFlag = false;
		Iterator<Text> it = values.iterator();
		while(it.hasNext()){
			String object = it.next().toString();
			JSONObject json = JSONObject.fromObject(object);
			String question = json.getString(Constants.QUESTION);
			JSONArray answerArray = json.getJSONArray(Constants.ANSWERS);
			String tmpUrl = json.getString(Constants.URL);
			String tmpDescription = json.getString(Constants.DESCRIPTION);
			int increFlagValue = 0;
			if(!json.containsKey(Constants.INCREFLAG)){
				json.put(Constants.INCREFLAG, 0);
			}else{
				increFlagValue = json.getInt(Constants.INCREFLAG);
			}
			
			JSONArray tagArray = new JSONArray();
			if(json.has(Constants.TAGS)){
				tagArray = json.getJSONArray(Constants.TAGS);
				if(tagArray != null && tagArray.size() > 0){
					for(int i=0; i<tagArray.size(); i++){
						tagSet.add(tagArray.getString(i));
					}
				}
			}
			// 从http://m.weibo.cn/page/json取到的数据， 或者还没有回复的
			if(tmpUrl != null && tmpUrl.startsWith("http://m.weibo.cn/page/json")){
				description = tmpDescription;
				url = tmpUrl;
				source = json.getString(Constants.SOURCE);
				rawQuestion = question;
//				if(json.has(Constants.UUID)){
//					uuid = json.getString(Constants.UUID);
//				}else{
//					uuid = UUID.randomUUID().toString();
//				}
				if(answerArray != null && answerArray.size() > 0){
					getAnswerMap(answerArray, answerMap, answerUUIDMap);
					if(increFlagValue == 1){
						containIncreFlag = true;
					}
				}
			}
			// 从http://m.weibo.cn/single/rcList取到的， 只有评论内容还没有question的
			if(tmpUrl != null && tmpUrl.startsWith("http://m.weibo.cn/single/rcList")){
				if(question != null && "".equalsIgnoreCase(question)){ // 还没有关联到question的评论
					if(answerArray != null && answerArray.size() > 0){
						getAnswerMap(answerArray, answerMap, answerUUIDMap);
						if(increFlagValue == 1){
							containIncreFlag = true;
						}
					}
				}else if(question != null && !"".equalsIgnoreCase(question)){
					// 回复中的回复
					list.add(json);
				}
			}
		}
		
		Set<String> answerKeys = answerMap.keySet();
		for(String answerKey: answerKeys){
			JSONObject answer = new JSONObject();
			answer.put(Constants.CONTENT, answerKey);
			answer.put("likecount", answerMap.get(answerKey));
			
			answers.add(answer);
		}

		if(!"".equalsIgnoreCase(rawQuestion) || answers.size() > 0){
			JSONObject newQAOb = new JSONObject();
			newQAOb.put(Constants.TITLE, "");
			newQAOb.put(Constants.QUESTION, rawQuestion);
			newQAOb.put(Constants.DESCRIPTION, description);
			newQAOb.put(Constants.ANSWERS, answers);
			newQAOb.put(Constants.URL, url);
			newQAOb.put(Constants.TAGS, tagSet.toArray());
			newQAOb.put(Constants.SOURCE, source);
			newQAOb.put(Constants.ID, key.toString());
			if(containIncreFlag){
				newQAOb.put(Constants.INCREFLAG, 1);
			}else{
				newQAOb.put(Constants.INCREFLAG, 0);
			}
//			newQAOb.put(Constants.UUID, uuid);
			context.write(new Text(newQAOb.toString()), new Text());
		}
		
		/*
		 *  对于回复中的回复，在这一步不能建立UUID， 否则在第二步merge的时候， 就不能保证最终选择的那个UUID与history的保持一致。
		 *  如果是history里面已经设置过了， 则会在下一步merge继续保留；
		 *  如果是新的数据，则会在下一步进行创建
		 */
		
		for(int i=0; i<list.size(); i++){
			JSONObject ob = list.get(i);
			ob.put(Constants.DESCRIPTION, description);
			context.write(new Text(ob.toString()), new Text());
	    }
	}
	
	public static void getAnswerMap(JSONArray answerArray, HashMap<String, Integer> answerMap, HashMap<String, String> answerUUIDMap){
		for(int i=0; i<answerArray.size(); i++){
			JSONObject answerObject = answerArray.getJSONObject(i);
			String answerContent = answerObject.getString(Constants.CONTENT).trim();
			String likeCount = answerObject.getString("likecount");
			
			if(likeCount == null || "null".equalsIgnoreCase(likeCount)){
				likeCount = "0";
			}
			if(answerMap.containsKey(answerContent)){
				int sumLike = answerMap.get(answerContent) + Integer.valueOf(likeCount);
				answerMap.put(answerContent, Integer.valueOf(sumLike));
			}else{
				answerMap.put(answerContent, Integer.valueOf(likeCount));
			}
			
//			if(answerObject.has(Constants.UUID)){
//				String answerUUID = answerObject.getString(Constants.ANSWER_UUID);
//				if(!answerUUIDMap.containsKey(answerContent)){
//					answerUUIDMap.put(answerContent, answerUUID);
//				}
//			}
		}
	}
}
