package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class WeiboJsonMerge2Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		JSONArray mergeAnswer = new JSONArray();
		String mergeQuestion = "";
		String mergeDescription = "";
		String mergeSource = "";
		String mergeUrl = "";
		String mergeCommentId = "";
//		String mergeUUID = "";
		
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
//		HashMap<String, Long> answerCreatedMap = new HashMap<String, Long>();

		Set<String> tagSet = new HashSet<String>();
		boolean increFlag = false;
		int i = 0;
		while(it.hasNext()){
			String jsonOb = it.next().toString();
			JSONObject object = JSONObject.fromObject(jsonOb);
			if(object.getInt(Constants.INCREFLAG) == 1){
				increFlag = true;
			}
			JSONArray tagArray = new JSONArray();
			if(object.has(Constants.TAGS)){
				tagArray = object.getJSONArray(Constants.TAGS);
				if(tagArray != null && tagArray.size() > 0){
					for(int j=0; j<tagArray.size(); j++){
						tagSet.add(tagArray.getString(j));
					}
				}
			}
			String question = object.getString(Constants.QUESTION);
			if(question != null && !"".equalsIgnoreCase(question)){
				if(i == 0){
					mergeQuestion = question;
					mergeDescription = object.getString(Constants.DESCRIPTION);
					mergeSource = object.getString(Constants.SOURCE);
					mergeUrl = object.getString(Constants.URL);
					mergeCommentId = object.getString(Constants.ID);
				}
				JSONArray answers = object.getJSONArray(Constants.ANSWERS);
				for(int j=0; j<answers.size(); j++){
					JSONObject answerObject = answers.getJSONObject(j);
					String answerContent = answerObject.getString(Constants.CONTENT).trim();
					String likeCount = answerObject.getString("likecount");
//					long created = answerObject.getLong("created");
					if(likeCount == null || "null".equalsIgnoreCase(likeCount)){
						likeCount = "0";
					}
					if(answerMap.containsKey(answerContent)){
						int sumLike = answerMap.get(answerContent) + Integer.valueOf(likeCount);
						answerMap.put(answerContent, Integer.valueOf(sumLike));
					}else{
						answerMap.put(answerContent, Integer.valueOf(likeCount));
//						answerCreatedMap.put(answerContent, created);
					}
				}
				i++;
			}else if(question != null && "".equalsIgnoreCase(question)){
				context.write(new Text(object.toString()), new Text());
			}
			
			/*
			 * 检查是否带有UUID的纪录，如果有，则表示history数据中已经含有过这条纪录，继续作为UUID；
			 * 如果没有，则表示是全新的数据， 需要创建新的
			 */
//			if(object.has(Constants.UUID)){
//				mergeUUID = object.getString(Constants.UUID);
//			}
		}
		
		Set<String> answerKeys = answerMap.keySet();
		for(String answerKey: answerKeys){
			JSONObject answer = new JSONObject();
			answer.put(Constants.CONTENT, answerKey);
			answer.put("likecount", answerMap.get(answerKey));
//			answer.put("created", answerCreatedMap.get(answerKey));
			mergeAnswer.add(answer);
		}
		
		if(!"".equalsIgnoreCase(mergeQuestion)){
			JSONObject finalObject = new JSONObject();
			finalObject.put(Constants.TITLE, "");
			finalObject.put(Constants.QUESTION, mergeQuestion);
			finalObject.put(Constants.ANSWERS, mergeAnswer);
			finalObject.put(Constants.DESCRIPTION, mergeDescription);
			finalObject.put(Constants.TAGS, tagSet.toArray());
			finalObject.put(Constants.URL, mergeUrl);
			finalObject.put(Constants.SOURCE, mergeSource);
			finalObject.put(Constants.ID, mergeCommentId);
			if(increFlag){
				finalObject.put(Constants.INCREFLAG, 1);
			}else{
				finalObject.put(Constants.INCREFLAG, 0);
			}
//			if("".equalsIgnoreCase(mergeUUID)){
//				mergeUUID = UUID.randomUUID().toString();
//			}
//			finalObject.put(Constants.UUID, mergeUUID);
			context.write(new Text(finalObject.toString()), new Text());
		}
	}
}
