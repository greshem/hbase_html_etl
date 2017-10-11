package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class BingMergeReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		JSONArray mergeAnswer = new JSONArray();
		String mergeQuestion = "";
		String mergeSource = "";
		String mergeTitle = "";
		String mergeUrl = "";
//		String mergeUUID = "";
		
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();

		boolean increFlag = false;
		int i = 0;
		while(it.hasNext()){
			String jsonOb = it.next().toString();
			JSONObject object = JSONObject.fromObject(jsonOb);
			if(object.getInt(Constants.INCREFLAG) == 1){
				increFlag = true;
			}
			String question = object.getString(Constants.QUESTION);
			JSONArray answers = object.getJSONArray(Constants.ANSWERS);
			if(question != null && !"".equalsIgnoreCase(question)
					&& answers != null && answers.size() > 0){
				if(i == 0){
					mergeQuestion = question;
					mergeTitle = object.getString(Constants.TITLE);
					mergeSource = object.getString(Constants.SOURCE);
					mergeUrl = object.getString(Constants.URL);
				}
				for(int j=0; j<answers.size(); j++){
					JSONObject answerObject = answers.getJSONObject(j);
					String answerContent = answerObject.getString(Constants.CONTENT).trim();
					int selectFlag = answerObject.getInt(Constants.SELECT);
					if(answerMap.containsKey(answerContent)){
						if(selectFlag == 1){
							answerMap.put(answerContent, selectFlag);
						}
					}else{
						answerMap.put(answerContent, selectFlag);
					}
				}
				i++;
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
			answer.put(Constants.SELECT, answerMap.get(answerKey));
			mergeAnswer.add(answer);
		}
		
		if(!"".equalsIgnoreCase(mergeQuestion)){
			JSONObject finalObject = new JSONObject();
			finalObject.put(Constants.TITLE, mergeTitle);
			finalObject.put(Constants.QUESTION, mergeQuestion);
			finalObject.put(Constants.ANSWERS, mergeAnswer);
			finalObject.put(Constants.URL, mergeUrl);
			finalObject.put(Constants.SOURCE, mergeSource);
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
