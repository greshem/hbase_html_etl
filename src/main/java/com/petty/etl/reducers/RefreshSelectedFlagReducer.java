package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class RefreshSelectedFlagReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		JSONObject newDataJson = new JSONObject();
		
		Set<String> selectedSet = new HashSet<String>();
		Iterator<Text> it = values.iterator();
		while(it.hasNext()){
			String object = it.next().toString();
			JSONObject jsonObject = JSONObject.fromObject(object);
			JSONArray answerArray = jsonObject.getJSONArray(Constants.ANSWERS);
			if(jsonObject.has(Constants.LATEST)){ // 如果json中包含了LATEST，则说明是最新process的数据
				newDataJson = JSONObject.fromObject(object);
			}else{
				for(int i=0; i<answerArray.size(); i++){
					JSONObject answer = answerArray.getJSONObject(i);
					int selectedFlag = answer.getInt(Constants.SELECT);
					if(selectedFlag == 1){
						selectedSet.add(answer.getString(Constants.CONTENT));
					}
				}
			}
		}
		
		// 更新最新process数据的select flag
		JSONArray answersArray = newDataJson.getJSONArray(Constants.ANSWERS);
		JSONArray newAnswerArray = new JSONArray();
		for(int i=0; i<answersArray.size(); i++){
			JSONObject answer = answersArray.getJSONObject(i);
			String content = answer.getString(Constants.CONTENT);
			if(selectedSet.contains(content)){
				answer.put(Constants.SELECT, 1);
			}
			newAnswerArray.add(answer);
		}
		newDataJson.put(Constants.ANSWERS, newAnswerArray);
		
		// 此时数据应该作为下次refresh的history数据，需要remove掉LATEST属性
		newDataJson.remove(Constants.LATEST);
		
		context.write(new Text(newDataJson.toString()), new Text());
	}
}
