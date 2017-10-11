package com.petty.etl.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class MergeSolrHistoryReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		
		while(it.hasNext()){
			String jsonOb = it.next().toString();
			JSONObject object = JSONObject.fromObject(jsonOb);
			
			String questionEmotion = object.getString(Constants.EMOTION);
			if("".equalsIgnoreCase(questionEmotion)){
				continue;
			}
			boolean flag = true;
			JSONArray answers = object.getJSONArray(Constants.ANSWERS);
			for(int i=0; i<answers.size(); i++){
				JSONObject answer = answers.getJSONObject(i);
				if(!answer.has(Constants.EMOTION)){
					flag = false;
					break;
				}
				String answerEmotion = answer.getString(Constants.EMOTION);
				if("".equalsIgnoreCase(answerEmotion)){
					flag = false;
					break;
				}
			}
			if(!flag){
				continue;
			}
			context.write(new Text(jsonOb), new Text());
			break;
		}
	}
}
