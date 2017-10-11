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


public class DoubanMergePageReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String keyvalue = key.toString();
		JSONObject targetObject = new JSONObject();
		Set<String> answerSet = new HashSet<String>();
		if(keyvalue.startsWith("{") && keyvalue.endsWith("}")){
			context.write(key, new Text());
		}else{
			Iterator<Text> it = values.iterator();
			while(it.hasNext()){
				String line = it.next().toString();
				if("".equalsIgnoreCase(line)){
					continue;
				}
				JSONObject object = JSONObject.fromObject(line);
				String question = object.getString(Constants.QUESTION);
				JSONArray answerArray = object.getJSONArray(Constants.ANSWERS);
				if(question != null && !"".equalsIgnoreCase(question)){
					targetObject = object;
				}
				for(int i=0; i<answerArray.size(); i++){
					answerSet.add(answerArray.getString(i).trim());
				}
			}
			JSONArray targetAnswers = new JSONArray();
			for(String answer : answerSet){
				targetAnswers.add(answer);
			}
			targetObject.put(Constants.ANSWERS, targetAnswers);
			context.write(new Text(targetObject.toString()), new Text());
		}
	}
}
