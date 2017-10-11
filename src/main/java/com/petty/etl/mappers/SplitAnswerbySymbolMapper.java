package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class SplitAnswerbySymbolMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			if (line != null) {
				JSONObject jsonOb = JSONObject.fromObject(line);
				String question = jsonOb.getString(Constants.QUESTION);
				JSONArray answers = jsonOb.getJSONArray(Constants.ANSWERS);
				JSONArray newAnswerArray = new JSONArray();
				for(int i=0; i<answers.size(); i++){
					JSONObject answer = answers.getJSONObject(i);
					String answerString = answer.getString(Constants.CONTENT);
					String[] sentences  = answerString.split(",|，|.|。|！|!|?|？");
					for(String sentence : sentences){
						if(!"".equalsIgnoreCase(sentence.trim())){
							JSONObject answerOb = new JSONObject();
							answerOb.put(Constants.CONTENT, sentence.trim());
							newAnswerArray.add(answerOb);
						}
					}
				}
				JSONObject qaOb = new JSONObject();
				qaOb.put(Constants.QUESTION, question);
				qaOb.put(Constants.ANSWERS, newAnswerArray);
				context.write(new Text(qaOb.toString()), new Text());
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
