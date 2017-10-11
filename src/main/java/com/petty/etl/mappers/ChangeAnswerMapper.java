package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ChangeAnswerMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			JSONObject jsonOb = JSONObject.fromObject(line);
			String answer = jsonOb.getString(Constants.ANSWERS);
			JSONObject answerOb = new JSONObject();
			answerOb.put(Constants.CONTENT, answer);
			answerOb.put(Constants.SELECT, 0);
			JSONArray answerArray = new JSONArray();
			answerArray.add(answerOb);
			jsonOb.put(Constants.ANSWERS, answerArray);
			jsonOb.put(Constants.INCREFLAG, 1);
			context.write(new Text(jsonOb.toString()), new Text());
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
