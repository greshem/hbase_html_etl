package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class UpdateSolrMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString().trim();
			try{
				JSONObject jsonOb = JSONObject.fromObject(line);
				String question = jsonOb.getString(Constants.QUESTION);
				String source = jsonOb.getString(Constants.SOURCE);
				JSONArray answerArray = jsonOb.getJSONArray(Constants.ANSWERS);
				JSONObject newJsonOb = new JSONObject();
				newJsonOb.put(Constants.QUESTION, question);
				newJsonOb.put(Constants.ANSWERS, answerArray);
				newJsonOb.put(Constants.SOURCE, source);
				context.write(new Text(question+source), new Text(newJsonOb.toString()));
			}catch(JSONException e){
				context.write(new Text(line), new Text(line));
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
