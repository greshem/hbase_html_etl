package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class GetValidQAMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			JSONObject lineObject = JSONObject.fromObject(value.toString());
			String question = lineObject.getString(Constants.QUESTION);
			JSONArray answerArray = lineObject.getJSONArray(Constants.ANSWERS);
			if(question != null && !"".equalsIgnoreCase(question)
					&& answerArray != null && answerArray.size() > 0){
				context.write(value, new Text());
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
