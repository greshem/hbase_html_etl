package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class RefreshSelectedFlagMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String jsonString = value.toString();
		try{
			JSONObject jsonObject = JSONObject.fromObject(jsonString);
			String question = jsonObject.getString(Constants.QUESTION);
			String title = jsonObject.getString(Constants.TITLE);
			// 根据title和question作为key
			context.write(new Text(title+question), value);
		}catch(JSONException e){
			System.out.println(jsonString + "\t" + e.getMessage());
		}
	}
}
