package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class ExtarctQuestionSourceMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			JSONObject jsonOb = JSONObject.fromObject(line);
			String question = jsonOb.getString(Constants.QUESTION);
			String source = jsonOb.getString(Constants.SOURCE);
			context.write(new Text(question+source), new Text());
			
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
