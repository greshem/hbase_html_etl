package com.petty.etl.mappers;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class RemoveUUIDMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			JSONObject jsonOb = JSONObject.fromObject(line);
			if(jsonOb.has(Constants.UUID)){
				jsonOb.remove(Constants.UUID);
			}
			context.write(new Text(jsonOb.toString()), new Text());
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
