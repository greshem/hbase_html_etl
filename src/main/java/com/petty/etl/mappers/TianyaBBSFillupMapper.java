package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class TianyaBBSFillupMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			if(line != null){
				JSONObject jsonOb = new JSONObject(line);
				String title = jsonOb.getString("title");
				if(title != null && !"".equalsIgnoreCase(title)){
					context.write(new Text(title), new Text(jsonOb.toString()));
				}
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
