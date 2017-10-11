package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class WeiboJsonMerge2Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			if(line != null){
				JSONObject jsonOb = JSONObject.fromObject(line);
				String commentId = jsonOb.getString(Constants.ID);
				String question = jsonOb.getString(Constants.QUESTION);
				context.write(new Text(commentId+question), new Text(jsonOb.toString()));
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
