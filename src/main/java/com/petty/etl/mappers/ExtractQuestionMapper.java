package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ExtractQuestionMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private MultipleOutputs<Text, Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString().trim();
			JSONObject jsonOb = JSONObject.fromObject(line);
			String question = jsonOb.getString(Constants.QUESTION);
			mos.write(new Text(question), new Text(), "q/part");
			mos.write(new Text("Q:"+question), new Text(), "qa/part");
			JSONArray answers = jsonOb.getJSONArray(Constants.ANSWERS);
			for(int i=0; i<answers.size(); i++){
				JSONObject answer = answers.getJSONObject(i);
				mos.write(new Text(answer.getString(Constants.CONTENT)), new Text(), "qa/part");
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
}
