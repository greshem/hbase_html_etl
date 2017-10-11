package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ExtractMultipleAnswerMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			
			String line = value.toString();
			JSONObject lineOb = JSONObject.fromObject(line);
			String question =  lineOb.getString(Constants.QUESTION);
			JSONArray answers = lineOb.getJSONArray(Constants.ANSWERS);
			if(answers.size() >=1 && answers.size() <= 50){
				for(int i=0; i<answers.size(); i++){
					String answer = getContent(answers.getString(i));
					context.write(new Text(question), new Text(answer));
				}
			}else if(answers.size() > 50){
				int num = 0;
				for(int i=0; i<answers.size(); i++){
					if(num < 50){
						String answer = getContent(answers.getString(i));
						context.write(new Text(question), new Text(answer));
						num++;
					}else{
						break;
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
			// e.printStackTrace();
		}
	}

	public static String getContent(String text){
		
		String finalText = "";
		try{
			JSONObject object = JSONObject.fromObject(text);
			finalText = object.getString(Constants.CONTENT);
		}catch(JSONException e){
			finalText = text;
		}
		
		return finalText;
		
	}
}
