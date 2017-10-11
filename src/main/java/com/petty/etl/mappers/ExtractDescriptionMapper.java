package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ExtractDescriptionMapper extends Mapper<Text, NullWritable, Text, Text>{
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
	
	@Override	
	public void map(Text key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = key.toString().trim();
			//line = line.substring(line.indexOf("{"), line.lastIndexOf("}") +1);
			if(line != null && line.length() < 1000){
				JSONObject jsonOb = JSONObject.fromObject(line);
				String question = jsonOb.getString(Constants.QUESTION);
				if(question.length() >= 5){
					context.write(new Text(question), new Text(""));
				}
				try{
					JSONArray descArr = jsonOb.getJSONArray(Constants.DESCRIPTION);
					for(int i=0; i<descArr.size(); i++){
						String desc = descArr.getString(i);
						if(desc.length() >= 5){
							context.write(new Text(desc), new Text(""));
						}
					}
				}catch(JSONException e){
					String desc = jsonOb.getString(Constants.DESCRIPTION);
					if(desc.length() >= 5){
						context.write(new Text(desc), new Text(""));
					}
				}
				JSONArray answerArray = jsonOb.getJSONArray(Constants.ANSWERS);
				for(int i=0; i<answerArray.size(); i++){
					String answer = answerArray.getString(i);
					try{
						JSONObject answerOb = JSONObject.fromObject(answer);
						String answerString = answerOb.getString(Constants.CONTENT);
						if(answerString.length() >= 5){
							context.write(new Text(answerString), new Text(""));
						}
					}catch(JSONException e){
						if(answer.length() >= 5){
							context.write(new Text(answer), new Text(""));
						}
					}
				}
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
	
}
