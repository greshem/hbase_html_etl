package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.HttpUtils;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

/**
 * @author greshem
 *
 */
public class LabelTextMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String url = null;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		url = context.getConfiguration().get(Constants.URL, "");
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject record = JSONObject.fromObject(line);
		String question = record.getString(Constants.QUESTION);
		JSONObject qObject = new JSONObject();
		qObject.put(Constants.CONTENT, question);
		// 把Question打上各module的属性值
		JSONObject questionObject = labelText(url, qObject, "question");
		record.put(Constants.QUESTION, questionObject);
		
		// 给每一个Answer打上各module的属性值
		JSONArray answers = record.getJSONArray(Constants.ANSWERS);
		JSONArray answersArray = new JSONArray();
		for(int i=0; i<answers.size(); i++){
			JSONObject aObject = answers.getJSONObject(i);
			JSONObject answerObject = labelText(url, aObject, "answer");
			answersArray.add(answerObject);
		}
		record.put(Constants.ANSWERS, answersArray);
		context.write(new Text(record.toString()), new Text());
	}
	
	/*
	 *  对每一个Text调用service进行label
	 *  param: 
	 * 
	 */
	public static JSONObject labelText(String url, JSONObject object, String type){
		JSONObject contentObject = JSONObject.fromObject(object.toString());
		String content = object.getString(Constants.CONTENT);
		String response = HttpUtils.callService(url, content, "Text1");
		JSONObject responseOb = new JSONObject();
		try{
			responseOb = JSONObject.fromObject(response);
			// 取得分值最高的Emotion值
			JSONObject emotionObject = responseOb.getJSONObject(Constants.EMOTION);
			String emotionText = getTargetValue(emotionObject);
			contentObject.put(Constants.EMOTION, emotionText);
			
			// 取得分值最高的Speech Act值
			JSONObject speechActObject = responseOb.getJSONObject(Constants.SPEECHACT);
			String speechActText = getTargetValue(speechActObject);
			contentObject.put(Constants.SPEECHACT, speechActText);
			
			// 取得分值最高的Topic值
			JSONObject topicObject = responseOb.getJSONObject(Constants.TOPIC);
			String topicText = getTargetValue(topicObject);
			contentObject.put(Constants.TOPIC, topicText);
		}catch(JSONException e){
			System.out.println( "Content: " + content);
		}
		return contentObject;
	}
	
	public static String getTargetValue(JSONObject jsonObject){
		String targetValue = "";
		double maxValue = 0.0;
		JSONArray array = jsonObject.getJSONArray("res");
		for(int i=0; i<array.size(); i++){
			JSONObject object = array.getJSONObject(i);
			double value = object.getDouble("score");
			if(value > maxValue){
				targetValue = object.getString("item");
			}
		}
		return targetValue;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
}
