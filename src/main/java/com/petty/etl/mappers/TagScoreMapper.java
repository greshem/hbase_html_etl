package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.HttpUtils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TagScoreMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String url = null;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		url = context.getConfiguration().get("url", "");
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject record = JSONObject.fromObject(line);
		String question = record.getString("question");
		JSONArray answers = record.getJSONArray("answers");
		JSONArray scoredAnswers = new JSONArray();
		String answer = null;
		float score = 0.0f;
		for (Object answerObj : answers) {
			JSONObject answerJson = JSONObject.fromObject(answerObj);
			try {
				
				if(answerJson.has("content")){
					answer = answerJson.getString("content").replaceAll("&", "");
				}
				else{
					answer = answerObj.toString().replaceAll("&", "");
				}
			} catch (Exception JSONException) {
				answer = answerObj.toString().replaceAll("&", "");
			}
			
			String score_original = HttpUtils.getUrlResult(url, question, answer);
			try {
				score = Float.parseFloat(score_original);
			} catch (Exception e) {
				System.out.println("parse float error");
				score = -1;
			}

//			JSONObject scoredAnswer = new JSONObject();
			answerJson.put("content", answer);
			answerJson.put("score", score);
			scoredAnswers.add(answerJson);
		}
		record.put("answers", scoredAnswers);
		context.write(new Text(record.toString()), new Text());
	}

	public static void main(String[] args) {
		for (int i = 0; i < 10; i++) {
			String url = "http://192.168.1.101:9001/qaScore?";
			String question = "= = 你她是吧，哭笑不得";
			String answer = "信不信你信不信真的有什么关系么。";
			String result = HttpUtils.getUrlResult(url, question, answer);
			float score = Float.parseFloat(result);
			System.out.println("score:\t" + score);
		}
	}

}
