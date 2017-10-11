package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.HanlpUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SegKeywordMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			JSONObject lineOb = JSONObject.fromObject(line);
			String question = lineOb.getString(Constants.QUESTION);
			if(question != null && !"".equalsIgnoreCase(question)){
				lineOb.put(Constants.QUESTION_SEG, HanlpUtil.getWords(question));
				lineOb.put(Constants.QUESTION_KEYWORD, HanlpUtil.getKeywords(question));
			}
			JSONArray answers = lineOb.getJSONArray(Constants.ANSWERS);
			JSONArray newAnswers = new JSONArray();
			for(int i=0; i<answers.size(); i++){
				JSONObject answer = answers.getJSONObject(i);
				answer.put(Constants.ANSWER_SEG, HanlpUtil.getWords(answer.getString(Constants.CONTENT)));
				answer.put(Constants.ANSWER_KEYWORD, HanlpUtil.getKeywords(answer.getString(Constants.CONTENT)));
				newAnswers.add(answer);
			}
			lineOb.put(Constants.ANSWERS, newAnswers);
			context.write(new Text(lineOb.toString()), new Text());
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
		}
	}


}
