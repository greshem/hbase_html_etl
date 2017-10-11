package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.constant.Constants;
import com.mongodb.util.JSONParseException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class QAScoreSampleMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Float minScore = 0F;
	private Float maxScore = 0F;
	private MultipleOutputs<Text, NullWritable> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
		minScore = context.getConfiguration().getFloat("minScore", 0);
		maxScore = context.getConfiguration().getFloat("maxScore", 0);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject lineObject = JSONObject.fromObject(line);
		
		JSONArray answers = lineObject.getJSONArray(Constants.ANSWERS);
		JSONArray selectedAnswers = new JSONArray();

		// zhihu (score between 0.8-0.9) has partly selected answers
		JSONArray partSelectedAnswers = new JSONArray();
		
		JSONArray updateSelectedAnswers = new JSONArray();

		int alreadySelect = 0;
		int finalSelect = 0;
		double score = 0.0f;

		for (Object answer : answers) {
			JSONObject answerObject = new JSONObject();
			try {
				answerObject = JSONObject.fromObject(answer.toString());
				alreadySelect = answerObject.getInt("select");
				score = answerObject.getDouble("score");

			} catch (JSONParseException e) {
				answerObject.put("content", answer.toString());
				answerObject.put("likecount", 0);
			}
			
			finalSelect = (score < maxScore && score >= minScore) ? 1 : alreadySelect;

			answerObject.put("select", finalSelect);
			selectedAnswers.add(answerObject);
			if (alreadySelect == 0 && finalSelect == 1) {
				partSelectedAnswers.add(answerObject);
			}
			if (finalSelect == 0){
				updateSelectedAnswers.add(answerObject);
			}
		}

		lineObject.put(Constants.ANSWERS, selectedAnswers);
		mos.write(new Text(lineObject.toString()), NullWritable.get(), "tag/part");

		if (score < maxScore && score >= minScore && partSelectedAnswers.size() > 0 ) {
			lineObject.put(Constants.ANSWERS, partSelectedAnswers);
			mos.write(new Text(lineObject.toString()), NullWritable.get(), "select/part");
		}
		
		if(updateSelectedAnswers.size()>0){
			lineObject.put(Constants.ANSWERS, updateSelectedAnswers);
			mos.write(new Text(lineObject.toString()), NullWritable.get(), "update/part");
		}else{
			lineObject.put(Constants.ANSWERS, updateSelectedAnswers);
			mos.write(new Text(lineObject.toString()), NullWritable.get(), "delete/part");
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}

}
