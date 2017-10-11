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

public class RandomSampleMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private double rate = 0.0f;
	private MultipleOutputs<Text, NullWritable> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
		rate = context.getConfiguration().getDouble("rate", 0);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject lineObject = JSONObject.fromObject(line);
		
		JSONArray answers = lineObject.getJSONArray(Constants.ANSWERS);
		JSONArray selectedAnswers = new JSONArray();

		// zhihu (score between 0.8-0.9) has partly selected answers
		JSONArray partSelectedAnswers = new JSONArray();

		double rand = Math.random();
		int alreadySelect = 0;

		for (Object answer : answers) {
			JSONObject answerObject = new JSONObject();
			try {
				answerObject = JSONObject.fromObject(answer.toString());
				alreadySelect = answerObject.getInt("select");

			} catch (JSONParseException e) {
				answerObject.put("content", answer.toString());
				answerObject.put("likecount", 0);
			}

			answerObject.put("select", rand < rate ? 1 : (0 + alreadySelect));
			selectedAnswers.add(answerObject);
			if (alreadySelect == 0) {
				partSelectedAnswers.add(answerObject);
			}
		}

		lineObject.put(Constants.ANSWERS, selectedAnswers);
		mos.write(new Text(lineObject.toString()), NullWritable.get(), "tag/part");

		if (rand < rate && partSelectedAnswers.size() > 0) {
			lineObject.put(Constants.ANSWERS, partSelectedAnswers);
			mos.write(new Text(lineObject.toString()), NullWritable.get(), "selected/part");
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}

}
