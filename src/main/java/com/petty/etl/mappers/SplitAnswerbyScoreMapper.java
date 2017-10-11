package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class SplitAnswerbyScoreMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos;
	private Float minScore = 0F;
	private Float maxScore = 0F;
	private String folderSuffix = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
		minScore = context.getConfiguration().getFloat("minScore", 0);
		maxScore = context.getConfiguration().getFloat("maxScore", 0);
		folderSuffix = String.format("%d_%d/part", (int)(minScore*100), (int)(maxScore*100));

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String answer = null;
			if (line != null) {
				JSONObject jsonOb = JSONObject.fromObject(line);
				String question = jsonOb.getString("question");
				JSONArray answers = jsonOb.getJSONArray("answers");
				JSONArray selectedAnswer = new JSONArray();
				JSONArray filteredAnswer = new JSONArray();
				
				for (Object answerObj : answers) {
					try {
						JSONObject answerJson = JSONObject.fromObject(answerObj);
						Double score = answerJson.getDouble("score");
						if (score >= minScore && score <= maxScore) {
							selectedAnswer.add(answerJson);
						}else{
							filteredAnswer.add(answerJson);
						}
					} catch (Exception JSONException) {
						answer = answerObj.toString().replaceAll("&", "");
					}
				}
				if (selectedAnswer.size() > 0) {
					jsonOb.put("question", question);
					jsonOb.put("answers", selectedAnswer);
					mos.write(new Text(jsonOb.toString()), NullWritable.get(), "in_" + folderSuffix);
				}
				if (filteredAnswer.size() > 0) {
					jsonOb.put("question", question);
					jsonOb.put("answers", filteredAnswer);
					mos.write(new Text(jsonOb.toString()), NullWritable.get(), "out_" + folderSuffix);
				}

			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
}
