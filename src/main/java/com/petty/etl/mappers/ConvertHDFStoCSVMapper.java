package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class ConvertHDFStoCSVMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject record = JSONObject.fromObject(line);
//		JSONObject answer = new JSONObject();
		String question = record.getString("question");
		JSONArray answers = record.getJSONArray("answers");
		String source = record.getString("source");
		String csv = null;
		String answer = null;
		float score = 0.0f;
		JSONObject scoredAnswer = new JSONObject();
		for (Object answerObj : answers) {
			try {
				scoredAnswer = JSONObject.fromObject(answerObj);
				if (scoredAnswer.has("answer")) {
					answer = scoredAnswer.getString("answer").replaceAll("&\t", "");
				}
				if (scoredAnswer.has("score")) {
					score = (float) scoredAnswer.getDouble("score");
				}

			} catch (Exception JSONException) {
				answer = answerObj.toString().replaceAll("&", "");
			}
			csv = String.format("%s\t%s\t%.6f\t%s", question, answer, score, source);
			context.write(new Text(csv), NullWritable.get());
		}
	}

}
