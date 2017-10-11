package com.petty.etl.mappers;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.HttpUtils;
import com.petty.etl.constant.Constants;
import com.mongodb.util.JSONParseException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TopicSampleMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private static String label = null;
	private MultipleOutputs<Text, NullWritable> mos;
	private HashSet<String> topics;
	private String url;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
		url = context.getConfiguration().get("url");
		URI[] uriArray = context.getCacheFiles();
		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			if (filename.contains("topic.txt")) {
				topics = FileUtil.readFile(filename);
				System.out.println(topics.toString());
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject lineObject = JSONObject.fromObject(line);
		String question = lineObject.getString(Constants.QUESTION);
		JSONArray answers = lineObject.getJSONArray(Constants.ANSWERS);
		JSONArray selectedAnswers = new JSONArray();
	    boolean filtered = false;

		// zhihu (score between 0.8-0.9) has partly selected answers
		JSONArray partSelectedAnswers = new JSONArray();

		filtered = filterTopic(url, question, topics);
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

			answerObject.put("select", filtered ? 1 : (0 + alreadySelect));
			selectedAnswers.add(answerObject);
			if (alreadySelect == 0) {
				partSelectedAnswers.add(answerObject);
			}
		}

		lineObject.put(Constants.ANSWERS, selectedAnswers);
		mos.write(new Text(lineObject.toString()), NullWritable.get(), "tag/part");

		if (filtered && partSelectedAnswers.size() > 0) {
			lineObject.put(Constants.ANSWERS, partSelectedAnswers);
			mos.write(new Text(lineObject.toString()), NullWritable.get(), "selected/part");
		}

	}

	public boolean filterTopic(String url, String sentence, HashSet<String> topics) {

		String result = HttpUtils.callService(url, sentence, "sent");
		JSONObject jsonObject = JSONObject.fromObject(result);
		JSONArray results = jsonObject.getJSONArray("res");
		boolean filtered = false;
		if (results.size() > 0) {
			try {
				label = results.getJSONObject(0).getJSONObject("topic").getString("label");
				filtered = topics.contains(label);
			} catch (Exception e) {
				System.out.println(e.getStackTrace());
			}
		}
		return filtered;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
	
}
