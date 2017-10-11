package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.HttpUtils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TagCUInfoMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String url = null;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		url = context.getConfiguration().get("url", "");
	}

	// @Override
	// public void map(LongWritable key, Text value, Context context) throws
	// IOException, InterruptedException {
	// String line = value.toString();
	// JSONObject record = JSONObject.fromObject(line);
	// String question = record.getString("question");
	//
	// question = question.replaceAll("&", "");
	// List<String> qaArray = new ArrayList<String>();
	// qaArray.add(question);
	//
	// JSONArray answers = record.getJSONArray("answers");
	// JSONArray taggedAnswers = new JSONArray();
	// String answer = null;
	// for (Object answerObj : answers) {
	// try {
	// JSONObject answerJson = JSONObject.fromObject(answerObj);
	// if (answerJson.has("content")) {
	// answer = answerJson.getString("content").replaceAll("&", "");
	// } else {
	// answer = answerObj.toString().replaceAll("&", "");
	// }
	//
	// } catch (Exception JSONException) {
	// answer = answerObj.toString().replaceAll("&", "");
	// }
	// qaArray.add(answer);
	// }
	//
	// String batchResult = getBatchCUTagInfo(url, qaArray);
	// int i = 0;
	// JSONObject questionObject = new JSONObject();
	// try {
	// JSONArray batchArrayResult = JSONArray.fromObject(batchResult);
	//
	// for (Object result : batchArrayResult) {
	// JSONObject tagObject =
	// JSONObject.fromObject(result).getJSONObject("result");
	// if (i == 0) {
	// questionObject = new JSONObject();
	// questionObject.put("question", tagObject.get("Text1").toString());
	// questionObject.put("emotion", getCUItem(tagObject, "emotion"));
	// questionObject.put("speech_act", getCUItem(tagObject, "speech_act"));
	// questionObject.put("topic", getCUItem(tagObject, "topic"));
	// } else {
	// JSONObject answerObject = answers.getJSONObject(i - 1);
	// if (answerObject.has("content")) {
	// answerObject.put("content", tagObject.get("Text1").toString());
	// answerObject.put("emotion", getCUItem(tagObject, "emotion"));
	// answerObject.put("speech_act", getCUItem(tagObject, "speech_act"));
	// answerObject.put("topic", getCUItem(tagObject, "topic"));
	// taggedAnswers.add(answerObject);
	// }
	// }
	// i++;
	// }
	// } catch (Exception e) {
	// System.out.println(e.getStackTrace());
	// }
	//
	// record.put("answers", taggedAnswers);
	// record.put("question", questionObject);
	// context.write(new Text(record.toString()), new Text());
	//
	// }

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject record = JSONObject.fromObject(line);
		JSONObject questionObject = new JSONObject();
		String question = record.getString("question");

		question = question.replaceAll("&", "");
		JSONObject tagObject = getCUTagInfo(url, question);
		questionObject.put("question", question);
		questionObject.put("emotion", getCUItem(tagObject, "emotion"));
		questionObject.put("speech_act", getCUItem(tagObject, "speech_act"));
		questionObject.put("topic", getCUItem(tagObject, "topic"));

		JSONArray answers = record.getJSONArray("answers");
		JSONArray taggedAnswers = new JSONArray();
		String answer = null;
		JSONObject answerJson = new JSONObject();
		for (Object answerObj : answers) {
			try {
				answerJson = JSONObject.fromObject(answerObj);
				int select = answerJson.getInt("select");
				if (answerJson.has("content") && select == 0) {
					answer = answerJson.getString("content").replaceAll("&", "");
					JSONObject answerObject = getCUTagInfo(url, answer);
					answerJson.put("emotion", getCUItem(answerObject, "emotion"));
					answerJson.put("speech_act", getCUItem(answerObject, "speech_act"));
					answerJson.put("topic", getCUItem(answerObject, "topic"));
				} else {
					answer = answerObj.toString().replaceAll("&", "");
				}

			} catch (Exception JSONException) {
				answer = answerObj.toString().replaceAll("&", "");
			}
			taggedAnswers.add(answerJson);
		}

		record.put("answers", taggedAnswers);
		record.put("question", questionObject);
		context.write(new Text(record.toString()), new Text());

	}

	public static String getCUItem(JSONObject jsonObject, String itemType) {
		double maxItemScore = 0.0;
		String queryItem = "";
		try {
			JSONObject itemObject = jsonObject.getJSONObject(itemType);
			JSONArray results = itemObject.getJSONArray("res");

			for (Object res : results) {
				JSONObject resObject = JSONObject.fromObject(res);
				String item = resObject.getString("item");
				double score = resObject.getDouble("score");
				if (score > maxItemScore) {
					maxItemScore = score;
					queryItem = item;
				}
			}
		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}
		return queryItem;
	}

	public static JSONObject getCUTagInfo(String url, String sentence) {

		JSONObject tagInfo = new JSONObject();
		String result = HttpUtils.callService(url, sentence, "Text1");
		JSONObject jsonObject = JSONObject.fromObject(result);

		tagInfo.put("emotion", getCUItem(jsonObject, "emotion"));
		tagInfo.put("speech_act", getCUItem(jsonObject, "speech_act"));
		tagInfo.put("topic", getCUItem(jsonObject, "topic"));

		return tagInfo;
	}

	public static String getBatchCUTagInfo(String url, List<String> params) {

		JSONObject item = new JSONObject();
		JSONObject postData = new JSONObject();
		JSONArray items = new JSONArray();

		for (int i = 0; i < params.size(); i++) {
			item.put("uniqueId", Integer.toString(i));
			item.put("text", params.get(i));
			items.add(item);
		}
		postData.put("inputs", items);
		postData.put("userId", "mrchatlib");

		String result = HttpUtils.postService(url, postData.toString());
		return result;
	}

	public static void main(String[] args) {
		String url = "http://192.168.1.53:8081/cu?";
		String input = "我爱打篮球";
		// List<String> params = new ArrayList<String>();
		// params.add("我爱打篮球");
		// params.add("滚你丫的");
		// String result = getBatchCUTagInfo(url, params);

		JSONObject result = getCUTagInfo(url, input);
		System.out.println("Tag:\t" + result.toString());
		// }
	}

}
