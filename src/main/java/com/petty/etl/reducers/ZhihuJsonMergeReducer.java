package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ZhihuJsonMergeReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		JSONArray mergeAnswer = new JSONArray();
		String mergeQuestion = "";
		String mergeTitle = "";
		String mergeSource = "";
		String mergeUrl = "";
		String mergeUUID = "";
		String mergeCommentId = "";
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
		HashMap<String, Integer> answerSelectMap = new HashMap<String, Integer>();
		Set<String> descriptionSet = new HashSet<String>();
		Set<String> tagSet = new HashSet<String>();
		int i = 0;
		Boolean increFlag = false;
		while (it.hasNext()) {
			String jsonOb = it.next().toString();
			JSONObject object = JSONObject.fromObject(jsonOb);
			JSONArray tagArray = new JSONArray();
			if (object.has(Constants.TAGS)) {
				tagArray = object.getJSONArray(Constants.TAGS);
				if (tagArray != null && tagArray.size() > 0) {
					for (int j = 0; j < tagArray.size(); j++) {
						tagSet.add(tagArray.getString(j));
					}
				}
			}
			if (!object.containsKey(Constants.INCREFLAG)) {
				object.put(Constants.INCREFLAG, 0);
			} else {
				if (object.getInt(Constants.INCREFLAG) == 1) {
					increFlag = true;
				}
			}
			/*
			 * 检查是否带有UUID的纪录，如果有，则表示history数据中已经含有过这条纪录，继续作为UUID；
			 * 如果没有，则表示是全新的数据， 需要创建新的
			 */
			if (object.has(Constants.UUID)) {
				mergeUUID = object.getString(Constants.UUID);
			}
			
			String description = object.getString(Constants.DESCRIPTION);
			if (description != null && !"".equalsIgnoreCase(description)) {
				try {
					JSONArray descriptionArray = JSONArray.fromObject(description);
					for (int k = 0; k < descriptionArray.size(); k++) {
						descriptionSet.add(String.valueOf(descriptionArray.get(k)));
					}
				} catch (JSONException e) {
					descriptionSet.add(description);
				}
			}
			
			String question = object.getString(Constants.QUESTION);
			String title = object.getString(Constants.TITLE);
			if (question != null && !"".equalsIgnoreCase(question)) {
				if (i == 0) {
					mergeQuestion = question;
					mergeTitle = title;
					mergeSource = object.getString(Constants.SOURCE);
					mergeUrl = object.getString(Constants.URL);
					if (object.has(Constants.COMMENTID)) {
						mergeCommentId = object.getString(Constants.COMMENTID);
					}
				}
				JSONArray answers = object.getJSONArray(Constants.ANSWERS);
				for (int j = 0; j < answers.size(); j++) {
					JSONObject answerObject = answers.getJSONObject(j);
					String answerContent = answerObject.getString(Constants.CONTENT).trim();
					int likeCount =0;
					if (answerObject.has("likecount")){
						likeCount = answerObject.getInt("likecount");
					}
					int select =0;
					if (answerObject.has("select")){
						select = answerObject.getInt("select");
					}
					
					if (answerMap.containsKey(answerContent)) {
						int sumLike = answerMap.get(answerContent) + likeCount;
						answerMap.put(answerContent, sumLike);
						int selectSum = answerSelectMap.get(answerContent) + select;
						answerSelectMap.put(answerContent, selectSum);
					} else {
						answerMap.put(answerContent, likeCount);
						answerSelectMap.put(answerContent, select);
					}
				}
				i++;
			} else if (question != null && "".equalsIgnoreCase(question)) {
				context.write(new Text(object.toString()), new Text());
			}
		}

		Set<String> answerKeys = answerMap.keySet();
		for (String answerKey : answerKeys) {
			JSONObject answer = new JSONObject();
			answer.put(Constants.CONTENT, answerKey);
			answer.put("likecount", answerMap.get(answerKey));
			answer.put("select", answerSelectMap.get(answerKey));
			mergeAnswer.add(answer);
		}

		if (!"".equalsIgnoreCase(mergeQuestion)) {
			JSONObject finalObject = new JSONObject();
			finalObject.put(Constants.TITLE, mergeTitle);
			finalObject.put(Constants.QUESTION, mergeQuestion);
			finalObject.put(Constants.ANSWERS, mergeAnswer);
			finalObject.put(Constants.DESCRIPTION, descriptionSet.toArray());
			finalObject.put(Constants.TAGS, tagSet.toArray());
			finalObject.put(Constants.URL, mergeUrl);
			finalObject.put(Constants.SOURCE, mergeSource);
			if (!"".equalsIgnoreCase(mergeCommentId)) {
				finalObject.put(Constants.COMMENTID, mergeCommentId);
			}
			if (increFlag) {
				finalObject.put(Constants.INCREFLAG, 1);
			} else {
				finalObject.put(Constants.INCREFLAG, 0);
			}
			if ("".equalsIgnoreCase(mergeUUID)) {
				mergeUUID = UUID.randomUUID().toString();
			}
			finalObject.put(Constants.UUID, mergeUUID);
			context.write(new Text(finalObject.toString()), new Text());
		}
	}
}
