package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class GeneralMergeQuestionReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		String updateTime = new String();
		String title = new String();
		String question = new String();
		String source = new String();
		HashSet<String> tagSet = new HashSet<String>();
		HashSet<String> answerSet = new HashSet<String>();
		HashSet<JSONObject> answerJsonSet = new HashSet<JSONObject>();
		HashSet<String> descriptionSet = new HashSet<String>();
		String urlStr = new String();

		while (it.hasNext()) {
			String jsonStr = it.next().toString();
			JSONObject jsonOb = JSONObject.fromObject(jsonStr);
			title = jsonOb.getString(Constants.TITLE);

			question = jsonOb.getString(Constants.QUESTION);
			if (question == null || "".equalsIgnoreCase(question)) {
				continue;
			}

			String tags = jsonOb.getString(Constants.TAGS);
			if (tags != null && !"".equalsIgnoreCase(tags) && tags.startsWith("[") && tags.endsWith("]")) {
				JSONArray tagArray = JSONArray.fromObject(tags);
				for (int i = 0; i < tagArray.size(); i++) {
					tagSet.add(String.valueOf(tagArray.get(i)));
				}
			}
			try {
				JSONArray answersJson = jsonOb.getJSONArray(Constants.ANSWERS);
				if (answersJson != null && answersJson.size() > 0) {
					answerJsonSet.addAll(answersJson);
				}
			} catch (Exception e) {
				String answers = jsonOb.getString(Constants.ANSWERS); 
				if (answers != null && !"".equalsIgnoreCase(answers) && answers.startsWith("[")
						&& answers.endsWith("]")) {
					JSONArray answerArray = JSONArray.fromObject(answers);
					for (int i = 0; i < answerArray.size(); i++) {
						answerSet.add(String.valueOf(answerArray.get(i)));
					}
				}
			}
			// String answers = jsonOb.getString(Constants.ANSWERS);
			// if(answers != null && !"".equalsIgnoreCase(answers) &&
			// answers.startsWith("[") && answers.endsWith("]")){
			// JSONArray answerArray = JSONArray.fromObject(answers);
			// for(int i=0; i<answerArray.size(); i++){
			// answerSet.add(String.valueOf(answerArray.get(i)));
			// }
			// }

			String description = jsonOb.getString(Constants.DESCRIPTION);
			if (description != null && !"".equalsIgnoreCase(description)) {
				try {
					JSONArray descriptionArray = JSONArray.fromObject(description);
					for (int i = 0; i < descriptionArray.size(); i++) {
						descriptionSet.add(String.valueOf(descriptionArray.get(i)));
					}
				} catch (JSONException e) {
					descriptionSet.add(description);
				}
			}

			urlStr = jsonOb.getString(Constants.URL);
			source = jsonOb.getString(Constants.SOURCE);
			updateTime = jsonOb.getString(Constants.UPDATETIME);
		}

		JSONObject jsonOb = new JSONObject();
		jsonOb.put(Constants.TITLE, title);
		jsonOb.put(Constants.QUESTION, question);

		if (answerJsonSet.size() > 0) {
			jsonOb.put(Constants.ANSWERS, answerJsonSet.toString());
		} else {
			JSONArray answerArray = new JSONArray();
			for (String answer : answerSet) {
				answerArray.add(answer);
			}
			jsonOb.put(Constants.ANSWERS, answerArray.toString());
		}
		JSONArray descriptionArray = new JSONArray();
		for (String description : descriptionSet) {
			descriptionArray.add(description);
		}
		jsonOb.put(Constants.DESCRIPTION, descriptionArray.toString());

		JSONArray tagArray = new JSONArray();
		for (String tag : tagSet) {
			tagArray.add(tag);
		}
		jsonOb.put(Constants.TAGS, tagArray.toString());

		jsonOb.put(Constants.URL, urlStr);
		if (source == null || "".equalsIgnoreCase(source)) {
			source = "000";
		}
		jsonOb.put(Constants.SOURCE, source);
		jsonOb.put(Constants.UPDATETIME, updateTime);
		context.write(new Text(jsonOb.toString()), new Text());
	}
}
