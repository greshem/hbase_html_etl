package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ZhihuMergeCommentsReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();

		String title = new String();
		String titleTemp = new String();
		String question = new String();
		String questionTemp = new String();
		String updateTime = new String();
		String commentID = new String();
		String url = new String();
		String uuid = new String();
		HashSet<String> tagSet = new HashSet<String>();
		JSONArray answerArray = new JSONArray();
		HashSet<String> descriptionSet = new HashSet<String>();
		boolean containIncreFlag = false;
		// HashSet<String> urlSet = new HashSet<String>();

		while (it.hasNext()) {
			try {
				String jsonStr = it.next().toString();
				JSONObject jsonOb = JSONObject.fromObject(jsonStr);
				titleTemp = jsonOb.getString(Constants.TITLE);
				if (titleTemp != null && !"".equalsIgnoreCase(titleTemp)) {
					title = titleTemp;
				}

				questionTemp = jsonOb.getString(Constants.QUESTION);
				if (questionTemp != null && !"".equalsIgnoreCase(questionTemp)) {
					question = questionTemp;
				}

				String tags = jsonOb.getString(Constants.TAGS);
				if (tags != null && !"".equalsIgnoreCase(tags) && tags.startsWith("[") && tags.endsWith("]")) {
					JSONArray tagArray = JSONArray.fromObject(tags);
					for (int i = 0; i < tagArray.size(); i++) {
						tagSet.add(String.valueOf(tagArray.get(i)));
					}
				}

				JSONArray answers = jsonOb.getJSONArray(Constants.ANSWERS);
				if (answers != null && answers.size() > 0) {
					answerArray.addAll(answers);
				}
				
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

				url = jsonOb.getString(Constants.URL);
				// urlSet.add(url);
				int increFlagValue = 0;
				if(!jsonOb.containsKey(Constants.INCREFLAG)){
					jsonOb.put(Constants.INCREFLAG, 0);
				}else{
					increFlagValue = jsonOb.getInt(Constants.INCREFLAG);
				}
				
				if(increFlagValue == 1){
					containIncreFlag = true;
				}
				
				if(jsonOb.has(Constants.UUID)){
					uuid = jsonOb.getString(Constants.UUID);
				}else{
					uuid = UUID.randomUUID().toString();
				}

				updateTime = jsonOb.getString(Constants.UPDATETIME);
				commentID = jsonOb.getString(Constants.COMMENTID);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		try {
			JSONObject jsonOb = new JSONObject();
			jsonOb.put(Constants.TITLE, title);
			jsonOb.put(Constants.QUESTION, question);

			jsonOb.put(Constants.ANSWERS, answerArray.toString());

			jsonOb.put(Constants.DESCRIPTION, descriptionSet.toArray());

			JSONArray tagArray = new JSONArray();
			for (String tag : tagSet) {
				tagArray.add(tag);
			}
			jsonOb.put(Constants.TAGS, tagArray.toString());
			
			if(containIncreFlag){
				jsonOb.put(Constants.INCREFLAG, 1);
			}else{
				jsonOb.put(Constants.INCREFLAG, 0);
			}
			jsonOb.put(Constants.UUID, uuid);

			// JSONArray urlArray = new JSONArray();
			// for (url : urlSet) {
			// urlArray.add(url);
			// }
			jsonOb.put(Constants.URL, url);
			jsonOb.put(Constants.SOURCE, "103");
			jsonOb.put(Constants.UPDATETIME, updateTime);
			jsonOb.put(Constants.COMMENTID, commentID);
			context.write(new Text(jsonOb.toString()), new Text());
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
