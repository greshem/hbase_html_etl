package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class TiebaMergeQuestionReducer extends Reducer<Text, Text, Text, Text> {

	// private MultipleOutputs<Text,Text> mos;
	//
	// @Override
	// protected void setup(Context context) throws
	// IOException,InterruptedException {
	// super.setup(context);
	// mos = new MultipleOutputs<Text,Text>(context);
	// }

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		String updateTime = new String();
		String title = new String();
		String question = new String();
		HashSet<String> tagSet = new HashSet<String>();
		HashSet<String> answerSet = new HashSet<String>();
		HashSet<String> descriptionSet = new HashSet<String>();
		HashSet<String> urlSet = new HashSet<String>();

		while (it.hasNext()) {
			try {
				String jsonStr = it.next().toString();
				JSONObject jsonOb = JSONObject.fromObject(jsonStr);
				title = jsonOb.getString("title");

				question = jsonOb.getString("question");
				if (question == null || "".equalsIgnoreCase(question)) {
					continue;
				}

				String tags = jsonOb.getString("tags");
				if (tags != null && !"".equalsIgnoreCase(tags) && tags.startsWith("[") && tags.endsWith("]")) {
					JSONArray tagArray = JSONArray.fromObject(tags);
					for (int i = 0; i < tagArray.size(); i++) {
						tagSet.add(String.valueOf(tagArray.get(i)));
					}
				}

				String answers = jsonOb.getString("answers");
				if (answers != null && !"".equalsIgnoreCase(answers) && answers.startsWith("[")
						&& answers.endsWith("]")) {
					JSONArray answerArray = JSONArray.fromObject(answers);
					for (int i = 0; i < answerArray.size(); i++) {
						answerSet.add(String.valueOf(answerArray.get(i)));
					}
				}

				String description = jsonOb.getString("description");
				descriptionSet.add(description);

				String url = jsonOb.getString("url");
				urlSet.add(url);

				updateTime = jsonOb.getString("update_time");
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		if (title != null && !"".equalsIgnoreCase(title)) {
			try {
				JSONObject jsonOb = new JSONObject();
				jsonOb.put("title", title);
				jsonOb.put("question", question);

				JSONArray answerArray = new JSONArray();
				for (String answer : answerSet) {
					answerArray.add(answer);
				}
				jsonOb.put("answers", answerArray.toString());

				JSONArray descriptionArray = new JSONArray();
				for (String description : descriptionSet) {
					descriptionArray.add(description);
				}
				jsonOb.put("description", descriptionArray.toString());

				JSONArray tagArray = new JSONArray();
				for (String tag : tagSet) {
					tagArray.add(tag);
				}
				jsonOb.put("tags", tagArray.toString());

				JSONArray urlArray = new JSONArray();
				for (String url : urlSet) {
					urlArray.add(url);
				}
				jsonOb.put("url", urlArray.toString());
				jsonOb.put("source", "106");
				jsonOb.put("update_time", updateTime);
				context.write(new Text(jsonOb.toString()), new Text());
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	// @Override
	// protected void cleanup(Context context) throws
	// IOException,InterruptedException {
	// super.setup(context);
	// mos.close();
	// }
}
