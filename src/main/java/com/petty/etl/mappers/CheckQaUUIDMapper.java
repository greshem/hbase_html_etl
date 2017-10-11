package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class CheckQaUUIDMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();

			if (line != null) {
				JSONObject jsonObj = JSONObject.fromObject(line);

				String qId = jsonObj.getString(Constants.UUID);

				JSONArray answers = jsonObj.getJSONArray(Constants.ANSWERS);
				JSONArray refreshAns = new JSONArray();
				for (Object answer : answers) {
					JSONObject ansObj = JSONObject.fromObject(answer);
					String aId = ansObj.getString(Constants.ANSWER_UUID);
					if (!aId.startsWith(qId)) {
						ansObj.put(Constants.ANSWER_UUID, qId + "_" + aId.split("_")[1]);
					}
					refreshAns.add(ansObj);
				}
				jsonObj.put(Constants.ANSWERS, refreshAns);
				context.write(new Text(jsonObj.toString()), new Text());
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
