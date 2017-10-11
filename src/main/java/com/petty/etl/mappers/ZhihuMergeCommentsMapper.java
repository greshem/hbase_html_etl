package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ZhihuMergeCommentsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			if (line != null) {
				JSONObject jsonOb = new JSONObject(line);
				String commentID = jsonOb.getString("comment_id");
				if (commentID != null && !"".equalsIgnoreCase(commentID)) {
					context.write(new Text(commentID), new Text(jsonOb.toString()));
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
