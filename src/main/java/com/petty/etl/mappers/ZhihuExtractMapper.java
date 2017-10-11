package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.constant.Constants;
import com.petty.etl.extractor.ZhihuExtractor;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ZhihuExtractMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();

			ZhihuExtractor zh = new ZhihuExtractor();

			List<JSONObject> result = zh.extract(line);

			if (result != null) {
				for (int i = 0; i < result.size(); i++) {
					JSONObject jsonOb = result.get(i);
					if (jsonOb.get(Constants.COMMENTID) != null && !"".equalsIgnoreCase(jsonOb.getString(Constants.COMMENTID))) {
						mos.write(new Text(jsonOb.toString()), NullWritable.get(), "zhihu_comments/part");
					} else {
						mos.write(new Text(jsonOb.toString()), NullWritable.get(), "zhihu_origin/part");
					}
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
