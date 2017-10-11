package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.extractor.ZhihuExtractor;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ZhihuExtractCommentsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			ZhihuExtractor zh = new ZhihuExtractor();
			if (line.length() <= 40000) {
				List<JSONObject> result = zh.extractComments(line);

				if (result != null) {
					for (int i = 0; i < result.size(); i++) {
						JSONObject jsonOb = result.get(i);
						String simple = SymbolUtil.TraToSim(jsonOb.toString());
						context.write(new Text(simple), new Text());
					}
				}
			}

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
