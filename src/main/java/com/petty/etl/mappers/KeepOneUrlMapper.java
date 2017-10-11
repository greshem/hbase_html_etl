package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class KeepOneUrlMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			// 繁体转成简体
			line = SymbolUtil.TraToSim(line);
			context.write(new Text(line), new Text());
//			if (line != null) {
//				JSONObject jsonOb = JSONObject.fromObject(line);
//				JSONArray urlArray = jsonOb.getJSONArray(Constants.URL);
//				if(urlArray.size() > 0){
//					jsonOb.put(Constants.URL, urlArray.get(0));
//				}
//				context.write(new Text(jsonOb.toString()), new Text());
//			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
