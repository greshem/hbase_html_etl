package com.petty.etl.mappers;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.filter.EtlFilter;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class GeneralFilterNewRuleMapper extends Mapper<LongWritable, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;
	private static HashSet<String> symbolSet;
	private EtlFilter ef;
	private Pattern pd = null;
	private String url = null;
	private boolean replaceFlag  = true;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
		ef = new EtlFilter();
		URI[] uriArray = context.getCacheFiles();
		url = context.getConfiguration().get(Constants.URL, "");
		if("".equalsIgnoreCase(url)){
			pd = ef.compilePattern(uriArray);
		}
		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			if (filename.contains("symbol.txt")) {
				symbolSet = FileUtil.readFile(filename);
			}
			if (filename.contains("regex.txt")) {
				replaceFlag = false;
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			if (line.length() <= 200000) {
				JSONObject obj = new JSONObject();
				obj = ef.filterNewRule(line, pd, url, symbolSet, replaceFlag);
				if (!obj.isEmpty()) {
					writeToDiffPart(obj, "valid");
					writeToDiffPart(obj, "invalid");
				}
				// 如果JSON中带有update属性，说明这条recored有被filter掉一些answer，需要在Solr中更新
				// 需要update的数据，就是valid的数据
				if(obj.containsKey("update")){
					mos.write(new Text(obj.getString("valid")), new Text(), "update/part");
				}else if(obj.containsKey("remove")){
					mos.write(new Text(obj.getString("invalid")), new Text(), "remove/part");
				} 
			}
		} catch (JSONException e) {
			System.out.println("Line: " + value.toString());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}

	public void writeToDiffPart(JSONObject obj, String type) throws IOException, InterruptedException {
		JSONObject jsonOb = obj.getJSONObject(type);
		JSONArray shortArray = jsonOb.getJSONArray(Constants.ANSWERS);
		if (shortArray.size() > 0) {
			mos.write(new Text(jsonOb.toString()), new Text(), type + "/part");
		}
	}
}
