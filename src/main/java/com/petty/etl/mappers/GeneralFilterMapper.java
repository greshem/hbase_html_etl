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
import net.sf.json.JSONObject;

public class GeneralFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;
	private static HashSet<String> symbolSet;
	private EtlFilter ef;
	private Pattern pd;
	private String url = null;
	private boolean replaceFlag  = true;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		URI[] uriArray = context.getCacheFiles();
		ef = new EtlFilter();
		url = context.getConfiguration().get(Constants.URL, "");
		if("".equalsIgnoreCase(url)){
			pd = ef.compilePattern(uriArray);
		}
		mos = new MultipleOutputs<Text, Text>(context);
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
			JSONObject obj = new JSONObject();
			obj = ef.filter(line, pd, url, symbolSet, replaceFlag);
			if (!obj.isEmpty()) {
				writeToDiffPart(obj, "valid");
				writeToDiffPart(obj, "invalid");
			}
		} catch (Exception e) {
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
