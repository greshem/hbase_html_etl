package com.petty.etl.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.HanlpUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class PersonRecognitionMapper extends Mapper<LongWritable, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			JSONObject lineOb = JSONObject.fromObject(line);
			JSONObject validResData = lineOb;
			JSONObject invalidData = lineOb;
			List<JSONObject> validList = new ArrayList<JSONObject>();
			List<JSONObject> invalidList = new ArrayList<JSONObject>();
//			String question = lineOb.getString(Constants.QUESTION);
//			if(question != null && !"".equalsIgnoreCase(question)){
//				lineOb.put(Constants.QUESTION_SEG, HanlpUtil.getWords(question));
//			}
			JSONArray answers = lineOb.getJSONArray(Constants.ANSWERS);
			for(int i=0; i<answers.size(); i++){
				JSONObject answer = answers.getJSONObject(i);
				String content = answer.getString(Constants.CONTENT);
				int pos = content.indexOf("查看详情");
				if(pos != -1){
					content = content.substring(0, pos);
				}
				content = HanlpUtil.getMain2ObjectPP(content);
				if(content != null && content.contains("1")){
					invalidList.add(answer);
				}else{
					validList.add(answer);
				}
			}
			if(validList.size() != 0){
				validResData.put(Constants.ANSWERS, validList.toArray());
				mos.write(new Text(validResData.toString()), new Text(), "valid/part");
			}
			if(invalidList.size() != 0){
				invalidData.put(Constants.ANSWERS, invalidList.toArray());
				mos.write(new Text(invalidData.toString()), new Text(), "invalid/part");
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

}
