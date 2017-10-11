package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ExtarctSportNewMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			line = SymbolUtil.TraToSim(line);
			JSONObject jsonOb = JSONObject.fromObject(line);
			String content = jsonOb.getString(Constants.CONTENT).replaceAll(" +", " ").trim();
			context.write(new Text(content), new Text());
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
