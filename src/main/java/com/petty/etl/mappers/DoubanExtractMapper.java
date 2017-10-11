package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.extractor.EtlExtractor;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class DoubanExtractMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			List<JSONObject> result = EtlExtractor.extract(line);
			if(result != null){
				for(int i=0; i<result.size(); i++){
					JSONObject jsonOb = result.get(i);
					context.write(new Text(jsonOb.toString()), new Text());
				}
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
