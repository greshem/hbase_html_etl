package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.extractor.TianyaBbsExtractor;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class TianyaBBSAfterFilterMapper extends Mapper<Text, NullWritable, Text, NullWritable>{
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
	
	@Override	
	public void map(Text key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = key.toString();
			if(line != null){
				TianyaBbsExtractor te = new TianyaBbsExtractor();
				JSONObject jObj = te.clearInvalidQuestionContent(line);
				if (jObj == null) {
					return;
				}
				
				String question = jObj.getString("question");
				if(question != null && !"".equalsIgnoreCase(question)){
					context.write(new Text(jObj.toString()), NullWritable.get());
				}
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
}
