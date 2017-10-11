package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class GetIncrementalMapper extends Mapper<LongWritable, Text, Text, Text> {

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
			JSONObject lineObject = JSONObject.fromObject(line);
			int increFlagValue = lineObject.getInt(Constants.INCREFLAG);
			if(increFlagValue == 1){
				String question = lineObject.getString(Constants.QUESTION);
				JSONArray answerArray = lineObject.getJSONArray(Constants.ANSWERS);
				// 只需要把既有Q又有A的留下来
				if(question != null && !"".equalsIgnoreCase(question)
						&& answerArray != null && answerArray.size() > 0){
					mos.write(value, new Text(), "incremental/part");
				}
				
				// set IncreFlag = 0, and write to history folder
				lineObject.put(Constants.INCREFLAG, 0);
				mos.write(new Text(lineObject.toString()), new Text(), "history/part");
			}else{
				mos.write(value, new Text(), "history/part");
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
