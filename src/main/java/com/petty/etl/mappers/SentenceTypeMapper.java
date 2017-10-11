package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.HttpUtils;

public class SentenceTypeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
//		SentenceClassifier sc = new SentenceClassifier("/home/hbase/etl/sentence_type/");
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] array = line.split("\t");
			if(array.length == 3){
				StringBuilder builder = new StringBuilder();
				builder.append(array[0]).append("\t")
					.append(array[1]).append("\t")
					//.append(SentenceClassifier.getSentenceType(array[1]));
				    .append(HttpUtils.callService("http://192.168.1.126:12001/ai/simpleChat?id=sentence_type", array[2], "input"));
				context.write(new Text(builder.toString()), new Text());
			}
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
		}
	}
	

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
}
