package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.HanlpUtil;

public class SegKeywordEditorialMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] array = line.split("\t");
			if(array.length == 3){
				StringBuilder builder = new StringBuilder();
				builder.append(array[0]).append("\t")
					.append(array[1]).append("\t")
					.append(HanlpUtil.getWords(array[2])).append("\t")
					.append(HanlpUtil.getKeywords(array[2]));
				context.write(new Text(builder.toString()), new Text());
			}
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
		}
	}
}
