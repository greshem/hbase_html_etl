package com.petty.etl.mappers;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.filter.EtlFilter;

public class ExtractTopicMapper extends Mapper<LongWritable, Text, Text, Text> {

	private EtlFilter ef;
	private Pattern pattern;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		URI[] uriArray = context.getCacheFiles();
		ef = new EtlFilter();
		pattern = ef.compileTopicPattern(uriArray);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			ArrayList<String> qaList = new ArrayList<String>();
			qaList = ef.getTopic(line, pattern);
			for(int i=0; i<qaList.size(); i++){
				context.write(new Text(qaList.get(i)), new Text());
			}
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
			// e.printStackTrace();
		}
	}

}
