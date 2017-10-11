package com.petty.etl.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class GetPicUrlReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int num = 0;
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()){
			num += it.next().get();
		}
		context.write(key, new IntWritable(num));
	}
}
