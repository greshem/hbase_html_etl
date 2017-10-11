package com.petty.etl.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestSplitReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long total = 0;
		Iterator<Text> it = values.iterator();
		while(it.hasNext()){
			long random = Long.valueOf(it.next().toString());
			total += random;
		}
		context.write(key, new Text(String.valueOf(total)));
	}
}
