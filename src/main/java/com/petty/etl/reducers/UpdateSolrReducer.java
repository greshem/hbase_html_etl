package com.petty.etl.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class UpdateSolrReducer extends Reducer<Text, Text, Text, Text> {
	
	private MultipleOutputs<Text, Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> list = new ArrayList<String>();
		Iterator<Text> it = values.iterator();
		while(it.hasNext()){
			String item = it.next().toString();
			list.add(item);
		}
		
		if(list.contains(key.toString())){
			if(list.size() == 1){
				mos.write(key, new Text(), "delete/part");
			}else{
				for(int i=0; i<list.size(); i++){
					String stringOb = list.get(i);
					if(!key.toString().equalsIgnoreCase(stringOb)){
						mos.write(new Text(stringOb), new Text(), "update/part");
					}
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
}
