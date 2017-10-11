package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.extractor.TiebaExtractor;

import net.sf.json.JSONObject;


public class TiebaExtractMapper extends TableMapper<Text, NullWritable>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
		String url = new String(value.getValue(Bytes.toBytes("url"), Bytes.toBytes("url")));
		String html = new String(value.getValue(Bytes.toBytes("html"), Bytes.toBytes("html")));
		String updateTime = new String();
		TiebaExtractor tb = new TiebaExtractor();
		List<JSONObject> result = tb.extractHbaseData(html, updateTime, url, null, null);
		if(result != null){
			for(int i=0; i<result.size(); i++){
				JSONObject jsonOb = result.get(i);
				context.write(new Text(jsonOb.toString()), NullWritable.get());
			}
		}		
	}
}
