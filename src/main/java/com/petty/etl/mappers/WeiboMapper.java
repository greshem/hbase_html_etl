package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.parser.WeiboParser;

import net.sf.json.JSONObject;


public class WeiboMapper extends TableMapper<Text, NullWritable> {
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
		String url = new String(value.getValue(Bytes.toBytes("url"), Bytes.toBytes("url")));
		String html = new String(value.getValue(Bytes.toBytes("html"), Bytes.toBytes("html")));
		
		JSONObject result = WeiboParser.parse(html);
		if(result != null){
			result.put("url", url);
			context.write(new Text(result.toString()), NullWritable.get());
		}
	}
}
