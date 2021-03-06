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

import com.petty.etl.extractor.DoubanExtractor;

import net.sf.json.JSONObject;


public class DoubanHbaseExtractMapper extends TableMapper<Text, NullWritable>{
	
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
		String url = new String(value.getValue(Bytes.toBytes("url"), Bytes.toBytes("url")));
		String html = new String(value.getValue(Bytes.toBytes("html_body"), Bytes.toBytes("html")));
		String tag = new String(value.getValue(Bytes.toBytes("tag"), Bytes.toBytes("tag")));
		String updateTime = new String(value.getValue(Bytes.toBytes("update_time"), Bytes.toBytes("time")));
		DoubanExtractor extractor = new DoubanExtractor();
		List<JSONObject> list = extractor.extractHbaseData(html, updateTime, url, tag, null);
		for(int i=0; i< list.size(); i++){
			context.write(new Text(list.get(i).toString()), NullWritable.get());
		}
	}
	
}
