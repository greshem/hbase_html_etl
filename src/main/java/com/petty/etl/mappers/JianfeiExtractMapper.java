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

import com.petty.etl.extractor.JianfeiExtractor;

import net.sf.json.JSONObject;


public class JianfeiExtractMapper extends TableMapper<Text, NullWritable>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
		byte[] u = value.getValue(Bytes.toBytes("url"), Bytes.toBytes("url"));
		byte[] h = value.getValue(Bytes.toBytes("html_body"), Bytes.toBytes("html"));
		if (u == null || h == null) {			
			u = value.getValue(Bytes.toBytes("url"), Bytes.toBytes(""));
			h = value.getValue(Bytes.toBytes("html_body"), Bytes.toBytes(""));
			if (u == null || h == null) {
				return;
			}
		}

		String url = new String(u);
		String html;
		if (url.startsWith("http://jianfei.39.net/fitask/topic-")) {
			html = new String(h,"gb2312");
		} else {
			html = new String(h);
		}

		String updateTime = new String();
		JianfeiExtractor tb = new JianfeiExtractor();
		List<JSONObject> result = tb.extractHbaseData(html, updateTime, url, null, null);
		if(result != null){
			for(int i=0; i<result.size(); i++){
				JSONObject jsonOb = result.get(i);
				context.write(new Text(jsonOb.toString()), NullWritable.get());
			}
		}
	}
}
