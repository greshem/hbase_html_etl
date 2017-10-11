package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.extractor.ZhidaoUrlExtrctor;


public class GetPicUrlMapper extends TableMapper<Text, IntWritable>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
		byte[] htmlDoc = value.getValue(Bytes.toBytes("html_body"), Bytes.toBytes(""));
		if(htmlDoc != null){
			String htmlStr = new String(htmlDoc,"UTF-8");
			List<String> urlList = ZhidaoUrlExtrctor.extractHbaseData(htmlStr);
			for(int i=0; i<urlList.size(); i++){
				String url =  urlList.get(i);
//				System.out.println("url: " + url);
			    context.write(new Text(url), new IntWritable(1));
			}
			
		}
	}
}
