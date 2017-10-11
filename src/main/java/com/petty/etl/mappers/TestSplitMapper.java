package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestSplitMapper extends TableMapper<Text, Text>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		if(value != null){
			String string = getValue("colfam1", "col1", value);
			String[] timeArray = string.split("\\|");
			if(timeArray.length > 1){
				String time = timeArray[0];
				String random = timeArray[1];
				context.write(new Text(time), new Text(random));
			}
		}
	}
	
	public static String getValue(String columnFamily, String qualifier, Result cellValue){
		String value = null;
		byte[] byteAarry = cellValue.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		if(byteAarry != null){
			value = new String(byteAarry);
		}
		return value;
	}
	
}
