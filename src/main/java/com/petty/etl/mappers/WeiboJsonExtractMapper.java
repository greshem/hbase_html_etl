package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.extractor.WeiboJsonExtractor;

import net.sf.json.JSONObject;


public class WeiboJsonExtractMapper extends TableMapper<Text, Text>{
	
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		if(value != null){
			String url = getValue("url", "", value);
			String html = getValue("html_body", "", value);
			String updateTime = getValue("update_time", "", value);
			String domain = getValue("domain", "", value);
			WeiboJsonExtractor extractor = new WeiboJsonExtractor();
			List<JSONObject> list = extractor.extractHbaseData(html, updateTime, url, domain, null);
			for(int i=0; i< list.size(); i++){
				String simple = SymbolUtil.TraToSim(list.get(i).toString());
				context.write(new Text(simple), new Text());
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
