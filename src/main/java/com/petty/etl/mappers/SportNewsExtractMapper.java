package com.petty.etl.mappers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.extractor.SportNewsExtrctor;

import net.sf.json.JSONObject;


public class SportNewsExtractMapper extends TableMapper<Text, Text>{
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		if(value != null){
			boolean utf8Flag = false;
			String html = getValue("html_body", "html", value, utf8Flag);
			String url = getValue("url", "url", value, utf8Flag);
			if((url.contains("sina") || url.contains("sohu")) && html.contains("content=\"text/html; charset=utf-8\"")){
			    utf8Flag = true;
			}else if(url.contains("163") && html.contains("<meta charset=\"utf-8\"/>")){
			    utf8Flag = true;
			}else if(url.contains("voice.hupu") && (html.contains("content=\"text/html; charset=utf-8\"") || html.contains("<meta charset=\"utf-8\"/>"))){
				utf8Flag = true;
			}else if(url.contains("afp.hupu")){
			     utf8Flag = true;
			}
			html = getValue("html_body", "html", value, utf8Flag);
//			String updateTime = getValue("update_time", "time", value);
//			String tag = getValue("tag", "tag", value);
			SportNewsExtrctor extractor = new SportNewsExtrctor();
			List<JSONObject> list = extractor.extractHbaseData(html, null, url, null, null);
			for(int i=0; i< list.size(); i++){
				context.write(new Text(list.get(i).toString()), new Text());
			}
		}
	}
	
	public static String getValue(String columnFamily, String qualifier, Result cellValue, boolean utf8Flag) throws UnsupportedEncodingException{
		
		String value = "";
		if(utf8Flag){
			value = new String(cellValue.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier)));
		}else{
			value = new String(cellValue.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier)), "gbk");
		}
		return value;
	}
	
}
