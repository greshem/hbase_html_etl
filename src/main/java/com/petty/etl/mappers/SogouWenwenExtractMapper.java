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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.extractor.SogouWenwenExtrctor;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


public class SogouWenwenExtractMapper extends TableMapper<Text, Text>{
	
	private MultipleOutputs<Text, Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		if(value != null){
			boolean utf8Flag = false;
			String html = getValue("html_body", "html", value, utf8Flag);
			String url = getValue("url", "url", value, utf8Flag);
			if(html.contains("content=\"text/html; charset=utf-8\"")
					|| html.contains("<meta charset=\"utf-8\"/>")){
			    utf8Flag = true;
			}
			html = getValue("html_body", "html", value, utf8Flag);
			SogouWenwenExtrctor extractor = new SogouWenwenExtrctor();
			List<JSONObject> list = extractor.extractHbaseData(html, null, url, null, null);
			for(int i=0; i< list.size(); i++){
				JSONObject object = list.get(i);
				if(object.has("related_question")){
					JSONArray relatedQArray = object.getJSONArray("related_question");
					for(int j=0; j<relatedQArray.size(); j++){
						mos.write(new Text(SymbolUtil.TraToSim(relatedQArray.getString(j))), new Text(), "questions/part");
					}
				}else{
					mos.write(new Text(SymbolUtil.TraToSim(object.toString())), new Text(), "extract/part");
				}
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
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
	
}
