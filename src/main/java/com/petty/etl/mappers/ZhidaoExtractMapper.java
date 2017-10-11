package com.petty.etl.mappers;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.extractor.ZhidaoExtrctor;

import net.sf.json.JSONObject;

public class ZhidaoExtractMapper extends TableMapper<Text, Text>{
	
	private HashMap<String, String> picMap;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		URI[] uriArray = context.getCacheFiles();
		for(int i=0; i<uriArray.length; i++){
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			if(filename.contains("zhidao_pic.map")){
				picMap = FileUtil.readFileToMap(filename, "\t");
			}
		} 
	}
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
					throws IOException, InterruptedException {
		String url = new String(value.getValue(Bytes.toBytes("url"), Bytes.toBytes("url")));
		String encode = new String(value.getValue(Bytes.toBytes("encode"), Bytes.toBytes("encode")));
		String html = "";
		if(encode == null || "".equalsIgnoreCase(encode)){
			html = new String(value.getValue(Bytes.toBytes("html"), Bytes.toBytes("html")), "gbk");
		}else{
			html = new String(value.getValue(Bytes.toBytes("html"), Bytes.toBytes("html"))); 
		}
		ZhidaoExtrctor extractor = new ZhidaoExtrctor();
		List<JSONObject> list = extractor.extractHbaseData(html, null, url, null, picMap);
		for(int i=0; i< list.size(); i++){
			JSONObject recordObject = list.get(i);
			context.write(new Text(recordObject.toString()), new Text());
		}
	}

}
