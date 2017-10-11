package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class DoubanMergePageMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			JSONObject lineObject = JSONObject.fromObject(line);
			JSONArray urlArray = lineObject.getJSONArray(Constants.URL);
			String url = "";
			for(int i=0; i<urlArray.size(); i++){
				String tmpUrl = urlArray.getString(i);
				if(tmpUrl.startsWith("http://www.douban.com/group/topic/")){
					url = tmpUrl;
					break;
				}
			}
			String question = lineObject.getString(Constants.QUESTION);
			JSONArray answerArray = lineObject.getJSONArray(Constants.ANSWERS);
			if(answerArray !=null && answerArray.size() > 0){
				if(url != null && url.startsWith("http://www.douban.com/group/topic/")){
					// https://www.douban.com/group/topic/51523417/?start=0
					if("".equalsIgnoreCase(question)){
						String postId = getPostId(url);
						if("".equalsIgnoreCase(postId)){
							context.write(value, new Text());
						}else{
							context.write(new Text(postId), value);
						}
					}else{
						context.write(value, new Text());
					}
				}else{
					context.write(value, new Text());
				}
			}
					
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
	
	private static String getPostId(String url){
		String postId = "";
		String[] array = url.split("/");
		for(int i=0; i<array.length; i++){
			String tmp = array[i];
			if("topic".equalsIgnoreCase(tmp) && i+1<=array.length -1){
				postId = array[i+1];
				break;
			}
		}
		return postId;
	}
}
