package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.HttpUtils;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class CheckNERMapper extends Mapper<LongWritable, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	private String url = null;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		url = context.getConfiguration().get(Constants.URL, "");
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		JSONObject record = JSONObject.fromObject(line);
		JSONObject containsNER = JSONObject.fromObject(line);
		JSONObject notContainsNER = JSONObject.fromObject(line);
		JSONArray answers = record.getJSONArray(Constants.ANSWERS);
		
		JSONArray needFilterAnswer = new JSONArray();
		JSONArray notNeedFilterAnswer = new JSONArray();
		for(int i=0; i<answers.size(); i++){
			String answer = answers.getString(i).trim();
			String tmpAnswer = "";
			// 微博或者新的知乎数据包含了“赞”的信息， Answer的格式是{"content":"xxxxx","likecount":"0"}
			// 如果JSON解析成功就是微博或知乎的数据，否则就是豆瓣，天涯的数据
			try{
				JSONObject answerOb = JSONObject.fromObject(answer);
				tmpAnswer = answerOb.getString(Constants.CONTENT);
				if(checkNERStr(url, tmpAnswer)){
					needFilterAnswer.add(answer);
				}else{
					notNeedFilterAnswer.add(answer);
				}
			}catch(JSONException e){
				System.out.println(e.getMessage());
			}
		}
		if(needFilterAnswer.size() > 0){
			containsNER.put(Constants.ANSWERS, needFilterAnswer);
			mos.write(new Text(containsNER.toString()), new Text(), "containNER/part");
		}

		if(notNeedFilterAnswer.size() > 0){
			notContainsNER.put(Constants.ANSWERS, notNeedFilterAnswer);
			mos.write(new Text(notContainsNER.toString()), new Text(), "notContainNER/part");
		}
		
	}

	public static boolean checkNERStr(String url, String text){
		boolean flag = false;
		String response = HttpUtils.callService(url, text, "t");
		try{
			JSONObject responseOb = JSONObject.fromObject(response);
	        if(responseOb.has("persons") && responseOb.getJSONArray("persons").size() > 0){
//	        	JSONArray persons = responseOb.getJSONArray("persons");
//	        	for(int i=0; i<persons.size(); i++){
//	        		String person = persons.getString(i);
//	        		if(person.endsWith("兄") || person.endsWith("弟") || person.endsWith("哥") || person.endsWith("老板")
//	    					|| person.endsWith("姐") || person.endsWith("妹") || person.endsWith("婆")
//	    					|| person.endsWith("叔") || person.endsWith("婶") || person.endsWith("嫂")
//	    					|| person.endsWith("孙子") || person.endsWith("爷") || person.endsWith("总")
//	    					|| person.endsWith("导") || person.endsWith("某") || person.endsWith("大")){
	        			flag = true;
//	    				break;
//	    			}
//	        	}
	        }
	        return flag;
		}catch(Exception e){
			System.out.println( "text: " + text);
			return false;
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
}
