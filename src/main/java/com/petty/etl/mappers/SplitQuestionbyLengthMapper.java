package com.petty.etl.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class SplitQuestionbyLengthMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos;
	private Integer length = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
		length = context.getConfiguration().getInt("length", 0);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			// 繁体转成简体
			line = SymbolUtil.TraToSim(line);
			String answer = null;
			if (line != null) {
				JSONObject jsonOb = JSONObject.fromObject(line);
				String question = jsonOb.getString(Constants.QUESTION);
				if (question.length() >= 5 && question.length() <= length) {
					JSONArray answers = jsonOb.getJSONArray(Constants.ANSWERS);
					JSONArray shortAnswer = new JSONArray();

					for (Object answerObj : answers) {
						// 微博或者新的知乎数据包含了“赞”的信息， Answer的格式是{"content":"xxxxx","likecount":"0"}
						// 如果JSON解析成功就是微博或知乎的数据，否则就是豆瓣，天涯的数据
						if(answerObj instanceof JSONObject){
							JSONObject answerJson = JSONObject.fromObject(answerObj);
							if (answerJson.has(Constants.CONTENT)) {
								answer = answerJson.getString(Constants.CONTENT).replaceAll("&", "");
								if (answer!= null && answer.length() >= 5 && answer.length() <= length) {
									//  设置answer的select flag为0
									answerJson.put(Constants.SELECT, 0);
									shortAnswer.add(answerJson.toString());
								}
							}
						}else{
							answer = answerObj.toString().replaceAll("&", "");
							if (answer!= null && answer.length() >= 5 && answer.length() <= length) {
								//  设置answer的select flag为0
								JSONObject answerOb = new JSONObject();
								answerOb.put(Constants.CONTENT, answer);
								answerOb.put(Constants.SELECT, 0);
								shortAnswer.add(answerOb.toString());
							}
						}
					}
					if (shortAnswer.size() > 0) {
						jsonOb.put(Constants.QUESTION, question);
						jsonOb.put(Constants.ANSWERS, shortAnswer);
						
						// 增加一个新的属性： latest作为后续RefreshSelectedFlag的标签
						jsonOb.put(Constants.LATEST, 1);
						
						mos.write(new Text(jsonOb.toString()), NullWritable.get(), "qa_5_to_30/part");
					}
				} else {
					mos.write(new Text(jsonOb.toString()), NullWritable.get(), "qa_n_5_to_30/part");
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
}
