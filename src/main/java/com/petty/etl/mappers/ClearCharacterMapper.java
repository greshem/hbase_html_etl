package com.petty.etl.mappers;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.RemoveAnswerUtil;
import com.petty.etl.commonUtils.RemoveForeignTextUtil;
import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.filter.EtlFilter;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class ClearCharacterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private static HashSet<String> deDupSet;
	private static HashSet<String> removeSet;
	private EtlFilter ef;
	private static Pattern pattern, pd, pr;
	private MultipleOutputs<Text, NullWritable> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, NullWritable>(context);
		URI[] uriArray = context.getCacheFiles();
		ef = new EtlFilter();
		pd = ef.compileDeleteRegexPattern(uriArray);
		pr = ef.compileReplaceRegexPattern(uriArray);
		for (int i = 0; i < uriArray.length; i++) {
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			if (filename.contains("symbol.txt")) {
				deDupSet = FileUtil.readFile(filename);
				pattern = RemoveAnswerUtil.constructPattern(filename);
				// System.out.println("pattern: " + pattern);
			}
			if (filename.contains("symbol_filter.txt")) {
				removeSet = FileUtil.readFile(filename);
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			if (line != null) {
				JSONObject jsonOb = JSONObject.fromObject(line);

				String question = jsonOb.getString(Constants.QUESTION);
				if (question.length() <= 80) {
					if (!ef.isFilterNeeded(pd, question)) {
						question = afterClean(question);
						question = pr.matcher(question).replaceAll("");
						if (!"".equalsIgnoreCase(question)) {

							String title = jsonOb.getString(Constants.TITLE);
							title = afterClean(title);

							JSONArray answersArray = jsonOb.getJSONArray(Constants.ANSWERS);
							JSONArray afterCleanArray = new JSONArray();
							for (int i = 0; i < answersArray.size(); i++) {
								String answer = answersArray.getString(i);
								try{ // 如果是微博数据， 需要把content拿出来
									JSONObject answerOb = JSONObject.fromObject(answer);
									String tmpAnswer = answerOb.getString(Constants.CONTENT);
									tmpAnswer = afterClean(tmpAnswer);
									tmpAnswer = pr.matcher(tmpAnswer).replaceAll("");
									if (!"".equalsIgnoreCase(tmpAnswer)) {
										answerOb.put(Constants.CONTENT, tmpAnswer);
										afterCleanArray.add(answerOb.toString());
									}
								}catch(JSONException e){
									answer = afterClean(answer);
									answer = pr.matcher(answer).replaceAll("");
									if (!"".equalsIgnoreCase(answer)) {
										afterCleanArray.add(answer);
									}
								}
							}
							if (afterCleanArray.size() > 0) {
								jsonOb.put(Constants.TITLE, title);
								jsonOb.put(Constants.QUESTION, question);
								jsonOb.put(Constants.ANSWERS, afterCleanArray);

								mos.write(new Text(jsonOb.toString()), NullWritable.get(), "q_lt_80_valid/part");
							}
						} else {
							mos.write(new Text(jsonOb.toString()), NullWritable.get(), "q_lt_80_invalid/part");
						}
					} else {
						mos.write(new Text(jsonOb.toString()), NullWritable.get(), "q_lt_80_dirty/part");
					}
				} else {
					mos.write(new Text(jsonOb.toString()), NullWritable.get(), "q_gt_80/part");
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public static String afterClean(String sentence) {
		String afterCleanString = "";
		sentence = RemoveAnswerUtil.RemoveOnlyHasSymbolAndNum(sentence, pattern);
		if (!"".equalsIgnoreCase(sentence)) { // 如果全部字符都是符号或者数字就过滤掉
			afterCleanString = SymbolUtil.deDupFilterSymbol(sentence, deDupSet, removeSet); // 去掉重复的符号
			if("".equalsIgnoreCase(afterCleanString)){
				return "";
			}
			// 检查是否超过80％的字符都是外文，目前检查(日文有bug暂时跳过检查)，韩文，阿拉伯文
			if (RemoveForeignTextUtil.checkKorean(afterCleanString) || RemoveForeignTextUtil.checkArabic(afterCleanString)) {
				return "";
			}
		}
		return afterCleanString;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
	
}
