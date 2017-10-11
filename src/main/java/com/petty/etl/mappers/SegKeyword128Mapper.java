package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;
import com.petty.nlp.GetSegFun;
import com.petty.nlp.KeyWordTerm;
import com.petty.nlp.NLPFlag;
import com.petty.nlp.NLPSevice;
import com.hankcs.hanlp.seg.common.Term;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SegKeyword128Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private static GetSegFun gsf = new GetSegFun();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			JSONObject lineOb = JSONObject.fromObject(line);
			String question = lineOb.getString(Constants.QUESTION);
			String filtered_pun_question = question.replaceAll(Constants.KEYWORD_REPLACE_CHAR," ");
			if(question != null && !"".equalsIgnoreCase(question)){
				lineOb.put("question_seg", getSegStr(question));
				lineOb.put("question_keyword", getKeyWords(filtered_pun_question));
			}
			
			JSONArray answerArray = lineOb.getJSONArray(Constants.ANSWERS);
			JSONArray newAnswerArray = new JSONArray();
			for(int i=0; i<answerArray.size(); i++){
				JSONObject answer = answerArray.getJSONObject(i);
				String content = answer.getString(Constants.CONTENT);
				String filtered_pun_answer = content.replaceAll(Constants.KEYWORD_REPLACE_CHAR," ");
				answer.put("answer_seg", getSegStr(content));
				answer.put("answer_keyword", getKeyWords(filtered_pun_answer));
				newAnswerArray.add(answer);
			}
			lineOb.put(Constants.ANSWERS, newAnswerArray);
			context.write(new Text(lineOb.toString()), new Text());
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
		}
	}
	
	public static String getSegStr(String question) {
		StringBuilder result = new StringBuilder();
		List<Term> segword;
		try {
			segword = gsf.testShortest(question);
			if (segword.size() > 0) {
				for (int i = 0; i < segword.size(); i++) {
					result.append(segword.get(i).word).append(" ");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString().trim();
	}


	public static String getKeyWords(String queryStr) {
		List<KeyWordTerm> keyWordTerms = NLPSevice.ProcessSentence(queryStr, NLPFlag.keyWord.getValue())
				.getKeyWordLevel();
		StringBuilder result = new StringBuilder();
		for (KeyWordTerm keyWordTerm : keyWordTerms) {
			String keyWord = keyWordTerm.getKeyword().trim();
			result.append(" ").append(keyWord);
		}
		return result.toString().trim();
	}
}
