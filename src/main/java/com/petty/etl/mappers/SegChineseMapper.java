package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.enlp.EWord;
import com.petty.enlp.NLPService;

public class SegChineseMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String seg = getSegStr(line);
			seg = removeNonChinese(seg);
			context.write(new Text(seg), new Text());
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
		}
	}
	
	public static String getSegStr(String question) {
		StringBuilder result = new StringBuilder();
		List<EWord> segList;
		try {
			segList = NLPService.getWords(question).wordList;
			if (segList.size() > 0) {
				for (int i = 0; i < segList.size(); i++) {
					result.append(segList.get(i).word).append(" ");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString().trim();
	}

	public static String removeNonChinese(String line){
		String tmpString = line.replaceAll("(?i)[^a-zA-Z0-9\u4E00-\u9FA5]", " ");//去掉所有中英文符号
    	char[] carr = tmpString.toCharArray();
    	for(int i = 0; i<tmpString.length();i++){
    		if(carr[i] < 0xFF){
    			carr[i] = ' ' ;//过滤掉非汉字内容
    		}
    	}
    	tmpString = String.copyValueOf(carr).trim();
    	tmpString = tmpString.replaceAll(" +", " ").trim();
    	return tmpString;
	}
}
