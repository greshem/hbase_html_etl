package com.petty.etl.commonUtils;

import java.util.HashSet;
import java.util.regex.Pattern;

public class RemoveAnswerUtil {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pattern pattern = constructPattern("/Users/greshem/codes/myprojects/corpusetl/files/symbol.txt");
		//Pattern pattern = constructPattern("/Users/ldy/git/corpusetl/corpusetl/files/symbol.txt");
		System.out.println(pattern);
		
		String[] sentence = {
				"abc你好，，，。}}}2352345?\\>>。。1039234234570923456", //不变为""
				"2352345?\\>>。。1039234234570923456",
				"?\\>>。。",
				"，，，。}}}2352345?\\>>。。1039234234570923456",
				"…",
				"abc",
				"abcACDP1234';.,",
				"欺负人呗"
		};
		
		for (String s:sentence) {
			String result = RemoveOnlyHasSymbolAndNum(s, pattern);
		
			System.out.println("result:"+result);
		}
	}
	
	public static String RemoveOnlyHasSymbolAndNum(String sentence, Pattern pattern) {
		if (pattern.matcher(sentence).find()) {
			return "";
		}
		
		return sentence;
	}
	
	public static Pattern constructPattern(String path) {
		String filterString = "";
		String convertChar = "*.?+$^[](){}|\\";
		HashSet<String> set = FileUtil.readFile(path);
		
		for (String symbol:set) {
			if (convertChar.contains(symbol)) {
				filterString = "\\" + symbol + "|" + filterString;
			} else {
				filterString = symbol + "|" + filterString;
			}
		}
		
		filterString = filterString + "\\d|\\s|[a-zA-Z]";		
		String pattern = "^("+filterString+"){1,500}$";
		
		return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
	}

}
