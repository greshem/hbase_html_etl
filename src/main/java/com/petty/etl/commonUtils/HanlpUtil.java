package com.petty.etl.commonUtils;

import com.petty.enlp.EKeyword;
import com.petty.enlp.EWord;
import com.petty.enlp.KeywordResult;
import com.petty.enlp.NLPService;
import com.petty.enlp.SegmentResult;

public class HanlpUtil {
	
	public static String getWords(String text){
		StringBuilder result = new StringBuilder();
		SegmentResult segmentResult = NLPService.getWords(text);
		for (EWord w : segmentResult.wordList) {
			result.append(w.word).append(" ");
		}
		return result.toString().trim();
	}
	
	public static String getKeywords(String text){
		StringBuilder result = new StringBuilder();
		KeywordResult keywordResult = NLPService.getKeywords(text);
		for (EKeyword w : keywordResult.keywordList) {
			if(w.level < 1)
				continue;
			result.append(w.word).append(" ");
		}
		return result.toString().trim();
	}
	
	public static String getNamedEntities(String text){
		String namedEntities = NLPService.getNamedEntities(text);
		return namedEntities;
	}
	
	public static String getMain2ObjectPP(String text){
		String main2ObjectPP = NLPService.getMain2ObjectPP(text);
		return main2ObjectPP;
	}
}
