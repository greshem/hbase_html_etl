package com.petty.etl.factory;
import com.petty.etl.extractor.BaseExtractor;
import com.petty.etl.extractor.DoubanExtractor;


public class ExtractorFactory {
	public static BaseExtractor getExtractor(String data) {
		return new DoubanExtractor();
	}
}
