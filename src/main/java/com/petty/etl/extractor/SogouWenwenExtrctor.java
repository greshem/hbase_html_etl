package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.petty.etl.parser.SogouWenwenParser;

import net.sf.json.JSONObject;

public class SogouWenwenExtrctor extends BaseExtractor{

	@Override
	public List<JSONObject> extractHbaseData(String html, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		if(html == null){
			return result;
		}
		
		if(url.startsWith("http://wenwen.sogou.com/s/") || url.startsWith("http://wenwen.sogou.com/z/")){
			result = SogouWenwenParser.parser(html, SOGOUWENWEN, url);
		}
		return result;
	}
	
	@Override
	protected int getDataSource() {
		return dataSource;
	}

	@Override
	public List<JSONObject> extract(String data) {
		return null;
	}

}
