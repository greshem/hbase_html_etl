package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.petty.etl.constant.Constants;
import com.petty.etl.parser.BooheeParser;

import net.sf.json.JSONObject;

public class BooheeExtrctor extends BaseExtractor{

	public static void main(String[] args) {

	}

	@Override
	public List<JSONObject> extractHbaseData(String html, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		if(html == null){
			return null;
		}
		long srcTime = Date.parse(updateTime);
		
		//Parse html get question and answer list
		result = parse(url, html);
		if (result == null) {
			return null;
		}else{
			for(JSONObject object: result){
				object.put(Constants.URL, url);
				object.put(Constants.UPDATETIME, srcTime);
				object.put(Constants.CATEGORY, "shoushen");
				object.put(Constants.DOCUMENTSOURCE, getDataSource());
				
			}
		}
		return result;
	}

	private List<JSONObject> parse(String srcurl, String html) {
		List<JSONObject> result = null;
		
		if(srcurl.startsWith("http://www.boohee.com/posts/")){
			result = BooheeParser.parser(html);
		}
				
		return result;
	}
	
	@Override
	protected int getDataSource() {
		dataSource = BOOHEE;
		return dataSource;
	}

	@Override
	public List<JSONObject> extract(String data) {
		return null;
	}

}
