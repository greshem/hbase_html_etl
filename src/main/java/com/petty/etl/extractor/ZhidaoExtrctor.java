package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.petty.etl.constant.Constants;
import com.petty.etl.parser.ZhidaoParser;

import net.sf.json.JSONObject;

public class ZhidaoExtrctor extends BaseExtractor{

	public static void main(String[] args) {

	}

	@Override
	public List<JSONObject> extractHbaseData(String html, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		if(html == null){
			return result;
		}
		long srcTime = 0L;
		if(updateTime != null && !"".contentEquals(updateTime)){
			srcTime = Date.parse(updateTime);
		}
		
		//Parse html get question and answer list
		result = parse(url, html, picMap);
		if (result == null) {
			return result;
		}else{
			for(JSONObject object: result){
				object.put(Constants.URL, url);
				object.put(Constants.UPDATETIME, srcTime);
				object.put(Constants.DOCUMENTSOURCE, getDataSource());
			}
		}
		return result;
	}

	private List<JSONObject> parse(String srcurl, String html, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		
		if(srcurl.startsWith("http://zhidao.baidu.com/question")){
			result = ZhidaoParser.parser(html, picMap);
//		}else if(srcurl.startsWith("http://zhidao.baidu.com/daily")){
//			result = DoubanParser.noteParse(html);
//		}else if(srcurl.startsWith("http://zhidao.baidu.com/liuyan")){
//			result = DoubanBookParser.parseComments(html);
//		}else if(srcurl.startsWith("http://baobao.baidu.com/article")){
//			result = DoubanBookParser.parseReview(html);
//		}else if(srcurl.startsWith("http://baobao.baidu.com/duma")){
//			result = DoubanBookParser.parseAnnotation(html);
//		}else if(srcurl.startsWith("http://muzhi.baidu.com/activity")){
//			result = DoubanMovieParser.parseComment(html);
//		}else if(srcurl.startsWith("http://muzhi.baidu.com/question")){
//			result = DoubanMovieParser.parseReview(html);
//		}else{
//			return null;
		}
				
		return result;
	}
	
	@Override
	protected int getDataSource() {
		dataSource = ZHIDAO;
		return dataSource;
	}

	@Override
	public List<JSONObject> extract(String data) {
		return null;
	}

}
