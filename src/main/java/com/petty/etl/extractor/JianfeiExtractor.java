package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.constant.Constants;
import com.petty.etl.parser.JianFeiParser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JianfeiExtractor extends BaseExtractor {
	private String html;

	// in result
	private long update_time;
	private String srcurl;
	private JSONArray tags;

	@Override
	public List<JSONObject> extract(String data) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag,
			HashMap<String, String> picMap) {
		html = htmlBody;
		srcurl = url;
		tags = new JSONArray();

		if (html == null) {
			return null;
		}

		update_time = 0L;

		return parse();
	}
	
	public List<JSONObject> parse() {
		List<JSONObject> result = new ArrayList<JSONObject>();
		List<JSONObject> qas = null;

		if(srcurl.startsWith("http://jianfei.39.net/fitask/topic-")) {			
			qas = JianFeiParser.parseQA(html);
		} else if (srcurl.startsWith("http://jianfei.39.net/thread-")) {
			qas = JianFeiParser.parseBBS(html);
		} else if(Pattern.matches("http://fitness.39.net/special/jfzrx/\\d+/\\S+", srcurl)) {
			qas = JianFeiParser.parseStar(html);
		} else {
			return null;
		}
		
		for (JSONObject qa:qas) {
			qa.put(Constants.TAGS, tags);
			qa.put(Constants.URL, srcurl);
			qa.put(Constants.DOCUMENTSOURCE, getDataSource());
			qa.put(Constants.UPDATETIME, update_time);
			qa.put(Constants.CATEGORY, "jianfei");
			result.add(qa);
		}
		
		return result;
	}

	@Override
	protected int getDataSource() {
		dataSource = JIANFEI;
		return dataSource;
	}

}
