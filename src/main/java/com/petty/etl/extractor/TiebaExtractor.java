package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.petty.etl.commonUtils.CalendarUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.parser.TiebaParser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TiebaExtractor extends BaseExtractor {

	private String html;

	// in result
	private long update_time;
	private String srcurl;
	private JSONArray tags;

	public List<JSONObject> extract(String data) {
		return null;
	}

	public JSONObject parse() {
		if (html == null || html.trim().equals("")) {
			return null;
		}

		JSONObject result = null;
		result = TiebaParser.parse(html, srcurl);
		return result;
	}

	protected int getDataSource() {
		dataSource = TIEBA;
		return dataSource;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		html = htmlBody;
		srcurl = url;

		if (html == null) {
			return null;
		}

		update_time = 0L;

		// Parse html get question and answer list
		JSONObject qa = parse();
		if (qa == null) {
			return null;
		}

		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, srcurl);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, update_time);

		result.add(qa);

		return result;
	}
}
