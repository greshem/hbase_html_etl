package com.petty.etl.parser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.constant.Constants;

public class TiebaParser {

	/*
	 * url pattern http://tieba.baidu.com/p/4308884216
	 */
	public static JSONObject parse(String html_body, String srcurl) {
		JSONObject result = new JSONObject();

		String title = "";
		String describe = "";
		boolean isTopPage = false;

		org.jsoup.nodes.Document html = Jsoup.parse(html_body);

		if (Pattern.matches("^http://tieba.baidu.com/p/\\d+.*", srcurl)) {
			// parse title
			Element title_el = html.select("#j_core_title_wrap > h3").first();
			if (title_el != null) {
				title = title_el.text();
				if (title.startsWith("回复：")) {
					title = title.substring(3, title.length());
				}else{
					isTopPage = true;
				}
			}

			Elements replisElements = html.getElementsByClass("d_post_content");

			JSONArray reply_content_list = new JSONArray();

			// parse replies and describe
			for (Element replyElement : replisElements) {

				if (replyElement != null) {
					if (replisElements.indexOf(replyElement) == 0 && isTopPage) {
						describe = replyElement.text();
					}
					if(reply_content_list != null){
						reply_content_list.add(replyElement.text());
					}
				}
			}

			Constants.parserResultBuilder(result, title, title, describe, reply_content_list);
		} else {
			return null;
		}
		return result;
	}

}
