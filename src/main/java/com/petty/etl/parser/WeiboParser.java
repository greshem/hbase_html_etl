package com.petty.etl.parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class WeiboParser {
	
	public static JSONObject parse(String html){
        JSONObject result = new JSONObject();

        Document html_obj = Jsoup.parse(html);
        Elements replies = html_obj.select("span.ctt");
        if(replies==null || replies.size()==0){
            return null;
        }
        String question = replies.get(0).text();
        JSONArray answers = new JSONArray();
        int size = replies.size();
        for(int i=1;i<size;i++){
            Element reply = replies.get(i);
            Elements at = reply.select("a");
            reply.select("a").remove();
            String an = reply.text().trim().replaceAll("^回复：", "").replaceAll("^回复:", "").replaceAll("^：", "").replaceAll("^:", "");
            answers.add(an);
        }
        result.put("title",question);
        result.put("question",question);
        result.put("answers",answers);
        result.put("description","");
        result.put("tags","");
        result.put("update_time","");
        result.put("source",102);
        
        return result;
    }
	
}
