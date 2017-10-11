package com.petty.etl.constant;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Constants {
	public static final String TITLE = "title";
	public static final String QUESTION = "question";
	public static final String ANSWERS = "answers";
	public static final String DESCRIPTION = "description";
	public static final String COMMENTID = "comment_id";
	public static final String TAGS = "tags";
	public static final String UPDATETIME = "update_time";
	public static final String DOCUMENTSOURCE = "source";
	public static final String URL = "url";
	public static final String CATEGORY = "category";
	public static final String ID = "id";
	public static final String SOURCE = "source";
	public static final String CONTENT = "content";
	public static final String SELECT = "select";
	public static final String LATEST = "latest";
	public static final String EMOTION = "emotion";
	public static final String TOPIC = "topic";
	public static final String SPEECHACT = "speech_act";
	public static final String INTENT = "intent";
	public static final String INCREFLAG = "incre_flag";
	public static final String UUID = "uuid";
	public static final String ANSWER_UUID = "answer_uuid";
	public static final String QUESTION_SEG = "question_seg";
	public static final String QUESTION_KEYWORD = "question_keyword";
	public static final String ANSWER_SEG = "answer_seg";
	public static final String ANSWER_KEYWORD = "answer_keyword";
	public static final String SPECIAL = "@@竹间智能@@";
	public static final String KEYWORD_REPLACE_CHAR = "[:|.|!|?|'|\"|：|。|！|？|‘|’|“|”|、]";

	public static void parserResultBuilder(JSONObject r, String title, String question, String describe,
			JSONArray answers) {
		try {
			r.put(TITLE, title);
			r.put(QUESTION, question);
			r.put(ANSWERS, answers);
			r.put(DESCRIPTION, describe);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
