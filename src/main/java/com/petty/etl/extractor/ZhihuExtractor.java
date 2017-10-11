package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.petty.etl.commonUtils.CalendarUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.parser.ZhihuParser;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class ZhihuExtractor extends BaseExtractor {

	private String html;

	// in result
	private long update_time;
	private String srcurl;
	private String question;
	private String title;
	private String description;
	private JSONArray tags;
	private JSONObject currentAnswer = new JSONObject();
	private String commentQuestion;

	public List<JSONObject> extract(String data) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		JSONObject srcData = JSONObject.fromObject(data);
		html = srcData.getString("html_body");
		srcurl = srcData.getString("url");

		tags = getTagList();

		if (html == null) {
			return null;
		}

		JSONObject date = JSONObject.fromObject(srcData.getString(Constants.UPDATETIME));
		if (String.valueOf(date.get("$date")).contains("T")) {
			String tt = String.valueOf(date.get("$date")).replace("T", " ").replace("Z", "");
			update_time = CalendarUtil.Datetime2Unix(tt, "yyyy-MM-dd HH:mm:ss.SSS");
		} else {
			update_time = date.getLong("$date");
		}

		// Parse html get question and answer list
		JSONObject qa = parse();
		if (qa == null) {
			return null;
		}

		// Get title
		title = qa.getString(Constants.TITLE);

		// Get description
		description = qa.getString(Constants.DESCRIPTION);

		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, srcurl);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, update_time);

		result.add(qa);
		List<JSONObject> replies = getCommentsToReply();
		result.addAll(replies);

		return result;
	}

	public List<JSONObject> extractComments(String line) {
		List<JSONObject> result = new ArrayList<JSONObject>();

		JSONObject lineObject = JSONObject.fromObject(line);
		JSONArray answerList = new JSONArray();
		JSONArray answerArray = lineObject.getJSONArray(Constants.ANSWERS);
		question = lineObject.getString(Constants.QUESTION);

		if (answerArray.size() > 0 && !"".equals(question)) {

			List<JSONObject> repliesToComments = new ArrayList<JSONObject>();

			HashMap<Integer, String> answerDict = new HashMap<Integer, String>();
			for (int i = 0; i < answerArray.size(); i++) {
				currentAnswer = answerArray.getJSONObject(i);
				String answer = currentAnswer.getString("content");
				int answerId = currentAnswer.getInt("id");
				answerDict.put(answerId, answer);
			}

			for (int i = 0; i < answerArray.size(); i++) {
				currentAnswer = answerArray.getJSONObject(i);
				JSONObject replyToComment = new JSONObject();
				String answer = currentAnswer.getString("content").replaceAll("(<p>|</p>|<br>|</br>)*", "");
				int likesCount = currentAnswer.getInt("likesCount");
				JSONObject likecountAnswer = new JSONObject();
				likecountAnswer.put("content", answer);
				likecountAnswer.put("likecount", likesCount);

				int replyToCommentId = currentAnswer.getInt("inReplyToCommentId");

				if (replyToCommentId != 0 && answerDict.containsKey(replyToCommentId)) {
					commentQuestion = answerDict.get(replyToCommentId);
					replyToComment = getReplyToComment(line, commentQuestion, likecountAnswer);
					repliesToComments.add(replyToComment);
				} else {
					answerList.add(likecountAnswer);
				}
			}

			lineObject.put("answers", answerList);
			result.add(lineObject);
			
			result.addAll(repliesToComments);
			answerDict.clear();
			return result;
		} else {		
			return null;
		}
	}

	public JSONObject parse() {
		if (html == null || html.trim().equals("")) {
			return null;
		}

		JSONObject result = null;
		result = ZhihuParser.parse(html, srcurl);
		return result;
	}

	public JSONArray getTagList() {
		JSONArray result = new JSONArray();
		org.jsoup.nodes.Document htmlDoc = Jsoup.parse(html);
		Elements tagElements = htmlDoc.getElementsByClass("zm-item-tag");
		if (tagElements == null) {
			return result;
		}

		for (Element tagElement : tagElements) {
			result.add(tagElement.text());
		}

		return result;
	}

	protected int getDataSource() {
		dataSource = ZHIHU;
		return dataSource;
	}

	private List<JSONObject> getCommentsToReply() {
		List<JSONObject> resultList = new ArrayList<JSONObject>();
		Document htmlDoc = Jsoup.parse(html);
		Elements reply_content_docs = htmlDoc.select(
				"#zh-question-answer-wrap > div.zm-item-answer.zm-item-expanded > div.zm-item-rich-text.js-collapse-body > div.zm-editable-content.clearfix");
		Elements reply_comments_docs = htmlDoc.select("#zh-question-answer-wrap > div > a.zg-anchor-hidden.ac");

		JSONArray content_answers = new JSONArray();
		String reply_comment;
		String reply_content;
		tags = getTagList();

		for (int i = 0; i < reply_content_docs.size(); i++) {
			reply_content = reply_content_docs.get(i).text();
			JSONObject result = new JSONObject();

			if (reply_content != null && !reply_content.trim().equals("")) {
				try {
					reply_comment = reply_comments_docs.get(i).attr("name").replaceAll("-comment", "");
					result.put(Constants.TITLE, title);
					result.put(Constants.COMMENTID, reply_comment);
					result.put(Constants.QUESTION, reply_content);
					result.put(Constants.ANSWERS, content_answers);
					result.put(Constants.TAGS, tags);
					result.put(Constants.DESCRIPTION, description);
					result.put(Constants.URL, srcurl);
					result.put(Constants.DOCUMENTSOURCE, getDataSource());
					result.put(Constants.UPDATETIME, update_time);
					result.put(Constants.INCREFLAG, 1);
					resultList.add(result);
				} catch (IndexOutOfBoundsException e) {
					System.out.println(e.getMessage());
				}
			}
		}
		return resultList;
	}

	private JSONObject getReplyToComment(String line, String question, JSONObject answer) {
		JSONObject result = JSONObject.fromObject(line);
		JSONArray content_answers = new JSONArray();
		content_answers.add(answer);
		result.put(Constants.QUESTION, question);
		result.put(Constants.ANSWERS, content_answers);
		result.put(Constants.INCREFLAG, 1);
		result.put(Constants.UUID, UUID.randomUUID().toString());
		return result;
	}

	@Override
	public List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag,
			HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		html = htmlBody;
		srcurl = url;

		tags = getTagList();

		if (html == null) {
			return null;
		}

		// Parse html get question and answer list
		JSONObject qa = parse();
		if (qa == null) {
			return null;
		}

		// Get title
		title = qa.getString(Constants.TITLE);

		// Get description
		description = qa.getString(Constants.DESCRIPTION);

		Document htmlDoc = Jsoup.parse(html);
		Elements like_counts = htmlDoc.select("span.count");

		JSONArray answers = qa.getJSONArray(Constants.ANSWERS);
		JSONArray finalAnswers = new JSONArray();

		for (int i = 0; i < answers.size(); i++) {
			String answer = answers.get(i).toString();

			if (!qa.containsKey(Constants.COMMENTID)) {
				int likecount = 0;
				try {
					likecount = Integer.parseInt(like_counts.get(i).text());
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				JSONObject likecountAnswer = new JSONObject();
				likecountAnswer.put("content", answer);
				likecountAnswer.put("likecount", likecount);
				finalAnswers.add(likecountAnswer);
			} else {
				finalAnswers.add(answer);
			}
		}

		qa.put(Constants.ANSWERS, finalAnswers);
		qa.put(Constants.TAGS, tags);
		qa.put(Constants.URL, url);
		qa.put(Constants.DOCUMENTSOURCE, getDataSource());
		qa.put(Constants.UPDATETIME, updateTime);
		qa.put(Constants.INCREFLAG, 1);
		result.add(qa);

		List<JSONObject> replies = getCommentsToReply();
		result.addAll(replies);
		return result;
	}
}
