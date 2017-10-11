package com.petty.etl.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.HanlpUtil;
import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class MergeSolrReducer extends Reducer<Text, Text, Text, Text> {
	
	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		JSONArray mergeAnswer = new JSONArray();
		String mergeQuestion = "";
		String mergeTitle = "";
		String mergeDescription = "";
		String mergeSource = "";
		String mergeUrl = "";
		String mergeCommentId = "";
		String mergeUUID = "";
		String questionSeg = "";
		String questionKeyword = "";
		String mergeQuestionEmotion = "";
		String mergeQuestionSpeech = "";
		String mergeQuestionTopic = "";
		
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
		HashMap<String, JSONObject> answerSegMap = new HashMap<String, JSONObject>();
		HashMap<String, Integer> answerSelectMap = new HashMap<String, Integer>();
		HashMap<String, String> answerEmotionMap = new HashMap<String, String>();
		HashMap<String, String> answerSpeechMap = new HashMap<String, String>();
		HashMap<String, String> answerTopicMap = new HashMap<String, String>();
		HashMap<String, String> answerUUIDMap = new HashMap<String, String>();

		Set<String> tagSet = new HashSet<String>();
		boolean increFlag = false;
		int i = 0;
		while(it.hasNext()){
			String jsonOb = it.next().toString();
			JSONObject object = JSONObject.fromObject(jsonOb);
			if(object.has(Constants.INCREFLAG) && object.getInt(Constants.INCREFLAG) == 1){
				increFlag = true;
			}
			JSONArray tagArray = new JSONArray();
			if(object.has(Constants.TAGS)){
				tagArray = object.getJSONArray(Constants.TAGS);
				if(tagArray != null && tagArray.size() > 0){
					for(int j=0; j<tagArray.size(); j++){
						tagSet.add(tagArray.getString(j));
					}
				}
			}
			
			// 检查是否已经包含了CU module的属性
			if(object.has(Constants.EMOTION)){
				mergeQuestionEmotion = object.getString(Constants.EMOTION);
				mergeQuestionSpeech = object.getString(Constants.SPEECHACT);
				mergeQuestionTopic = object.getString(Constants.TOPIC);
			}
			
			String question = object.getString(Constants.QUESTION);
			if(question != null && !"".equalsIgnoreCase(question)){
				if(i == 0){
					mergeQuestion = question;
					mergeTitle = object.getString(Constants.TITLE);
					if(object.has(Constants.DESCRIPTION)){
						mergeDescription = object.getString(Constants.DESCRIPTION);
					}
					mergeSource = object.getString(Constants.SOURCE);
					mergeUrl = object.getString(Constants.URL);
					if(object.has(Constants.ID)){
						mergeCommentId = object.getString(Constants.ID);
					}
				}
				JSONArray answers = object.getJSONArray(Constants.ANSWERS);
				
				for(int j=0; j<answers.size(); j++){
					JSONObject answerObject = answers.getJSONObject(j);
					String answerContent = answerObject.getString(Constants.CONTENT).trim();
					String likeCount = "0";
					if(answerObject.has("likecount")){
						likeCount = answerObject.getString("likecount");
					}
					String answerSeg = "";
					String answerKeyword = "";
					if(answerObject.containsKey(Constants.ANSWER_SEG)){
						answerSeg = answerObject.getString(Constants.ANSWER_SEG);
						answerKeyword = answerObject.getString(Constants.ANSWER_KEYWORD);
					}
					
					// 搜集answer的Seg和keyword的信息
					if(answerSegMap.containsKey(answerContent)){
						JSONObject answerSegOb = answerSegMap.get(answerContent);
						String currentSeg = answerSegOb.getString(Constants.ANSWER_SEG);
						// 当现有的seg和keyword是空，而同样的Answer中的seg，keyword不为空，就覆盖
						if("".equalsIgnoreCase(currentSeg) && !"".equalsIgnoreCase(answerSeg)){
							answerSegOb.put(Constants.ANSWER_SEG, answerSeg);
							answerSegOb.put(Constants.ANSWER_KEYWORD, answerKeyword);
						}
						answerSegMap.put(answerContent, answerSegOb);
					}else{
						JSONObject segOb = new JSONObject();
						segOb.put(Constants.ANSWER_SEG, answerSeg);
						segOb.put(Constants.ANSWER_KEYWORD, answerKeyword);
						answerSegMap.put(answerContent, segOb);
					}
					
					if(likeCount == null || "null".equalsIgnoreCase(likeCount)){
						likeCount = "0";
					}
					// 此时的点赞数，应该用最大的那个值， 而不是再相加
					if(answerMap.containsKey(answerContent)){
						if(answerMap.get(answerContent) < Integer.valueOf(likeCount)){
							answerMap.put(answerContent, Integer.valueOf(likeCount));
						}
					}else{
						answerMap.put(answerContent, Integer.valueOf(likeCount));
					}
					
					// 检查answer是否被人工筛选过
					int selectFlag = 0;
					if(answerObject.has(Constants.SELECT)){
						selectFlag = answerObject.getInt(Constants.SELECT);
					}
					if(answerSelectMap.containsKey(answerContent)){
						if(selectFlag == 1){
							answerSelectMap.put(answerContent, selectFlag);
						}
					}else{
						answerSelectMap.put(answerContent, selectFlag);
					}
					
					// 检查Answer CU的属性
					if(answerObject.has(Constants.EMOTION)){
						answerEmotionMap.put(answerContent, answerObject.getString(Constants.EMOTION));
						answerSpeechMap.put(answerContent, answerObject.getString(Constants.SPEECHACT));
						answerTopicMap.put(answerContent, answerObject.getString(Constants.TOPIC));
					}
					
					// 检查Answer UUID的属性
					if(answerObject.has(Constants.ANSWER_UUID) && !"".equalsIgnoreCase(answerObject.getString(Constants.ANSWER_UUID))){
						if(!answerObject.has(answerContent)){
							answerUUIDMap.put(answerContent, answerObject.getString(Constants.ANSWER_UUID));
						}
					}
				}
				i++;
				if(object.containsKey(Constants.QUESTION_SEG)){
					if(!"".equalsIgnoreCase(object.getString(Constants.QUESTION_SEG))){
						questionSeg = object.getString(Constants.QUESTION_SEG);
						questionKeyword = object.getString(Constants.QUESTION_KEYWORD);
					}
				}
			}
			
			/*
			 * 检查是否带有UUID的纪录，如果有，则表示history数据中已经含有过这条纪录，继续作为UUID；
			 * 如果没有，则表示是全新的数据， 需要创建新的
			 */
			if(object.has(Constants.UUID) && !"".equalsIgnoreCase(object.getString(Constants.UUID))){
				mergeUUID = object.getString(Constants.UUID);
			}
		}
		
		if("".equalsIgnoreCase(mergeUUID)){
			mergeUUID = UUID.randomUUID().toString();
		}
		
		Set<String> answerKeys = answerMap.keySet();
		for(String answerKey: answerKeys){
			JSONObject answer = new JSONObject();
			answer.put(Constants.CONTENT, answerKey);
			answer.put("likecount", answerMap.get(answerKey));
			JSONObject answerSegOb = answerSegMap.get(answerKey);
			if("".equalsIgnoreCase(answerSegOb.getString(Constants.ANSWER_SEG))){
				answer.put(Constants.ANSWER_SEG, HanlpUtil.getWords(answerKey));
				answer.put(Constants.ANSWER_KEYWORD, HanlpUtil.getKeywords(answerKey));
			}else{
				answer.put(Constants.ANSWER_SEG, answerSegOb.getString(Constants.ANSWER_SEG));
				answer.put(Constants.ANSWER_KEYWORD, answerSegOb.getString(Constants.ANSWER_KEYWORD));
			}
			answer.put(Constants.SELECT, answerSelectMap.get(answerKey));
			
			if(answerEmotionMap.containsKey(answerKey)){
				answer.put(Constants.EMOTION, answerEmotionMap.get(answerKey));
				answer.put(Constants.SPEECHACT, answerSpeechMap.get(answerKey));
				answer.put(Constants.TOPIC, answerTopicMap.get(answerKey));
			}
			if(answerUUIDMap.containsKey(answerKey)){
				String aId = answerUUIDMap.get(answerKey);
				answer.put(Constants.ANSWER_UUID, mergeUUID + "_" + aId.split("_")[1]);
			}else{
				answer.put(Constants.ANSWER_UUID, mergeUUID + "_" + UUID.randomUUID().toString());
			}
			mergeAnswer.add(answer);
		}
		
		if(!"".equalsIgnoreCase(mergeQuestion) && mergeAnswer.size() > 0){
			JSONObject finalObject = new JSONObject();
			finalObject.put(Constants.TITLE, mergeTitle);
			finalObject.put(Constants.QUESTION, mergeQuestion);
			finalObject.put(Constants.ANSWERS, mergeAnswer);
			finalObject.put(Constants.DESCRIPTION, mergeDescription);
			finalObject.put(Constants.TAGS, tagSet.toArray());
			finalObject.put(Constants.URL, mergeUrl);
			finalObject.put(Constants.SOURCE, mergeSource);
			finalObject.put(Constants.ID, mergeCommentId);
			finalObject.put(Constants.UUID, mergeUUID);
			if(!"".equalsIgnoreCase(questionSeg)){
				finalObject.put(Constants.QUESTION_SEG, questionSeg);
				finalObject.put(Constants.QUESTION_KEYWORD, questionKeyword);
			}else{
				finalObject.put(Constants.QUESTION_SEG, HanlpUtil.getWords(mergeQuestion));
				finalObject.put(Constants.QUESTION_KEYWORD, HanlpUtil.getKeywords(mergeQuestion));
			}
			finalObject.put(Constants.TOPIC, mergeQuestionTopic);
			finalObject.put(Constants.SPEECHACT, mergeQuestionSpeech);
			finalObject.put(Constants.EMOTION, mergeQuestionEmotion);
			if(increFlag){
				mos.write(new Text(finalObject.toString()), new Text(), "solr/part");
			}
			mos.write(new Text(finalObject.toString()), new Text(), "history/part");
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
	
}
