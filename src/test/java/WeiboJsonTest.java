import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.io.Text;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.HanlpUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.extractor.WeiboJsonExtractor;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class WeiboJsonTest {

	public static void main(String[] args) {
		String html1 = FileUtil.readFileToString("/Users/greshem/Downloads/container.txt");
		String url1 = "http://m.weibo.cn/page/json?containerid=1005051193491727_-_WEIBO_SECOND_PROFILE_WEIBO&page=1";
		String html2 = FileUtil.readFileToString("/Users/greshem/Downloads/comments.txt");
		String url2 = "http://m.weibo.cn/single/rcList?format=cards&id=3927236056539928&type=comment&hot=0&page=1";
		
		WeiboJsonExtractor ex = new WeiboJsonExtractor();
		List<JSONObject> obs = ex.extractHbaseData(html1, "", url1, null, null);
		obs.addAll(ex.extractHbaseData(html2, "", url2, null, null));
		HashMap<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		for(int i =0; i<obs.size(); i++){
			JSONObject ob = obs.get(i);
			String id = ob.getString("id");
			List<JSONObject> list = new ArrayList<JSONObject>();
			if(map.containsKey(id)){
				list = map.get(id);
			}
			JSONObject newOb = JSONObject.fromObject(ob.toString());
			newOb.put(Constants.INCREFLAG, 0);
			System.out.println(newOb);
			list.add(newOb);
			map.put(id, list);
		} 
		System.out.println();
		System.out.println();
		
		List<JSONObject> finalList = new ArrayList<JSONObject>();
		Set<String> idSet = map.keySet();
		for(String id : idSet){
			List<JSONObject> list = map.get(id);
			finalList = merge1(list, id);
		}
		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println();
		
		for(int i =0; i<obs.size(); i++){
			JSONObject ob = obs.get(i);
			String id = ob.getString("id");
			List<JSONObject> list = new ArrayList<JSONObject>();
			if(map.containsKey(id)){
				list = map.get(id);
			}
			list.add(ob);
			System.out.println(ob);
			map.put(id, list);
		} 
		System.out.println();
		System.out.println();
		idSet = map.keySet();

		List<JSONObject> totalList = new ArrayList<JSONObject>();
		for(String id : idSet){
			List<JSONObject> list = map.get(id);
			finalList = merge1(list, id);
			System.out.println(finalList.size());
			totalList.addAll(finalList);
		}
		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println();
		
		map = new HashMap<String, List<JSONObject>>();
		for(int i=0; i<totalList.size(); i++){
			JSONObject ob = totalList.get(i);
			String id = ob.getString("id");
			String question = ob.getString("question");
			String key = id + question;
			List<JSONObject> list = new ArrayList<JSONObject>();
			if(map.containsKey(key)){
				list = map.get(key);
			}
			
			list.add(ob);
			map.put(key, list);
		} 
		idSet = map.keySet();
		for(String id : idSet){
			List<JSONObject> list = map.get(id);
			merge2(list);
		}
	}

	
	public static List<JSONObject> merge1(List<JSONObject> rawList, String id){
		List<JSONObject> finalList = new ArrayList<JSONObject>();
		String description = "";
		String url = "";
		String source = "";
		String rawQuestion = "";
		String uuid = "";
		String questionSeg = "";
		String questionKeyword = "";
		JSONArray answers = new JSONArray();
		
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
		HashMap<String, JSONObject> answerSegMap = new HashMap<String, JSONObject>();
		
		List<JSONObject> list = new ArrayList<JSONObject>();
		Set<String> tagSet = new HashSet<String>();
		boolean containIncreFlag = false;
		for(int m=0; m< rawList.size(); m++){
			String object = rawList.get(m).toString();
			JSONObject json = JSONObject.fromObject(object);
			String question = json.getString(Constants.QUESTION);
			JSONArray answerArray = json.getJSONArray(Constants.ANSWERS);
			String tmpUrl = json.getString(Constants.URL);
			String tmpDescription = json.getString(Constants.DESCRIPTION);
			int increFlagValue = 0;
			if(!json.containsKey(Constants.INCREFLAG)){
				json.put(Constants.INCREFLAG, 0);
			}else{
				increFlagValue = json.getInt(Constants.INCREFLAG);
			}
			
			JSONArray tagArray = new JSONArray();
			if(json.has(Constants.TAGS)){
				tagArray = json.getJSONArray(Constants.TAGS);
				if(tagArray != null && tagArray.size() > 0){
					for(int i=0; i<tagArray.size(); i++){
						tagSet.add(tagArray.getString(i));
					}
				}
			}
			// 从http://m.weibo.cn/page/json取到的数据， 或者还没有回复的
			if(tmpUrl != null && tmpUrl.startsWith("http://m.weibo.cn/page/json")){
				description = tmpDescription;
				url = tmpUrl;
				source = json.getString(Constants.SOURCE);
				rawQuestion = question;
				if(json.has(Constants.UUID)){
					uuid = json.getString(Constants.UUID);
				}else{
					uuid = UUID.randomUUID().toString();
				}
				if(json.has(Constants.QUESTION_SEG)){
					questionSeg = json.getString(Constants.QUESTION_SEG);
					questionKeyword = json.getString(Constants.QUESTION_KEYWORD);
				}else{
					questionSeg = HanlpUtil.getWords(question);
					String filtered_pun_question = question.replaceAll("[:|.|!|?|'|\"|：|。|！|？|‘|’|“|”|、]"," ");
					questionKeyword = HanlpUtil.getKeywords(filtered_pun_question);
				}
				if(answerArray != null && answerArray.size() > 0){
					getAnswerMap(answerArray, answerMap);
					getAnswerSegMap(answerArray, answerSegMap);
					if(increFlagValue == 1){
						containIncreFlag = true;
					}
				}
			}
			// 从http://m.weibo.cn/single/rcList取到的， 只有评论内容还没有question的
			if(tmpUrl != null && tmpUrl.startsWith("http://m.weibo.cn/single/rcList")){
				if(question != null && "".equalsIgnoreCase(question)){ // 还没有关联到question的评论
					if(answerArray != null && answerArray.size() > 0){
						getAnswerMap(answerArray, answerMap);
						getAnswerSegMap(answerArray, answerSegMap);
						if(increFlagValue == 1){
							containIncreFlag = true;
						}
					}
				}else if(question != null && !"".equalsIgnoreCase(question)){
					// 回复中的回复
					list.add(json);
				}
			}
		}
		
		Set<String> answerKeys = answerMap.keySet();
		for(String answerKey: answerKeys){
			JSONObject answer = new JSONObject();
			answer.put(Constants.CONTENT, answerKey);
			answer.put("likecount", answerMap.get(answerKey));
			JSONObject answerSegOb = answerSegMap.get(answerKey);
			answer.put(Constants.ANSWER_SEG, answerSegOb.getString(Constants.ANSWER_SEG));
			answer.put(Constants.ANSWER_KEYWORD, answerSegOb.getString(Constants.ANSWER_KEYWORD));
			answers.add(answer);
		}

		JSONObject newQAOb = new JSONObject();
		newQAOb.put(Constants.TITLE, "");
		newQAOb.put(Constants.QUESTION, rawQuestion);
		newQAOb.put(Constants.DESCRIPTION, description);
		newQAOb.put(Constants.ANSWERS, answers);
		newQAOb.put(Constants.URL, url);
		newQAOb.put(Constants.TAGS, tagSet.toArray());
		newQAOb.put(Constants.SOURCE, source);
		newQAOb.put(Constants.ID, id);
		if(containIncreFlag){
			newQAOb.put(Constants.INCREFLAG, 1);
		}else{
			newQAOb.put(Constants.INCREFLAG, 0);
		}
		newQAOb.put(Constants.UUID, uuid);
		newQAOb.put(Constants.QUESTION_SEG, questionSeg);
		newQAOb.put(Constants.QUESTION_KEYWORD, questionKeyword);
		finalList.add(newQAOb);
		System.out.println(newQAOb.toString());
		
		/*
		 *  对于回复中的回复，在这一步不能建立UUID， 否则在第二步merge的时候， 就不能保证最终选择的那个UUID与history的保持一致。
		 *  如果是history里面已经设置过了， 则会在下一步merge继续保留；
		 *  如果是新的数据，则会在下一步进行创建
		 */
		
		for(int i=0; i<list.size(); i++){
	      JSONObject ob = list.get(i);
	      ob.put(Constants.DESCRIPTION, description);
	      System.out.println(ob.toString());
	      finalList.add(ob);
	    }
		System.out.println("=============================================");
		
		return finalList;
	}
	
	
	public static void merge2(List<JSONObject> rawList){
		JSONArray mergeAnswer = new JSONArray();
		String mergeQuestion = "";
		String mergeDescription = "";
		String mergeSource = "";
		String mergeUrl = "";
		String mergeCommentId = "";
		String mergeUUID = "";
		String questionSeg = "";
		String questionKeyword = "";
		
		HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
		HashMap<String, JSONObject> answerSegMap = new HashMap<String, JSONObject>();

		Set<String> tagSet = new HashSet<String>();
		boolean increFlag = false;
		int i = 0;
		for(int m=0; m< rawList.size(); m++){
			
			String jsonOb = rawList.get(m).toString();
			JSONObject object = JSONObject.fromObject(jsonOb);
			if(object.getInt(Constants.INCREFLAG) == 1){
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
			String question = object.getString(Constants.QUESTION);
			if(question != null && !"".equalsIgnoreCase(question)){
				if(i == 0){
					mergeQuestion = question;
					mergeDescription = object.getString(Constants.DESCRIPTION);
					mergeSource = object.getString(Constants.SOURCE);
					mergeUrl = object.getString(Constants.URL);
					mergeCommentId = object.getString(Constants.ID);
				}
				JSONArray answers = object.getJSONArray(Constants.ANSWERS);
				for(int j=0; j<answers.size(); j++){
					JSONObject answerObject = answers.getJSONObject(j);
					String answerContent = answerObject.getString(Constants.CONTENT).trim();
					String likeCount = answerObject.getString("likecount");
					String answerSeg = "";
					if(answerObject.containsKey(Constants.ANSWER_SEG)){
						answerSeg = answerObject.getString(Constants.ANSWER_SEG);
					}
					
					String answerKeyword = "";
					if(answerObject.containsKey(Constants.ANSWER_KEYWORD)){
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
					if(answerMap.containsKey(answerContent)){
						int sumLike = answerMap.get(answerContent) + Integer.valueOf(likeCount);
						answerMap.put(answerContent, Integer.valueOf(sumLike));
					}else{
						answerMap.put(answerContent, Integer.valueOf(likeCount));
					}
				}
				if(object.containsKey(Constants.QUESTION_SEG)){
					if(!"".equalsIgnoreCase(object.getString(Constants.QUESTION_SEG))){
						questionSeg = object.getString(Constants.QUESTION_SEG);
						questionKeyword = object.getString(Constants.QUESTION_KEYWORD);
					}
				}
				i++;
			}else if(question != null && "".equalsIgnoreCase(question)){
				System.out.println(object.toString());
			}
			
			/*
			 * 检查是否带有UUID的纪录，如果有，则表示history数据中已经含有过这条纪录，继续作为UUID；
			 * 如果没有，则表示是全新的数据， 需要创建新的
			 */
			if(object.has(Constants.UUID)){
				mergeUUID = object.getString(Constants.UUID);
			}
		}
		
		Set<String> answerKeys = answerMap.keySet();
		for(String answerKey: answerKeys){
			JSONObject answer = new JSONObject();
			answer.put(Constants.CONTENT, answerKey);
			answer.put("likecount", answerMap.get(answerKey));
			JSONObject answerSegOb = answerSegMap.get(answerKey);
			if("".equalsIgnoreCase(answerSegOb.getString(Constants.ANSWER_SEG))){
				answer.put(Constants.ANSWER_SEG, HanlpUtil.getWords(answerKey));
				String filtered_pun_answerKey = answerKey.replaceAll(Constants.KEYWORD_REPLACE_CHAR," ");
				answer.put(Constants.ANSWER_KEYWORD, HanlpUtil.getKeywords(filtered_pun_answerKey));
			}else{
				answer.put(Constants.ANSWER_SEG, answerSegOb.getString(Constants.ANSWER_SEG));
				answer.put(Constants.ANSWER_KEYWORD, answerSegOb.getString(Constants.ANSWER_KEYWORD));
			}
			mergeAnswer.add(answer);
		}
		
		if(!"".equalsIgnoreCase(mergeQuestion)){
			JSONObject finalObject = new JSONObject();
			finalObject.put(Constants.TITLE, "");
			finalObject.put(Constants.QUESTION, mergeQuestion);
			finalObject.put(Constants.ANSWERS, mergeAnswer);
			finalObject.put(Constants.DESCRIPTION, mergeDescription);
			finalObject.put(Constants.TAGS, tagSet.toArray());
			finalObject.put(Constants.URL, mergeUrl);
			finalObject.put(Constants.SOURCE, mergeSource);
			finalObject.put(Constants.ID, mergeCommentId);
			if(increFlag){
				finalObject.put(Constants.INCREFLAG, 1);
			}else{
				finalObject.put(Constants.INCREFLAG, 0);
			}
			if("".equalsIgnoreCase(mergeUUID)){
				mergeUUID = UUID.randomUUID().toString();
			}
			finalObject.put(Constants.UUID, mergeUUID);
			if(!"".equalsIgnoreCase(questionSeg)){
				finalObject.put(Constants.QUESTION_SEG, questionSeg);
				finalObject.put(Constants.QUESTION_KEYWORD, questionKeyword);
			}else{
				finalObject.put(Constants.QUESTION_SEG, HanlpUtil.getWords(mergeQuestion));
				String filtered_pun_question = mergeQuestion.replaceAll(Constants.KEYWORD_REPLACE_CHAR," ");
				finalObject.put(Constants.QUESTION_KEYWORD, HanlpUtil.getKeywords(filtered_pun_question));
			}
			System.out.println(finalObject.toString());
		}
	}
	
	public static void getAnswerMap(JSONArray answerArray, HashMap<String, Integer> answerMap){
		for(int i=0; i<answerArray.size(); i++){
			JSONObject answerObject = answerArray.getJSONObject(i);
			String answerContent = answerObject.getString(Constants.CONTENT).trim();
			String likeCount = answerObject.getString("likecount");
			
			if(likeCount == null || "null".equalsIgnoreCase(likeCount)){
				likeCount = "0";
			}
			if(answerMap.containsKey(answerContent)){
				int sumLike = answerMap.get(answerContent) + Integer.valueOf(likeCount);
				answerMap.put(answerContent, Integer.valueOf(sumLike));
			}else{
				answerMap.put(answerContent, Integer.valueOf(likeCount));
			}
		}
		System.out.println("----------"+answerMap);
	}

	public static void getAnswerSegMap(JSONArray answerArray, HashMap<String, JSONObject> answerSegMap){
		for(int i=0; i<answerArray.size(); i++){
			JSONObject answerObject = answerArray.getJSONObject(i);
			String answerContent = answerObject.getString(Constants.CONTENT).trim();
			
			String answerSeg = "";
			if(answerObject.containsKey(Constants.ANSWER_SEG)){
				answerSeg = answerObject.getString(Constants.ANSWER_SEG);
			}
			
			String answerKeyword = "";
			if(answerObject.containsKey(Constants.ANSWER_KEYWORD)){
				answerKeyword = answerObject.getString(Constants.ANSWER_KEYWORD);
			}
			
			// 搜集answer的Seg和keyword的信息
			if(answerSegMap.containsKey(answerContent)){
				JSONObject jsonOb = answerSegMap.get(answerContent);
				String currentSeg = jsonOb.getString(Constants.ANSWER_SEG);
				// 当现有的seg和keyword是空，而同样的Answer中的seg，keyword不为空，就覆盖
				if("".equalsIgnoreCase(currentSeg) && !"".equalsIgnoreCase(answerSeg)){
					jsonOb.put(Constants.ANSWER_SEG, answerSeg);
					jsonOb.put(Constants.ANSWER_KEYWORD, answerKeyword);
				}
				answerSegMap.put(answerContent, jsonOb);
			}else{
				JSONObject segOb = new JSONObject();
				segOb.put(Constants.ANSWER_SEG, answerSeg);
				segOb.put(Constants.ANSWER_KEYWORD, answerKeyword);
				answerSegMap.put(answerContent, segOb);
			}
		}
	}
}
