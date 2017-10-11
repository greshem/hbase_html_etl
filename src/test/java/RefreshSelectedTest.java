import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class RefreshSelectedTest {

	public static void main(String[] args) {
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("{\"answers\":[{\"content\":\"CBS没剧了?\",\"likecount\":0,\"select\":1},{\"content\":\"破产姐妹新的一季不要再让我失望\",\"likecount\":0,\"select\":1},{\"content\":\"老妈是目前最佳喜剧类了\",\"likecount\":0,\"select\":0}]}");
		list.add("{\"answers\":[{\"content\":\"CBS没剧了?\",\"likecount\":0,\"select\":0},{\"content\":\"破产姐妹新的一季不要再让我失望\",\"likecount\":0,\"select\":0},{\"content\":\"老妈是目前最佳喜剧类了\",\"likecount\":0,\"select\":0},{\"content\":\"老妈是目前最佳喜剧类了,哈哈\",\"likecount\":4,\"select\":0}], \"latest\":1}");
		JSONObject newDataJson = new JSONObject();
		
		Set<String> selectedSet = new HashSet<String>();
		
		for(int j=0; j<list.size(); j++){
			String object = list.get(j);
			JSONObject jsonObject = JSONObject.fromObject(object);
			JSONArray answerArray = jsonObject.getJSONArray(Constants.ANSWERS);
			if(jsonObject.has(Constants.LATEST)){ // 如果json中包含了LATEST，则说明是最新process的数据
				newDataJson = JSONObject.fromObject(object);
			}else{
				for(int i=0; i<answerArray.size(); i++){
					JSONObject answer = answerArray.getJSONObject(i);
					int selectedFlag = answer.getInt(Constants.SELECT);
					if(selectedFlag == 1){
						selectedSet.add(answer.getString(Constants.CONTENT));
					}
				}
			}
		}
		
		// 更新最新process数据的select flag
		JSONArray answersArray = newDataJson.getJSONArray(Constants.ANSWERS);
		JSONArray newAnswerArray = new JSONArray();
		for(int i=0; i<answersArray.size(); i++){
			JSONObject answer = answersArray.getJSONObject(i);
			String content = answer.getString(Constants.CONTENT);
			if(selectedSet.contains(content)){
				answer.put(Constants.SELECT, 1);
			}
			newAnswerArray.add(answer);
		}
		newDataJson.put(Constants.ANSWERS, newAnswerArray);
		
		// 此时数据应该作为下次refresh的history数据，需要remove掉LATEST属性
		newDataJson.remove(Constants.LATEST);
		
		System.out.println(newDataJson);
	}

}
