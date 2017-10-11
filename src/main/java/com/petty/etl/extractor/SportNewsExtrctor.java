package com.petty.etl.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.petty.etl.parser.HupuSportNewsParser;
import com.petty.etl.parser.SinaSportNewsParser;
import com.petty.etl.parser.SohuSportNewsParser;
import com.petty.etl.parser.WangyiSportNewsParser;

import net.sf.json.JSONObject;

public class SportNewsExtrctor extends BaseExtractor{

	@Override
	public List<JSONObject> extractHbaseData(String html, String updateTime, String url, String tag, HashMap<String, String> picMap) {
		List<JSONObject> result = new ArrayList<JSONObject>();
		if(html == null){
			return result;
		}
		
		if(url.startsWith("http://sports.sina.com.cn/")){
			result = SinaSportNewsParser.parser(html, SINA, url);
		}else if(url.startsWith("http://sports.163.com/")){
			result = WangyiSportNewsParser.parser(html, WANGYI, url);
		}else if(url.startsWith("http://sports.sohu.com/") || url.startsWith("http://cbachina.sports.sohu.com/")){
			result = SohuSportNewsParser.parser(html, SOHU, url);
		}else if(url.startsWith("http://voice.hupu.com/") || url.startsWith("http://afp.hupu.com/") ){
			result = HupuSportNewsParser.parser(html, HUPU, url);
		}
		return result;
	}
	
	@Override
	protected int getDataSource() {
		return dataSource;
	}

	@Override
	public List<JSONObject> extract(String data) {
		return null;
	}

}
