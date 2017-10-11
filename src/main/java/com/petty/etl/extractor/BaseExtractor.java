package com.petty.etl.extractor;

import java.util.HashMap;
import java.util.List;

import net.sf.json.JSONObject;

public abstract class BaseExtractor {
	//data source
	protected static final int DOUBAN = 101;
	protected static final int WEIBO = 102;
	protected static final int ZHIHU = 103;
	protected static final int TIANYA = 104;
	protected static final int ZHIDAO = 105;
	protected static final int TIEBA = 106;
	protected static final int SHUOLIANAI = 107;
	protected static final int BOOHEE = 108;
	protected static final int FENGONE = 109;
	protected static final int JIANFEI = 110;
	protected static final int SINA = 111;
	protected static final int SOHU = 112;
	protected static final int WANGYI = 113;
	protected static final int HUPU = 114;
	protected static final int BING = 115;
	protected static final int SOGOUWENWEN = 116;
	
	protected int dataSource;
	
	public abstract List<JSONObject> extract(String data);

	public abstract List<JSONObject> extractHbaseData(String htmlBody, String updateTime, String url, String tag, HashMap<String, String> picMap);
	
	protected abstract int getDataSource();
}
