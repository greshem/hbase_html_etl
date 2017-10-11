package com.petty.etl.extractor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ZhidaoUrlExtrctor{

	public static List<String> extractHbaseData(String html) {
		List<String> urlList = new ArrayList<String>();
		if(html == null || "".equalsIgnoreCase(html.trim())){
			return urlList;
		}
//		File file = new File("/Users/greshem/codes/myprojects/corpusetl/test.html");
		Document htmlDoc = null;
		htmlDoc = Jsoup.parse(html);
//		try {
//			htmlDoc = Jsoup.parse(file, "UTF-8", "http://zhidao.baidu.com/");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		if(htmlDoc == null){
			return urlList;
		}
		Elements imgs = htmlDoc.select("img");
		for(int i=0; i< imgs.size(); i++){
			Element img = imgs.get(i);
			String url = img.attr("src");
//			System.out.println("url: "+url);
			if(url.contains("api/getdecpic")){
//				System.out.println(url);
				urlList.add(url);
			}
		}
		return urlList;
	}
	
	public static void main(String[] args){
		extractHbaseData("1");
	}
}
