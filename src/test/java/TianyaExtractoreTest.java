import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.bson.Document;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.RemoveAnswerUtil;
import com.petty.etl.commonUtils.RemoveForeignTextUtil;
import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.extractor.TianyaBbsExtractor;
import com.petty.etl.extractor.ZhihuExtractor;
import com.petty.etl.factory.MongoDBFactory;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class TianyaExtractoreTest {
	private TianyaBbsExtractor extractor = new TianyaBbsExtractor();
	private List<JSONObject> result;
	private static int count = 0;
	
	private void test() {
		// TODO Auto-generated method stub
		Long sTime = System.currentTimeMillis();   	    	
    	MongoDatabase src_db = MongoDBFactory.getClientInstance("192.168.1.21", 27017, "tianya");    	
    	FindIterable<Document> iterable = src_db.getCollection("html").find().limit(100);
    	
    	iterable.forEach(new Block<Document>() {
    	    
    	    public void apply(final Document document) {
    	    	result = extractor.extract(document.toJson());
    	    	if (result != null) {
    	    		System.out.println(result.toString());
    	    		count ++;
    	    	}
    	    }
    	});
    	
    	System.out.println( "count:"+TianyaExtractoreTest.count );
    	Long eTime = System.currentTimeMillis();
        System.out.println( "time:"+(eTime-sTime)/1000 );
	}
	
	private void testCommentToReply() {
		// TODO Auto-generated method stub
		Long sTime = System.currentTimeMillis();
		String filename = "/Users/ldy/tianya_html.html";
		
		try {
			String html = readFile(filename);
			List<JSONObject> result = extractor.getCommentToReply(html);
			
			for (JSONObject r : result) {
				System.out.println(r);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	Long eTime = System.currentTimeMillis();
        System.out.println( "time:"+(eTime-sTime)/1000 );
	}
	
	private void testReplytoReply() {
		Long sTime = System.currentTimeMillis();
		//String filename = "/Users/ldy/tianya2.html";
		String filename = "/Users/ldy/tianya8.html";
		
		try {
			String html = readFile(filename);
			List<JSONObject> result = extractor.getReplyToReply(html);
			
			for (JSONObject r : result) {
				System.out.println(r);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	Long eTime = System.currentTimeMillis();
        System.out.println( "time:"+(eTime-sTime)/1000 );
	}
	
	public void testRemoveString() {
		String r = extractor.removeUselessString("作者: @cagcag 回复日期: 2012-01-27 23:45:58 aa");
		
		System.out.println("result:"+r);
	}
	
	public void testRemovequestion() {
		//作者:无事出来遛老公 回复日期:2010-04-20 15:58:08
		String r = extractor.removeUselessFromQuestion("作者:无事出来遛老公 回复日期:2010-04-20 15:58:08 作者:暖包包 回复日期:2010-04-19 19:49:16 牛奶美白法确实可行,兰州好多朋友长期喝牛奶,肤色那个赞啊~~~BUT,兰州杯具了,兰州是喝牛奶会便秘");
		
		r = extractor.removeUselessFromQuestion("[发自iPad客户端-贝客悦读] 来自:Android客户端 来自Android客户端 | 举报 | 回复 本帖发自天涯社区手机客户端 [发自Android客户端-贝客悦读] aaa");
		
		r = extractor.removeUselessFromQuestion("作者:吃饱穿暖就好 提交日期:2012-08-13 22:54:13    16# 在这谈车论驾的时");
		System.out.println("result:"+r);
	}
	
	public void testClearQuestion() {
		List<String> list = new ArrayList<String>();
		String filename = "/Users/ldy/output_raw";
		String outFileName = "/Users/ldy/output";

		int count = 0;
		File f = new File(outFileName);		
		
		//read file
		try {
			if (!f.exists()) {
				f.createNewFile();
			}
			File file = new File(filename);
			BufferedReader bf = new BufferedReader(new FileReader(file));
			
			String content = "";

			while(content != null){
				content = bf.readLine();
				
				if(content == null){
					break;
				}
				if (content.contains("(null)")) {
					content = content.replace("(null)", "");
				}
				System.out.println(content);
				JSONObject jObj = extractor.clearInvalidQuestionContent(content);
						
				if (jObj == null) {
					continue;
				}
				
				String question = jObj.getString("question");
				JSONArray answers = jObj.getJSONArray(Constants.ANSWERS);
		
				list.add("question:"+question+"\n");
				for (int i = 0; i < answers.size(); i++) {
					list.add(Constants.ANSWERS+":"+answers.getString(i)+"\n");
				}
				list.add("\n\n");

			}
			
			bf.close();
			
			FileOutputStream fos = new FileOutputStream(f);
			for(String str:list) {
				byte[] contentInBytes = str.getBytes();
				fos.write(contentInBytes);
			}
			fos.flush();
			fos.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public String readFile(String filename) throws IOException{		
		File file = new File(filename);
		BufferedReader bf = new BufferedReader(new FileReader(file));
		
		String content = "";
		StringBuilder sb = new StringBuilder();

		while(content != null){
			content = bf.readLine();
	
			if(content == null){
				break;
			}
	
			sb.append(content.trim());
		}

		bf.close();
		return sb.toString();		
	}
	
	public void testRemove() {
		Pattern pattern = RemoveAnswerUtil.constructPattern("/Users/ldy/git/corpusetl/corpusetl/files/symbol.txt");
		HashSet<String> deDupSet = FileUtil.readFile("/Users/ldy/git/corpusetl/corpusetl/files/symbol.txt");;
		HashSet<String> removeSet = FileUtil.readFile("/Users/ldy/git/corpusetl/corpusetl/files/symbol_filter.txt");;
		
		String filename = "/Users/ldy/invalid";
		
		try{
			File file = new File(filename);
			BufferedReader bf = new BufferedReader(new FileReader(file));
			
			String line = "";

			while(line != null){
				System.out.println("loop!!");
				line = bf.readLine();

				if(line != null){
					JSONObject jsonOb = JSONObject.fromObject(line);
					
					String question = jsonOb.getString("question");
					question = afterClean(question, pattern, deDupSet, removeSet);
					if(!"".equalsIgnoreCase(question)){
						
						String title = jsonOb.getString("title");
						title = afterClean(title, pattern, deDupSet, removeSet);;
						
						JSONArray answersArray = jsonOb.getJSONArray("answers");
						JSONArray afterCleanArray = new JSONArray();
						for(int i=0; i<answersArray.size(); i++){
							System.out.println("answer:");
							String answer = answersArray.getString(i);
							answer = afterClean(answer, pattern, deDupSet, removeSet);;
							if(!"".equalsIgnoreCase(answer)){
								afterCleanArray.add(answer);
							}
						}
						if(afterCleanArray.size() > 0){
							jsonOb.put("title", title);
							jsonOb.put("question", question);
							jsonOb.put("answers", afterCleanArray);
							//context.write(new Text(jsonOb.toString()), new Text());
							System.out.println("result:valid");
						}
						
						else {
							System.out.println("result:invalid");
						}
					}
					
				}
				
			}
			bf.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public String afterClean(String sentence, Pattern pattern, HashSet<String> deDupSet, HashSet<String> removeSet){
		String afterCleanString = "";
		sentence = RemoveAnswerUtil.RemoveOnlyHasSymbolAndNum(sentence, pattern);
		System.out.println("1:"+sentence);
		if(!"".equalsIgnoreCase(sentence)){ // 如果全部字符都是符号或者数字就过滤掉
			afterCleanString = SymbolUtil.deDupFilterSymbol(sentence, deDupSet, removeSet); //去掉重复的符号
			System.out.println("3:"+afterCleanString);
			// 检查是否超过80％的字符都是外文，目前检查日文，韩文，阿拉伯文
			if(RemoveForeignTextUtil.checkJanpanese(sentence) || RemoveForeignTextUtil.checkKorean(sentence)
					|| RemoveForeignTextUtil.checkArabic(sentence)){
				System.out.println("4:");
				return "";
			}
			
		}
		System.out.println("2:"+sentence);
		return afterCleanString;
	}
	
	public static void main(String[] args) {
		System.out.println( "start test!" );
		TianyaExtractoreTest t = new TianyaExtractoreTest();
		//t.test();
		
		
		//t.testCommentToReply();
		
		//t.testReplytoReply();
		
		//t.testRemoveString();
		
		//t.testRemovequestion();
		//t.testClearQuestion();
		
		t.testRemove();
		
		String sentence = "急。寻高人解答";
		if(RemoveForeignTextUtil.checkJanpanese(sentence) || RemoveForeignTextUtil.checkKorean(sentence)
				|| RemoveForeignTextUtil.checkArabic(sentence)){
			System.out.println("why");
		}
		
	}
}
