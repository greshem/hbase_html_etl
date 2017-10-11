import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.bson.Document;

import com.petty.etl.extractor.ShuolianaiExtractor;
import com.petty.etl.extractor.TianyaBbsExtractor;
import com.petty.etl.extractor.ZhihuExtractor;
import com.petty.etl.factory.MongoDBFactory;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import net.sf.json.JSONObject;

public class ShuolianaiExtractoreTest {
	private ShuolianaiExtractor extractor = new ShuolianaiExtractor();
	private List<JSONObject> result;
	private static int count = 0;
	
	private void testextract() {
		String filename = "/Users/ldy/shoulianai.html";
		try {
			String html = readFile(filename);
			List<JSONObject> result = extractor.extractHbaseData(html,"","",null, null);
			
			for (JSONObject r : result) {
				System.out.println(r);
			}
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
	
	public static void main(String[] args) {
		System.out.println( "start test!" );
		ShuolianaiExtractoreTest t = new ShuolianaiExtractoreTest();
		//t.test();
		
		
		//t.testCommentToReply();
		
		t.testextract();
		
		//t.testRemoveString();
	}
}
