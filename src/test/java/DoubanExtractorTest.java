import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.bson.Document;

import com.petty.etl.extractor.DoubanExtractor;
import com.petty.etl.extractor.WeiboExtractor;
import com.petty.etl.factory.MongoDBFactory;
import com.petty.etl.parser.DoubanParser;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import net.sf.json.JSONObject;

public class DoubanExtractorTest {
	
	private void testNullTitleIssue() {
		
		try {
			String html = readFile();
			
			JSONObject question = DoubanParser.groupParse(html);
			System.out.println(question.toString());			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
	public static String readFile() throws IOException{
		String filename = "/Users/ldy/douban.html";
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
		DoubanExtractorTest test = new DoubanExtractorTest();
		test.testNullTitleIssue();
	}
}
