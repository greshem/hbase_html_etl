import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.bson.Document;

import com.petty.etl.extractor.JianfeiExtractor;
import com.petty.etl.extractor.ShuolianaiExtractor;
import com.petty.etl.extractor.TianyaBbsExtractor;
import com.petty.etl.extractor.ZhihuExtractor;
import com.petty.etl.factory.MongoDBFactory;
import com.petty.etl.parser.JianFeiParser;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import net.sf.json.JSONObject;

public class JianFeiExtractoreTest {
	private JianfeiExtractor extractor = new JianfeiExtractor();
	private List<JSONObject> result;
	private static int count = 0;
	
	private void testparser() {
		String filename = "/Users/ldy/testhtml/darenfocustext.html";
		//String filename = "/Users/ldy/testhtml/darenbigbox.html";
		//String filename = "/Users/ldy/testhtml/darenwrapc.html";
		//String filename = "/Users/ldy/testhtml/darenwrapper.html";
		try {
			String html = readFile(filename);
//			byte[] bytes = html.getBytes();
			String htmlUTF8 = new String(html.getBytes("ISO-8859-1"));
			System.out.println(html);
			System.out.println(htmlUTF8);
//			List<JSONObject> result = JianFeiParser.parseStar(html);
//						
//			for (JSONObject r : result) {
//				System.out.println(r);
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void testextract() {
		String filename = "/Users/ldy/testhtml/darenold.html";
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
		JianFeiExtractoreTest t = new JianFeiExtractoreTest();
		
		t.testparser();
	}
}
