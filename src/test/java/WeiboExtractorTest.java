import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.petty.etl.extractor.WeiboExtractor;

public class WeiboExtractorTest {
	
	public static void main(String[] args) {
		WeiboExtractor extractor = new WeiboExtractor();
		try {
			String html = readFile();
			
			String question = extractor.getQuestionFromHTML(html);
			System.out.println(question);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static String readFile() throws IOException{
		String filename = "/Users/ldy/source_html.html";
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
	
}
