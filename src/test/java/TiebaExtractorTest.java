import java.io.IOException;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.parser.TiebaParser;

import net.sf.json.JSONObject;

public class TiebaExtractorTest {

	public static void main(String[] args) throws IOException {
		String html = FileUtil.readFileToString("/Users/greshem/codes/myprojects/corpusetl/test.html");
		String url = "http://tieba.baidu.com/p/3861308307";
		TiebaParser te = new TiebaParser();
		@SuppressWarnings("static-access")
		JSONObject result = te.parse(html, url);
		System.out.println(result);

	}

}
