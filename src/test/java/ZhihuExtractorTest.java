import java.util.List;

import net.sf.json.JSONObject;

import org.bson.Document;

import com.petty.etl.extractor.ZhihuExtractor;
import com.petty.etl.factory.MongoDBFactory;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class ZhihuExtractorTest {
	private ZhihuExtractor extractor = new ZhihuExtractor();
	private List<JSONObject> result;
	private static int count = 0;

	private void test() {
		// TODO Auto-generated method stub
		Long sTime = System.currentTimeMillis();
		MongoDatabase src_db = MongoDBFactory.getClientInstance("192.168.1.22",
				27017, "zhihu");
		FindIterable<Document> iterable = src_db.getCollection("html").find()
				.limit(800);

		iterable.forEach(new Block<Document>() {

			public void apply(final Document document) {
				result = extractor.extract(document.toJson());
				if (result != null) {
					System.out.println(result.toString());
					count++;
				}
			}
		});

		Long eTime = System.currentTimeMillis();
		System.out.println("time:" + (eTime - sTime) / 1000);
	}

	public static void main(String[] args) {
		System.out.println("start test!");
		ZhihuExtractorTest t = new ZhihuExtractorTest();
		t.test();
		System.out.println("count:" + ZhihuExtractorTest.count);
	}
}
