import java.util.List;

import org.bson.Document;

import com.petty.etl.extractor.ZhihuExtractor;
import com.petty.etl.factory.MongoDBFactory;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import net.sf.json.JSONObject;

public class ZhihuExtractorWithCommentsTest {
	private ZhihuExtractor extractor = new ZhihuExtractor();
	private List<JSONObject> result;
	private static int count = 0;

	private void test() {
		Long sTime = System.currentTimeMillis();
		MongoDatabase src_db = MongoDBFactory.getClientInstance("192.168.1.50", 27017, "pm");
		FindIterable<Document> iterable = src_db.getCollection("html").find().limit(2000);

		iterable.forEach(new Block<Document>() {

			public void apply(final Document document) {
				result = extractor.extract(document.toJson());
				if (result != null) {
					System.out.println("Final Results:" + result.toString() + "\n");
					count++;
				}
			}
		});

		 Long eTime = System.currentTimeMillis();
		 System.out.println("time:" + (eTime - sTime) / 1000);
	}

	private void testExtractComments() {

		String line = "{\"title\":\"\",\"question\":\"会不会楼主也是不怎么说话的人，吃饭没话题了\",\"answers\":[{\"content\":\"不对 她会感觉她才是第三者…\",\"liked\":false,\"href\":\"/r/answers/10134178/comments/66792127\",\"inReplyToUser\":null,\"author\":{\"isSelf\":false,\"bio\":\"\",\"name\":\"不賴DENG\",\"url\":\"http://www.zhihu.com/people/bu-lai-deng\",\"meta\":{\"isAnswerAuthor\":false,\"isQuestionCreator\":false},\"avatar\":{\"id\":\"d367cabccac969a832bbe94df834fe79\",\"template\":\"https://pic2.zhimg.com/{id}_{size}.jpg\"},\"slug\":\"bu-lai-deng\"},\"createdTime\":\"2015-01-09T18:44:28+08:00\",\"inReplyToCommentId\":86792127,\"own\":false,\"id\":66792127,\"likesCount\":0},{\"content\":\"她才是第三者…\",\"liked\":false,\"href\":\"/r/answers/10134178/comments/66792127\",\"inReplyToUser\":null,\"author\":{\"isSelf\":false,\"bio\":\"\",\"name\":\"不賴DENG\",\"url\":\"http://www.zhihu.com/people/bu-lai-deng\",\"meta\":{\"isAnswerAuthor\":false,\"isQuestionCreator\":false},\"avatar\":{\"id\":\"d367cabccac969a832bbe94df834fe79\",\"template\":\"https://pic2.zhimg.com/{id}_{size}.jpg\"},\"slug\":\"bu-lai-deng\"},\"createdTime\":\"2015-01-09T18:44:28+08:00\",\"inReplyToCommentId\":0,\"own\":false,\"id\":86792127,\"likesCount\":0}],\"description\":[\"真的是不放下手机啊……\"],\"tags\":[\"手机\",\"男朋友\"],\"url\":[\"https://www.zhihu.com/question/27301485\",\"https://www.zhihu.com/r/answers/10134178/comments\"],\"source\":\"103\",\"update_time\":\"1453415671636\",\"comment_id\":\"10134178\"}";
		result = extractor.extractComments(line);
		System.out.println(result.toString());
	}

	public static void main(String[] args) {
		System.out.println("start test!");
		ZhihuExtractorWithCommentsTest t = new ZhihuExtractorWithCommentsTest();
		t.testExtractComments();
		t.test();
		System.out.println("count:" + ZhihuExtractorWithCommentsTest.count);
	}
}
