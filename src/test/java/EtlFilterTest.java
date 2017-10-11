import java.net.URI;
import java.util.HashSet;
import java.util.regex.Pattern;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.filter.EtlFilter;

import net.sf.json.JSONObject;

public class EtlFilterTest {

	public static void main(String[] args) throws Exception {
		EtlFilter ef = new EtlFilter();
		// String jstringweibo =
		// "{\"title\":\"\",\"question\":\"……你最好了!\",\"answers\":[{\"content\":\"记到你说的话哦,不要到时候不甩我就是啦!早点睡,不要乱想,一定要记住,对自己好一点!\",\"likecount\":\"0\"}],\"description\":\"暴雨暴雨……今夜的雨淋得很舒坦……跟朋友说想喝酒了,很能喝……看到酒的一瞬间便没有了兴致……我对不起你们我夸大口了……看你们醉了醉了疯了疯了………………我就去淋雨了\",\"url\":\"http://m.weibo.cn/single/rcList?format=cards&id=5608829656900604032&type=comment&hot=0&page=1\",\"source\":\"102\",\"id\":\"10945032524\"}";
		String jstring = "{\"title\":\"\",\"question\":\"钢铁侠3开头曲 就是电影开头回忆时候 1999年的时候在哪个酒店放\",\"answers\":[{\"content\":\"找到了。口哨与小狗 儿童古典音乐欣赏 普赖克\",\"select\":0}],\"description\":\"有些事情,你以为明天还要继续,有些人还会再见面。因为你以为昨天今天明天,应该是没有什么不同的,但是就会有那么一次,在你一放手或一转身的瞬间,有些事情就完完全全的改变了。太阳落下去,在它重新升起之前,有些人,就从此和你永别了!所以能把握的现在的才是最真实的。\",\"tags\":[],\"url\":\"http://m.weibo.cn/single/rcList?format=cards&id=5602395164537523525&type=comment&hot=0&page=1\",\"source\":\"102\",\"id\":\"10034379126\",\"incre_flag\":1,\"uuid\":\"afc0c728-2809-498e-bc50-e6c3eb6efa02\",\"latest\":1}";

		URI[] uriArray = new URI[1];
//		uriArray[0] = new URI(
//				"/Users/greshem/Documents/corpusetl/src/main/java/com/petty/etl/filter/DeleteTotal.txt");
//		uriArray[1] = new URI(
//				"/Users/greshem/Documents/corpusetl/src/main/java/com/petty/etl/filter/ReplacePart.txt");
		 uriArray[0] = new URI("/Users/greshem/Downloads/rules/digital.txt");
		 HashSet<String>  symbolSet = FileUtil.readFile("/Users/greshem/Downloads/rules/symbol.txt");

		Pattern pd = null;
		pd = ef.compilePattern(uriArray);
		// System.out.println(pd.pattern());
//		Pattern pr = ef.compileReplaceRegexPattern(uriArray);
//		JSONObject obj = ef.filterNewRule(jstring, pd, "http://192.168.1.53:9500/json?f=ner_person");
		
		JSONObject obj = ef.filter(jstring, pd, "", symbolSet, true);
		System.out.println(obj);

		// Boolean t = ef.isFilterNeeded(pd, "[挖鼻屎]看来我练习意念控制别人出成效了");
		// System.out.println(t);
		//
		// Pattern p2 =
		// ef.compileRegexPattern("/Users/greshem/Documents/corpusetl/src/main/java/com/petty/etl/filter/zhihurules.txt");
		// JSONObject result2 = ef.filterZhihu(jstring, p2);
		// System.out.println(result2);
	}
}
