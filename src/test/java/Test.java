import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.HttpUtils;
import com.petty.etl.constant.Constants;
import com.petty.etl.filter.EtlFilter;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class Test {

	public static void main(String[] args) throws JSONException, UnsupportedEncodingException {
//        File file = new File("/home/greshem/douban_test");
//        BufferedReader reader = null;
//        try {
//            System.out.println("以行为单位读取文件内容，一次读一整行：");
//            reader = new BufferedReader(new FileReader(file));
//            String tempString = null;
//            // 一次读入一行，直到读入null为文件结束
//            while ((tempString = reader.readLine()) != null) {
//            	List<JSONObject> result = EtlExtractor.extract(tempString);
//            	if(result == null){
//            		continue;
//            	}
//        		System.out.println(result.size());
//        		for(int i=0; i<result.size(); i++){
//        			JSONObject jsonOb = result.get(i);
//        			jsonOb.put("tags", "123123");
//        			long time = jsonOb.getLong("update_time");
//        			String date = CalendarUtil.Unix2Datetime(time, "yyyy-MM-dd");
//        			System.out.println(jsonOb.toString() + "         " + date);
//        		}
//            }
//            reader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (reader != null) {
//                try {
//                    reader.close();
//                } catch (IOException e1) {
//                }
//            }
//        }
		String text1="你还要我怎样!!!!!要我怎样，你才能。离得开！";
		String[] array1 = text1.split("。|！|!");
		for(String text : array1){
			if(!"".equalsIgnoreCase(text)){
				System.out.println(text);
			}
		}
		
    	String string = "有2015年圣诞节去香港的童鞋吗 短评";
    	System.out.println(string.substring(string.length()-3, string.length()));
    	System.out.println(string.substring(0, string.length()-3));
    	
    	HashSet<String> set = new HashSet<String>();
    	set.add("成都");
    	set.add("音乐");
    	
    	
    	JSONObject ob = new JSONObject();
    	JSONArray array = new JSONArray();
    	for(String tt: set){
    		array.add(tt);
    	}
		
		System.out.println(array);
		ob.put("1", array.toString());
		System.out.println(ob.getString("1"));
		JSONArray answerArray = JSONArray.fromObject(ob.getString("1"));
		for(int i=0; i<answerArray.size(); i++){
			System.out.println(answerArray.get(i));
		}
		System.out.println(ob.toString());
		
		ob.put("1", "1231");
		ob.put("source", "");
		System.out.println("========"+ob.toString());
		
		JSONObject o1 = JSONObject.fromObject(ob.toString());
		JSONObject o2 = JSONObject.fromObject(ob.toString());
		o1.put("source", "1-----------");
		System.out.println(o1.toString());
		System.out.println(o2.toString());
		System.out.println(ob.toString());
		JSONObject o4 = new JSONObject();
		o4.put(Constants.QUESTION, "{憨场封渡莩盗凤醛脯互 ---throw OverFlow();//抛出匿名对象------------------}");
		o4.put(Constants.ANSWERS, "");
		System.out.println(o4.toString());
		System.out.println(EtlFilter.class.getResource("").getPath());
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("你好，早");
		list.add("Hello, hey!!");
		System.out.println(list.toArray());
		
		ob.put("short", list.toArray());
		System.out.println(ob.getJSONArray("short"));
		System.out.println(ob.toString());
		
		System.out.println(toStringHex("0a0064695f310000"));

		System.out.println("历史上有没有超越时代的人?103	".trim());
		
		System.out.println("👶");
		String emoji = "👶";
		System.out.println(emoji);
        
        String str = "~\\(≧▽≦)/~";
        Pattern p = Pattern.compile("(http://|ftp://|https://|www){0,1}[^\u4e00-\u9fa5\\s]*?\\.(com|net|cn|me|tw|fr)[^\u4e00-\u9fa5\\s]*",Pattern.CASE_INSENSITIVE );   
        Matcher m = p.matcher(str);    
        Set<String> urlSet = new HashSet<String>();
        while(m.find()){  
        	urlSet.add(m.group());  
        }  
        for(String s: urlSet){
        	System.out.println("s: "+s);
        	str = str.replace(s, "").trim();
        }
        Set<String> symbolSet = FileUtil.readFile("/Users/greshem/codes/myprojects/corpusetl/files/symbol.txt");
        for(String s: symbolSet){
        	str = str.replace(s, " ").trim();
        }
        str = str.replaceAll(" +", " ").trim();
        System.out.println("str: "+str);
        
        boolean flag = Pattern.matches("^http://.*zhihu\\.com/question/.*", "http://m.zhihu.com/question/22978737/answer/48982732");
        System.out.println(flag);
        
        String word = "ラドクリフ、マラソン五輪代表に1万m出場にも含み";
        if(word.getBytes("shift-jis").length>=(2*word.length())){
        	System.out.println("日文字符");
        }else{
        	System.out.println("不全部是日文");
        }
        
        String tt = "分享好的(12)瘦腿健美操~一星期大腿减掉2CM~~(第2页)";
        Pattern pattern = Pattern.compile("(?<=\\()[^\\)]+");
        Matcher matcher = pattern.matcher(tt);
        String last = "";
        while(matcher.find()){
        	last = matcher.group();
        }
        System.out.println("-" + last + "-");
        
        
        String tt1 = "卡奖嘛？<i class=\"face face_1 icon_27\">[鼓掌]</i><i class=\"face face_1 icon_27\">[鼓掌]</i><i class=\"face face_1 icon_27\">[鼓掌]</i>今天 我要提前告诉你明";
        Pattern pattern1 = Pattern.compile("(<i.*?/i>)");
        Matcher matcher1 = pattern1.matcher(tt1);
        String last1 = "";
        while(matcher1.find()){
        	last1 = matcher1.group();
            tt1 = tt1.replace(last1, "");
        }
        System.out.println(tt1);
        
        
        JSONObject obj = new JSONObject();
        if(obj.isEmpty()){
        	System.out.println(111);
        }
        			
        String  line = "爵士主场球迷将比湖人球迷少?宗教原因不利主队因为摩门教的要求,周日将会有很多爵士的球迷不能到现场观看比赛,爵士老板米勒就是其中之一……网易体育5月11日消息你能想像在疯狂的盐湖城能量解决中心球馆、主场的球迷比客场的球迷少这样的情况吗?或许你会觉得不可思议,但在当地时间周日,也就是北京时间5月12日凌晨,爵士队可能会面临这样的窘境。据《犹他新闻》报道,宗教的问题将让可能让很多爵士球迷不会到现场观战。原文如下——自从2001年的季后赛以来,爵士队已经很久没有尝试过周日在主场打比赛,但在明天,他们将再次面临这样的情况。或许这样的事情在犹他州是很大的事情,但对于爵士队的球员和教练来说,却不得不面对。(译者注:犹他州是摩门教徒最大的聚集地,摩门教对信徒的一个要求,就是在周日这天避免做一切与宗教活动无关的事情。)“我觉得我的底限,”爵士主帅斯隆说,“就是无论在何时打比赛,都不应该有任何区别。”对于将在中午打比赛,斯隆也表示无所谓。“我觉得大部分球员可能更希望在下午打比赛,而不是晚上,”斯隆说,“这样你不用一整天都思前想后,你只需要吃完早餐之后就可以准备比赛了。”德隆·威廉姆斯也表示什么时间打比赛都无所谓。“什么时间都行,”威廉姆斯说,“即便是凌晨2点钟打球都行,我一定会在那里。”但是很多犹他州的球迷却不会出现在球场上,爵士老板米勒同样是摩门教徒,他将不会出现在平时的位置上。爵士队前锋布泽尔被问道会否担心到时候湖人的球迷比爵士的球迷还多,“我不担心这些问题,”布泽尔说,“我觉得我们还是能够得到足够的支持。”(本文来源:网易体育 作者:Jason)体育今日热点SPORTS.163.COM日本足球专家称杜伊是替罪羊,中国足球已经无药可救;而奥运方面39岁的前乒乓球国手将再次代表美国出战,梅西将来北京而罗比尼奥因伤缺席;NBA方面麦蒂表示为了总冠军愿意转会;米兰巨头戏称卡卡3亿才卖。 >>详细";
        System.out.println(line.length());
        int group = line.length() / 400;
        int rest = line.length() % 400;
        if(rest == 0){
        	group = group - 1;
        }
        System.out.println(group);
		for(int j=0; j<=group; j++){
			String tmpString = "";
			if(j == group){
				tmpString = line.substring(j*400, line.length());
			}else{
				tmpString = line.substring(j*400, (j+1)*400);
			}
			System.out.println(tmpString);
		}

		
	}
	
	public static String toStringHex(String s){ 
		byte[] baKeyword = new byte[s.length()/2]; 
		for(int i = 0; i < baKeyword.length; i++) { 
			try{ 
				baKeyword[i] = (byte)(0xff & Integer.parseInt(s.substring(i*2, i*2+2),16)); 
			}catch(Exception e){ 
				e.printStackTrace(); 
			}
		} 
		try{ 
			s = new String(baKeyword, "utf-8");
		}catch (Exception e1){ 
			e1.printStackTrace(); 
		} 
		return s; 
	} 

	
}

