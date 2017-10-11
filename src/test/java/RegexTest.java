import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexTest {

	public static void main(String[] args) {
		String string = "哈我好的，你好的";

		//String regex = "(\\d+.\\d+|\\d+)";
		//String regex = "[^(你好|我好)]";
		String regex = "(?<!^)(我好)";
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m = pattern.matcher(string);
	//	System.out.println(pattern.pattern());
		while (m.find()) {
			System.out.println(m.group());
		}
		
		isMentionAge("我今年是101岁");
		isMentionAge("我今年是一五岁");
	}
	
	private static final Pattern[] MENTIONAGE = {
            Pattern.compile("是*(\\D[0-9|一|二|三|四|五|六|七|八|九|十]{2}|[0-9|一|二|三|四|五|六|七|八|九|十]{4}|[0-9|一|二|三|四|五|六|七|八|九|十]{6})岁"),
//            Pattern.compile("[已经|已|现在]?[0-9|一|二|三|四|五|六|七|八|九|十|千]{2|4}了"),
//            Pattern.compile("是?[0-9|零|一|二|三|四|五|六|七|八|九|两|千]{2|4}[年的|生的|的]"),
//            Pattern.compile("[0-9|零|一|二|三|四|五|六|七|八|九|十]{2|4}"),
    };

    public static boolean isMentionAge(String text) {
        for (Pattern pattern : MENTIONAGE){
        	Matcher m = pattern.matcher(text);
            if (m.find()){
            	System.out.println(m.group());
                return true;
            }
        }    
        return false;
    }
    
}
