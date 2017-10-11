package com.petty.etl.commonUtils;

import java.util.HashSet;

import com.spreada.utils.chinese.ZHConverter;

public class SymbolUtil {

	public static void main(String[] args) {
		HashSet<String> set = FileUtil.readFile("/Users/greshem/codes/myprojects/corpusetl/files/symbol.txt");
		HashSet<String> setFilter = FileUtil.readFile("/Users/greshem/codes/myprojects/corpusetl/files/symbol_filter.txt");

		System.out.println(deDupFilterSymbol("终于等到开播啦!!!黄雀在哪里[抱抱]", set, setFilter));
		System.out.println(deDupFilterSymbol("직접야휴대전화번호?", set, setFilter));
		System.out.println(deDupFilterSymbol("=====^^^^==天气不好 ==你怎么样啊!!....", set, setFilter));
		System.out.println(deDupFilterSymbol("=====^^^^==天气不好 ==你怎么样啊？？？？？？？？，:::", set, setFilter));
		System.out.println(deDupFilterSymbol("＃＃＃＃＃＃＃＃＃＃＃＃＃＃＃＃＃＃“天气不好” ==你怎么样啊;;!!！！;;", set, setFilter));
		System.out.println(deDupFilterSymbol("｛天气不好｝ ==你怎么样啊\"哈哈\"\"", set, setFilter));
		System.out.println(deDupFilterSymbol("[第六集]天气不好，，，，，，，，，，＝＝＝＝＝＝＝＝＝＝＝＝＝＝你怎么样啊??????", set, setFilter));
		System.out.println(deDupFilterSymbol("％％％％％％……………………(☆_☆)％％％％2011年????????????????上推五百年!!!下寻一千年!!!!!!!!!!!!!!!!!!!!1楼主慢慢找吧!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1有木有???????????????、、!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", set, setFilter));
		
		System.out.println(TraToSim("餵小竹子音檔回應的東西怪怪的"));
		System.out.println(removeSymbol("我！是!你！妈", setFilter));
		
		
	}
	
	public static String deDupSymbol(String sentence, HashSet<String> set){
		String afterConvert = ToDBC(sentence).trim();
		String backStr =  new String();
		boolean flag = false;
		if(afterConvert.endsWith("?")){
			flag = true;
		}
		String afterDedup = "";
		for(String symbol: set){
			if("".equalsIgnoreCase(symbol)){
				continue;
			}
			if("+".equalsIgnoreCase(symbol) || "^".equalsIgnoreCase(symbol)
					|| ".".equalsIgnoreCase(symbol) || "$".equalsIgnoreCase(symbol)
					|| "(".equalsIgnoreCase(symbol) || ")".equalsIgnoreCase(symbol)
					|| "*".equalsIgnoreCase(symbol) || "[".equalsIgnoreCase(symbol)
					|| "{".equalsIgnoreCase(symbol) || "?".equalsIgnoreCase(symbol)
					|| "\"".equalsIgnoreCase(symbol) || "\\".equalsIgnoreCase(symbol)
					|| "|".equalsIgnoreCase(symbol)){
				afterConvert = afterConvert.replaceAll("\\" + symbol + "+", "\\" + symbol);
			}else{
				afterConvert = afterConvert.replaceAll(symbol + "+", symbol);
			}
		}
//		System.out.println(afterConvert);
		backStr = afterConvert;
		for(String symbol: set){
			if("[".equalsIgnoreCase(symbol)){
				afterConvert = afterConvert.replaceAll("\\" + symbol, " ");
			}else{
				afterConvert = afterConvert.replace(symbol, " ");
			}
		}
		afterConvert = trimRight(afterConvert);
		
		if(afterConvert.length() == 0){
			afterDedup = backStr;
		}else{
			afterDedup = backStr.substring(0, afterConvert.length());
		}
		if(flag){
			return afterDedup + "?";
		}else{
			return afterDedup;
		}
	}
	
	public static boolean hasLength(String str){
        return str != null && str.length() > 0;
    }
	
	public static String trimRight(String source){
        if(!hasLength(source))
            return source;
        if(source.trim().length()==0)
            return "";
        int index=0;
        for(int i=source.length()-1;i>=0;i--){
            if(Character.isWhitespace(source.charAt(i))){
                index=i;
            }else{
                break;
            }
        }
        return index!=0 ? source.substring(0,index): source;
    }
	
	public static String trimLeft(String source){
        if(!hasLength(source))
            return source;
        if(source.trim().length()==0)
            return "";
        int index=0;
        for(int i=0;i<source.length();i++){
            if(Character.isWhitespace(source.charAt(i))){
                index=i+1;
            }else{
                break;
            }
        }
        return index!=0 ? source.substring(index): source;
    }
	
	/**
     * 半角转全角
     * @param input String.
     * @return 全角字符串.
     */
    public static String ToSBC(String input) {
         char c[] = input.toCharArray();
         for (int i = 0; i < c.length; i++) {
           if (c[i] == ' ') {
             c[i] = '\u3000';
           } else if (c[i] < '\177') {
             c[i] = (char) (c[i] + 65248);
           }
         }
         return new String(c);
    }

    /**
     * 全角转半角
     * @param input String.
     * @return 半角字符串
     */
    public static String ToDBC(String input) {
         char c[] = input.toCharArray();
         for (int i = 0; i < c.length; i++) {
           if (c[i] == '\u3000') {
             c[i] = ' ';
           } else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
             c[i] = (char) (c[i] - 65248);
           }
         }
         String returnString = new String(c);
         return returnString;
    }
    
    public static String deDupFilterSymbol(String sentence, HashSet<String> set, HashSet<String> setFilter){
    	String afterConvert = ToDBC(sentence).trim();
		String backStr =  new String();
		boolean flag = false;
		if(afterConvert.endsWith("?")){
			flag = true;
		}
		String afterDedup = "";
		for(String symbol: set){
			if("".equalsIgnoreCase(symbol)){
				continue;
			}
			if("+".equalsIgnoreCase(symbol) || "^".equalsIgnoreCase(symbol)
					|| ".".equalsIgnoreCase(symbol) || "$".equalsIgnoreCase(symbol)
					|| "(".equalsIgnoreCase(symbol) || ")".equalsIgnoreCase(symbol)
					|| "*".equalsIgnoreCase(symbol) || "[".equalsIgnoreCase(symbol)
					|| "{".equalsIgnoreCase(symbol) || "?".equalsIgnoreCase(symbol)
					|| "\"".equalsIgnoreCase(symbol) || "\\".equalsIgnoreCase(symbol)
					|| "|".equalsIgnoreCase(symbol)){
				afterConvert = afterConvert.replaceAll("\\" + symbol + "+", "\\" + symbol);
			}else{
				afterConvert = afterConvert.replaceAll(symbol + "+", symbol);
			}
		}
		backStr = afterConvert;
//		System.out.println(afterConvert);
		for(String symbol: setFilter){
			if("[".equalsIgnoreCase(symbol)){
				afterConvert = afterConvert.replaceAll("\\" + symbol, " ");
			}else{
				afterConvert = afterConvert.replace(symbol, " ");
			}
		}
		afterConvert = trimLeft(afterConvert);
		if(afterConvert.length() == 0){
			afterDedup = backStr;
		}else{
			afterDedup = backStr.substring(backStr.length()-afterConvert.length(), backStr.length());
		}
		backStr = afterDedup;
		afterConvert = trimRight(afterConvert);
		if(afterConvert.length() == 0){
			afterDedup = backStr;
		}else{
			afterDedup = backStr.substring(0, afterConvert.length());
		}
		afterDedup = afterDedup.replaceAll(" +", " ");
		if(flag){
			return afterDedup + "?";
		}else{
			return afterDedup;
		}
    }
    
    /**
	 * 繁体转简体
	*
	 * @param tradStr
	 * 繁体字符串
	 * @return 简体字符串
	*/
	public static String TraToSim(String tradStr) {
		ZHConverter converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED);
		String simplifiedStr = converter.convert(tradStr);
		return simplifiedStr;
	}
	
	/**
	 * 简体转繁体
	*
	 * @param simpStr
	 * 简体字符串
	 * @return 繁体字符串
	*/
	public static String SimToTra(String simpStr) {
		ZHConverter converter = ZHConverter.getInstance(ZHConverter.TRADITIONAL);
		String traditionalStr = converter.convert(simpStr);
		return traditionalStr;
	}
	
	public static String removeSymbol(String sentence, HashSet<String> set){
		String afterRemove = ToDBC(sentence).trim();
		for(String symbol: set){
			if("".equalsIgnoreCase(symbol)){
				continue;
			}
			if("+".equalsIgnoreCase(symbol) || "^".equalsIgnoreCase(symbol)
					|| ".".equalsIgnoreCase(symbol) || "$".equalsIgnoreCase(symbol)
					|| "(".equalsIgnoreCase(symbol) || ")".equalsIgnoreCase(symbol)
					|| "*".equalsIgnoreCase(symbol) || "[".equalsIgnoreCase(symbol)
					|| "{".equalsIgnoreCase(symbol) || "?".equalsIgnoreCase(symbol)
					|| "\"".equalsIgnoreCase(symbol) || "\\".equalsIgnoreCase(symbol)
					|| "|".equalsIgnoreCase(symbol)){
				afterRemove = afterRemove.replaceAll("\\" + symbol + "+", "");
			}else{
				afterRemove = afterRemove.replaceAll(symbol + "+", "");
			}
		}
		return afterRemove.replaceAll(" +", "");
	}
}
