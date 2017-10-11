package com.petty.etl.commonUtils;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

public class RemoveForeignTextUtil {

	private final static Double THRESHOLD = 0.8;
	
	public static void main(String[] args) {
		String japanese = "ラドクリフ、マラソン五輪代表に1万m出場にも含み";
        System.out.println(checkJanpanese(japanese));
        String korean = "직접야휴대전화번호?";
        System.out.println(checkKorean(korean));
        String arabic = "؟ىودعاست نأهكمم";
        System.out.println(checkArabic(arabic));
	}

	public static boolean checkJanpanese(String sentence){
		boolean needFilter = false;
		// write your code here
        Set<Character.UnicodeBlock> japaneseUnicodeBlocks = new HashSet<Character.UnicodeBlock>(){{
                add(Character.UnicodeBlock.HIRAGANA);
                add(Character.UnicodeBlock.KATAKANA);
                add(Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS);
            }};
        int janpaneseLen = 0;
        for (char c : sentence.toCharArray()){
            if (japaneseUnicodeBlocks.contains(Character.UnicodeBlock.of(c))){
            	janpaneseLen += String.valueOf(c).length();
            }
        }
        BigDecimal decimal = new BigDecimal(janpaneseLen).divide(new BigDecimal(sentence.length()), 4, BigDecimal.ROUND_HALF_DOWN);
//        System.out.println(janpaneseLen);
//        System.out.println(sentence.length());
//        System.out.println(decimal);
        if(decimal.compareTo(BigDecimal.valueOf(THRESHOLD)) >= 0){
        	needFilter = true;
        }
        return needFilter;
	}
	
	public static boolean checkKorean(String sentence){
		boolean needFilter = false;
		// write your code here
        Set<Character.UnicodeBlock> koreanUnicodeBlocks = new HashSet<Character.UnicodeBlock>(){{
                add(Character.UnicodeBlock.HANGUL_COMPATIBILITY_JAMO);
                add(Character.UnicodeBlock.HANGUL_JAMO);
                add(Character.UnicodeBlock.HANGUL_JAMO_EXTENDED_A);
                add(Character.UnicodeBlock.HANGUL_JAMO_EXTENDED_B);
                add(Character.UnicodeBlock.HANGUL_SYLLABLES);
            }};
        int koreanLen = 0;
        for (char c : sentence.toCharArray()){
            if (koreanUnicodeBlocks.contains(Character.UnicodeBlock.of(c))){
            	koreanLen += String.valueOf(c).length();
            }
        }
        BigDecimal decimal = new BigDecimal(koreanLen).divide(new BigDecimal(sentence.length()), 4, BigDecimal.ROUND_HALF_DOWN);
//        System.out.println(koreanLen);
//        System.out.println(sentence.length());
//        System.out.println(decimal);
        if(decimal.compareTo(BigDecimal.valueOf(THRESHOLD)) >= 0){
        	needFilter = true;
        }
        return needFilter;
	}
	
	public static boolean checkArabic(String sentence){
		boolean needFilter = false;
		// write your code here
        Set<Character.UnicodeBlock> arabicUnicodeBlocks = new HashSet<Character.UnicodeBlock>(){{
                add(Character.UnicodeBlock.ARABIC);
                add(Character.UnicodeBlock.ARABIC_PRESENTATION_FORMS_A);
                add(Character.UnicodeBlock.ARABIC_PRESENTATION_FORMS_B);
                add(Character.UnicodeBlock.ARABIC_SUPPLEMENT);
            }};
        int arabicLen = 0;
        for (char c : sentence.toCharArray()){
            if (arabicUnicodeBlocks.contains(Character.UnicodeBlock.of(c))){
            	arabicLen += String.valueOf(c).length();
            }
        }
        BigDecimal decimal = new BigDecimal(arabicLen).divide(new BigDecimal(sentence.length()), 4, BigDecimal.ROUND_HALF_DOWN);
//        System.out.println(arabicLen);
//        System.out.println(sentence.length());
//        System.out.println(decimal);
        if(decimal.compareTo(BigDecimal.valueOf(THRESHOLD)) >= 0){
        	needFilter = true;
        }
        return needFilter;
	}
}
