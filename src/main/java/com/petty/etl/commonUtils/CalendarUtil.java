package com.petty.etl.commonUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CalendarUtil {
	
	public static void main(String[] args){
		System.out.println(Unix2Datetime(1466006400000L, "yyyy-MM-dd  HH:mm:ss"));
		System.out.println(Datetime2Unix("20160126000000", "yyyyMMddHHmmss"));
		System.out.println(Datetime2Unix("20160127000000", "yyyyMMddHHmmss"));
	}
	
	public static String Unix2Datetime(long time, String format){
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		String date = sdf.format(time);	
		return date;
	}
	
	public static long Datetime2Unix(String datetime, String format){
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		long unixtime = 0L;
		try {
			unixtime = sdf.parse(datetime).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return unixtime;
	}
}
