package com.petty.etl.commonUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;

public class FileUtil {

	public static void main(String[] args) {

	}
	
	public static HashSet<String> readFile(String filePath){
		HashSet<String> set = new HashSet<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String s = null;

			while ((s = br.readLine()) != null) {
				set.add(s);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return set;
	}
	
	public static HashMap<String, String> readFileToMap(String filePath, String separator){
		HashMap<String, String> map = new HashMap<String, String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String s = null;
			while ((s = br.readLine()) != null) {
				String[] array = s.split(separator);
				if(array.length == 2){
					map.put(array[0], array[1]);
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	
	public static String readFileToString(String filePath){
		StringBuilder builder = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String s = null;
			while ((s = br.readLine()) != null) {
				builder.append(s).append("\n");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
}
