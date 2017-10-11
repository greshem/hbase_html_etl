package com.petty.etl.filter;

import java.util.HashSet;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

public abstract class BaseFilter {
	/**
	 * 
	 * @param data: input string
	 * @param pd:   pattern to delete the total input
	 * @param pr:   pattern to replace the matched part with empty only
	 * @return
	 */
	public abstract JSONObject filter(String data, Pattern pd, String url, HashSet<String> symbolSet, boolean flag);
}
