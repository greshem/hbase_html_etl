package com.petty.etl.mappers;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.petty.etl.constant.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class DecreceDescriptionMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			JSONObject lineOb = JSONObject.fromObject(line);
			HashSet<String> hash = new HashSet<String>();

			Object description = lineOb.get(Constants.DESCRIPTION);
			hash = getDesc(description);
			lineOb.put(Constants.DESCRIPTION, hash.toArray());

			context.write(new Text(lineOb.toString()), new Text());
		} catch (Exception e) {
			System.out.println("Line: " + value.toString());
		}
	}

	public static HashSet<String> getDesc(Object lineObj) {

		HashSet<String> hash = new HashSet<String>();
		try {
			JSONArray objs = JSONArray.fromObject(lineObj);
			for (Object obje : objs) {

				if (obje instanceof String) {
					hash.add(obje.toString());
				} else if(obje instanceof JSONArray) {
					hash = getDesc(obje);
				}
			}
		} catch (JSONException e) {
			hash.add(lineObj.toString());
		}
		return hash;
	}

	public static void main(String[] args) {

		Object line = "[[[\"\"],\"本人做开发然坏了，现在做自己挣钱了再自己买，但如果这有高人强烈推荐，还是会考虑的 2.商务本，因为开发用嘛，显卡直接集成，能看就好，没什么游戏要求 3.内存尽量大吧，我看到的这个价位一般都是2G，有个别4G的，希望在可选范围内尽可能大 4.硬盘大小不要求，但是缓存和转速还是尽量高端一点 5.CPU我不太懂，"
				+ "希望能应付开发 6.多吧，可能有点苛刻了，我主要现在没时间挑了，如果电脑修不好，需要马上入手，所以需要各位购机达人和开发达人的帮助，在此谢过了\"]]";
		HashSet<String> hash = null;
		hash = getDesc(line);
		System.out.println(hash.toString());
	}

}
