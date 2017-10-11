package com.petty.etl.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;


public class TianyaBBSFillupReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		ArrayList<JSONObject> list = new ArrayList<JSONObject>();		
		HashSet<String> descriptionSet = new HashSet<String>();
		
		while (it.hasNext()) {
			try{
				String jsonStr = it.next().toString();
				JSONObject jsonOb = JSONObject.fromObject(jsonStr);
				list.add(jsonOb);
				
				String description = jsonOb.getString("description");
				JSONArray descriptionArray = JSONArray.fromObject(description);
				for(int i=0; i<descriptionArray.size(); i++){
					String des = String.valueOf(descriptionArray.get(i));
					if(!"".equalsIgnoreCase(des)){
						descriptionSet.add(String.valueOf(descriptionArray.get(i)));
					}
				}
				
			}catch(JSONException e){
				e.printStackTrace();
			}
		}

		try{
			for(int i=0; i<list.size(); i++){
				JSONObject jsonOb = list.get(i);
				
				JSONArray descriptionArray = new JSONArray();
				for(String description : descriptionSet){
					descriptionArray.add(description);
				}
				jsonOb.put("description", descriptionArray.toString());
				
				context.write(new Text(jsonOb.toString()), new Text());
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
		
	}
}
