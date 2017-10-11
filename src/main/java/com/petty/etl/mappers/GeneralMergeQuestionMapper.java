package com.petty.etl.mappers;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;


public class GeneralMergeQuestionMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private HashSet<String> set;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		URI[] uriArray = context.getCacheFiles();
		for(int i=0; i<uriArray.length; i++){
			Path uriPath = new Path(uriArray[i].getPath());
			String filename = uriPath.getName().toString();
			if(filename.contains("symbol.txt")){
				set = FileUtil.readFile(filename);
			}
		} 
	}
	
	@Override	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = value.toString();
			if(line != null){
				JSONObject jsonOb = new JSONObject(line);
				String question = jsonOb.getString(Constants.QUESTION);
				String title = jsonOb.getString(Constants.TITLE);
				if(question != null && !"".equalsIgnoreCase(question) && title != null && !"".equalsIgnoreCase(title)){
					if(question.endsWith(" 短评")){
						question = question.substring(0,question.length() - 3).trim();
					}else if(question.startsWith("回应: ")){
						question = question.substring(4,question.length()).trim();
					}else if(question.startsWith("转:")){
						question = question.substring(2,question.length()).trim();
					}
					if(title.endsWith(" 短评")){
						title = title.substring(0,title.length() - 3).trim();
					}else if(title.startsWith("回应: ")){
						title = title.substring(4,title.length()).trim();
					}else if(question.startsWith("转:")){
						title = title.substring(2,title.length()).trim();
					}
					question = SymbolUtil.deDupSymbol(question, set);
					title = SymbolUtil.deDupSymbol(title, set);
					jsonOb.put(Constants.QUESTION, question);
					jsonOb.put(Constants.TITLE, title);
					context.write(new Text(title + question), new Text(jsonOb.toString()));
				}
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
}
