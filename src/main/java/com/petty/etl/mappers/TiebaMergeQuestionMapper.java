package com.petty.etl.mappers;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.petty.etl.commonUtils.FileUtil;
import com.petty.etl.commonUtils.SymbolUtil;


public class TiebaMergeQuestionMapper extends Mapper<Text, NullWritable, Text, Text>{
	
	private String path;
	private HashSet<String> set;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		path = context.getConfiguration().get("symbollist");
		set = FileUtil.readFile(path);
	}
	
	@Override	
	public void map(Text key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		try{
			String line = key.toString();
			if(line != null){
				JSONObject jsonOb = new JSONObject(line);
				String question = jsonOb.getString("question");
				if(question != null && !"".equalsIgnoreCase(question)){					
					question = SymbolUtil.deDupSymbol(question, set);
					context.write(new Text(question), new Text(jsonOb.toString()));
				}
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
	}
	
//	@Override
//	protected void cleanup(Context context) throws IOException,InterruptedException {
//		super.setup(context);
//		mos.close();
//	}
}
