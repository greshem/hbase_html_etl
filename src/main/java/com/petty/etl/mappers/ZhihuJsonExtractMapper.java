package com.petty.etl.mappers;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.petty.etl.commonUtils.SymbolUtil;
import com.petty.etl.constant.Constants;
import com.petty.etl.extractor.ZhihuExtractor;

import net.sf.json.JSONObject;

public class ZhihuJsonExtractMapper extends TableMapper<Text, Text> {

	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (value != null) {
			String url = getValue("url", "url", value);
			String html = getValue("html_body", "html", value);
			String updateTime = getValue("update_time", "time", value);
			String tag = getValue("tag", "tag", value);

			ZhihuExtractor extractor = new ZhihuExtractor();
			List<JSONObject> result = extractor.extractHbaseData(html, updateTime, url, tag, null);

			if (result != null) {
				for (int i = 0; i < result.size(); i++) {
					JSONObject jsonOb = result.get(i);
					String simple = SymbolUtil.TraToSim(jsonOb.toString());
					if (jsonOb.get(Constants.COMMENTID) != null
							&& !"".equalsIgnoreCase(jsonOb.getString(Constants.COMMENTID))) {
						
						mos.write(new Text(simple), new Text(), "zhihu_comments/part");
					} else {
						mos.write(new Text(simple), new Text(), "zhihu_origin/part");
					}
				}
			}

		}
	}

	public static String getValue(String columnFamily, String qualifier, Result cellValue) {
		String value = null;
		byte[] byteAarry = cellValue.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
		if (byteAarry != null) {
			value = new String(byteAarry);
		}
		return value;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos.close();
	}
}
