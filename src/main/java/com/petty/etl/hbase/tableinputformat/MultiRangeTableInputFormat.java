package com.petty.etl.hbase.tableinputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public class MultiRangeTableInputFormat extends TableInputFormat{
	
	private HTable table = null;
	private Configuration conf = null;
	
	@Override 
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		conf = context.getConfiguration();
		
		String tableName = conf.get("table");
		System.out.println("table:\t" + tableName);
		table = new HTable(conf, tableName);
		if (table == null) {
			throw new IOException("No table was provided.");
		}
		// Get the name server address and the default value is null.
		String scanStart = conf.get("region.scan.start");
		String scanStop = conf.get("region.scan.stop");
		
		System.out.println("scanStart:\t" + scanStart);
		System.out.println("scanStop:\t" + scanStop);
		 
		Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			throw new RuntimeException("At least one region is expected");
		}
		List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
		for (int i = 0; i < keys.getFirst().length; i++) {

			String regionLocation = getTableRegionLocation(table, keys.getFirst()[i]);
			String regionSalt = null;
			if (keys.getFirst()[i].length == 0) {
				regionSalt = "00";
			}else{
				regionSalt = Bytes.toString(keys.getFirst()[i]).substring(0, 2);
			}
			byte[] startRowKey = Bytes.toBytes(regionSalt + scanStart);
			byte[] endRowKey = Bytes.toBytes(regionSalt + scanStop);
			 
			InputSplit split = new TableSplit(table.getName(), startRowKey, endRowKey, regionLocation);
			splits.add(split);
		}
		return splits;
	}
	
	public String getTableRegionLocation(HTable table, byte[] rowKey) throws IOException {
		String regionLocation = table.getRegionLocation(rowKey).getHostname();
		return regionLocation;
	}
}
