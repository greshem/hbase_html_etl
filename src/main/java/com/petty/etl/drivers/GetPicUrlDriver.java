package com.petty.etl.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.petty.etl.mappers.GetPicUrlMapper;
import com.petty.etl.reducers.GetPicUrlReducer;

public class GetPicUrlDriver {

	private static Configuration conf = null;
	
	public static void connToHbase(String server) {
		Configuration customConf = new Configuration();
		customConf.set("hbase.zookeeper.quorum", server);
		customConf.set("hbase.zookeeper.property.clientPort", "2181");
		customConf.setLong("hbase.rpc.timeout", 60000);
		customConf.setLong("hbase.client.scanner.caching", 1000);
		customConf.setLong("hbase.client.keyvalue.maxsize",524288000);
		customConf.set("hbase.coprocessor.user.region.classes",
				"org.apache.hadoop.hbase.coprocessor.AggregateImplementation");

		conf = HBaseConfiguration.create(customConf);
		System.out.println(conf.get("hbase.zookeeper.quorum"));
	}
	
	public static void main(String[] args) {
		if (args.length != 4){
            System.err.printf("Usage: %s <hbase server> <table name> <limit> <output>",
            		GetPicUrlDriver.class.getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(1);             
		}
		connToHbase(args[0]);
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("get_pic_url_hbase");
			job.setJarByClass(GetPicUrlDriver.class);
			
			Scan scan = new Scan();
//			String startKey = args[2];
//			String endKey = args[3];
//			scan.setStartRow(Bytes.toBytes(startKey));
//			scan.setStopRow(Bytes.toBytes(endKey)); 
			scan.setFilter(new PageFilter(Integer.parseInt(args[2])));
			scan.setCaching(100);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs

			TableMapReduceUtil.initTableMapperJob(
					  args[1],        // input table
					  scan,               // Scan instance to control CF and attribute selection
					  GetPicUrlMapper.class,     // mapper class
					  Text.class,         // mapper output key
					  IntWritable.class,  // mapper output value
					  job);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(1);
			job.setReducerClass(GetPicUrlReducer.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(args[3]));
			
			boolean b = job.waitForCompletion(true);
			if (!b) {
			  throw new IOException("error with job!");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
