package com.petty.etl.drivers;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.petty.etl.commonUtils.CalendarUtil;
import com.petty.etl.hbase.tableinputformat.MultiRangeTableInputFormat;
import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

public class HbaseDataDriver {

	private static Configuration config = null;

	static {
		config = HBaseConfiguration.create();
		config = HBaseConfiguration.addHbaseResources(config);
	}

	public static void main(String[] args) throws Exception {
		String output = null;
		String serverAddress = null;
		String tableName = null;
		String startKey = null;
		String endKey = null;
		String jobName = null;
		String picConvert = null;
		String zkServer = null;
		String mapper = null;

		// parse command args
		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(config, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();

			opts.addOption("output", "output", true, "output path");
			opts.addOption("serverAddress", "serverAddress", true, "serverAddress");
			opts.addOption("tableName", "tableName", true, "tableName");
			opts.addOption("startKey", "startKey", true, "startKey");
			opts.addOption("endKey", "endKey", true, "endKey");
			opts.addOption("jobName", "jobName", true, "jobName");
			opts.addOption("zkserver", "zkserver", true, "zkserver");
			opts.addOption("picConvert", "picConvert", true, "picConvert");
			opts.addOption("mapper", "mapper", true, "mapper class");

			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			output = cl.getOptionValue("output");
			serverAddress = cl.getOptionValue("serverAddress");
			tableName = cl.getOptionValue("tableName");
			startKey = cl.getOptionValue("startKey");
			endKey = cl.getOptionValue("endKey");
			jobName = cl.getOptionValue("jobName");
			picConvert = cl.getOptionValue("picConvert");
			zkServer = cl.getOptionValue("zkserver");
			mapper = cl.getOptionValue("mapper");

		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		// 删除现有的output文件夹
		final FileSystem filesystem = FileSystem.get(new URI(output), config);
		if (filesystem.exists(new Path(output))) {
			filesystem.delete(new Path(output), true);
		}

		if (zkServer != null) {
			System.out.println("serverAddress:\t" + zkServer);
			config.set("hbase.zookeeper.quorum", zkServer);
		}
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.setLong("hbase.rpc.timeout", 60000);
		config.setLong("hbase.client.scanner.caching", 1000);
		config.setLong("hbase.client.keyvalue.maxsize", 524288000);
		config.set("hbase.coprocessor.user.region.classes",
				"org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
		config.set("mapreduce.framework.name", "yarn");
		config.set("yarn.resourcemanager.address", serverAddress + ":8032");
		config.set("yarn.resourcemanager.scheduler.address", serverAddress + ":8030");

		Scan scan = new Scan();
		scan.setCaching(100); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		long startKeyTime = CalendarUtil.Datetime2Unix(startKey, "yyyyMMddHHmmss");
		System.out.println("startKeyTime: " + startKeyTime);
		if (endKey != null) {
			long endKeyTime = CalendarUtil.Datetime2Unix(endKey, "yyyyMMddHHmmss");
			System.out.println("endKeyTime: " + endKeyTime);
			config.set("region.scan.start", String.valueOf(startKeyTime));
			config.set("region.scan.stop", String.valueOf(endKeyTime));
		}

		config.set("table", tableName);
		config.set("jobname", jobName);

		try {
			Job job = Job.getInstance(config);
			job.setJobName("extract_Hbase_" + jobName);
			job.setJarByClass(HbaseDataDriver.class);
			if (picConvert != null && !"".equalsIgnoreCase(picConvert)) {
				job.addCacheFile(new Path(picConvert).toUri());
			}
			job.setNumReduceTasks(10);
			Class mapperClass = Class.forName(mapper);
			TableMapReduceUtil.initTableMapperJob(tableName, // input table
					scan, // Scan instance to control CF and attribute selection
					mapperClass, // mapper class
					Text.class, // mapper output key
					Text.class, // mapper output value
					job, true, MultiRangeTableInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(output));
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			int result = job.waitForCompletion(true) ? 0 : 1;
			DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
			
			lzoIndexer.setConf(config);
			lzoIndexer.run(new String[] { output });
			System.exit(result);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
