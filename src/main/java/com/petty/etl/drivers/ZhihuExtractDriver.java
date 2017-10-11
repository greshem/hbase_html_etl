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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.petty.etl.mappers.ZhihuExtractMapper;

public class ZhihuExtractDriver {

	public static void main(String[] args) throws Exception {
		String input = null;
		String output = null;

		// conf.set("mapreduce.framework.name", "yarn");
		// conf.set("yarn.resourcemanager.address", "192.168.1.73:8032");
		// conf.set("yarn.resourcemanager.scheduler.address",
		// "192.168.1.73:8030");
		Configuration conf = new Configuration();

		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("input", "input", true, "input path");
			opts.addOption("output", "output", true, "output path");
			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			input = cl.getOptionValue("input");
			output = cl.getOptionValue("output");
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		final FileSystem filesystem = FileSystem.get(new URI(output), conf);
		if (filesystem.exists(new Path(output))) {
			filesystem.delete(new Path(output), true);
		}

		Job job = Job.getInstance(conf);
		job.setJobName("extract_zhihu");

		// conf.set("hadoop.client.ugi", "root,supergroup");

		job.setJarByClass(ZhihuExtractDriver.class);
		job.setMapperClass(ZhihuExtractMapper.class);

		job.setNumReduceTasks(10);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		// Output Text File
		// job.setOutputFormatClass(TextOutputFormat.class);
		// LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
