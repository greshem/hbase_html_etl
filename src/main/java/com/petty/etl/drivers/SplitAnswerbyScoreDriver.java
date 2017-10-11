package com.petty.etl.drivers;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
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

import com.petty.etl.mappers.QAScoreSampleMapper;

public class SplitAnswerbyScoreDriver {

	private static Configuration conf = null;

	static {
		conf = new Configuration();
	}

	public static void main(String[] args) throws Exception {
		String input = null;
		String output = null;
		String minScore = null;
		String maxScore = null;

		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("minScore", "minScore", true, "split answers by downlimit score");
			opts.addOption("maxScore", "maxScore", true, "split answers by uplimit score");
			opts.addOption("input", "input", true, "input path");
			opts.addOption("output", "output", true, "output path");

			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			minScore = cl.getOptionValue("minScore");
			maxScore = cl.getOptionValue("maxScore");
			if (Float.parseFloat(minScore) < 0 || Float.parseFloat(maxScore) > 1) {
				throw new Exception("Specify the score between 0 and 1.");
			}
			if (Float.parseFloat(minScore) > Float.parseFloat(maxScore)) {
				throw new Exception("Specify the minScore smaller than maxScore ");
			}
			conf.setFloat("minScore", Float.parseFloat(minScore));
			conf.setFloat("maxScore", Float.parseFloat(maxScore));
			input = cl.getOptionValue("input");
			output = cl.getOptionValue("output");
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		Job job = Job.getInstance(conf);
		job.setJobName("SplitQAbyTaggedScore");
		job.setJarByClass(SplitAnswerbyScoreDriver.class);
		job.setMapperClass(QAScoreSampleMapper.class);

		job.setNumReduceTasks(10);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
