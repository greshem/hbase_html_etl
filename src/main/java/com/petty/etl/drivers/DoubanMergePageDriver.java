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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.petty.etl.mappers.DoubanMergePageMapper;
import com.petty.etl.reducers.DoubanMergePageReducer;

public class DoubanMergePageDriver{
	private static Configuration conf = null;
	static{
		conf = new Configuration(); 
	}

	public static void main(String[] args) throws Exception{
		String input = null;
		String output = null;

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
		
		// 删除现有的output文件夹
		final FileSystem filesystem = FileSystem.get(new URI(output), conf);  
        if(filesystem.exists(new Path(output))){  
            filesystem.delete(new Path(output), true);  
        }  
        
		Job job = Job.getInstance(conf);
		job.setJobName("merge_page_douban");
		
		job.setJarByClass(DoubanMergePageDriver.class); 
		job.setMapperClass(DoubanMergePageMapper.class);
		job.setReducerClass(DoubanMergePageReducer.class);
		
		job.setNumReduceTasks(10); 
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
