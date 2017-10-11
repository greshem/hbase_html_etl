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

public class CallHttpServiceDriver{

	public static void main(String[] args) throws Exception{
		String input = null;
		String output = null;
		String url = null;
		String mapper = null;
		
		Configuration conf = new Configuration(); 
		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("input", "input", true, "input path");
			opts.addOption("output", "output", true, "output path");
			opts.addOption("url", "url", true, "web service entry");
			opts.addOption("mapper", "mapper", true, "map class name");

			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			input = cl.getOptionValue("input");
			output = cl.getOptionValue("output");
			url = cl.getOptionValue("url");
			mapper = cl.getOptionValue("mapper");
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		conf.set("url", url);
		final FileSystem filesystem = FileSystem.get(new URI(output), conf);  
        if(filesystem.exists(new Path(output))){  
            filesystem.delete(new Path(output), true);  
        }
        
		Job job = Job.getInstance(conf);
		job.setJobName("CallHttpService");
		
		job.setJarByClass(CallHttpServiceDriver.class); 
		Class mapperClass = Class.forName(mapper);
		job.setMapperClass(mapperClass);
		
		job.setNumReduceTasks(0); 
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
