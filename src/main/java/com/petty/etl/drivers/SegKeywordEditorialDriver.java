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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SegKeywordEditorialDriver{

	public static void main(String[] args) throws Exception{
		String input = null;
		String output = null;
		String mapper = null;
		String jobname = null;
		
		Configuration conf = new Configuration(); 
		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("input", "input", true, "input path");
			opts.addOption("output", "output", true, "output path");
			opts.addOption("mapper", "mapper", true, "mapper class");
			opts.addOption("jobname", "jobname", true, "jobname");
			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			input = cl.getOptionValue("input");
			output = cl.getOptionValue("output");
			mapper = cl.getOptionValue("mapper");
			jobname = cl.getOptionValue("jobname");
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		final FileSystem filesystem = FileSystem.get(new URI(output), conf);  
        if(filesystem.exists(new Path(output))){  
            filesystem.delete(new Path(output), true);  
        }  
        
		Job job = Job.getInstance(conf);
		job.setJobName(jobname);
		
		job.setJarByClass(SegKeywordEditorialDriver.class); 
		Class mapperClass = Class.forName(mapper);
		job.setMapperClass(mapperClass);
		
		job.setNumReduceTasks(0); 
		
		// Output Text File
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
