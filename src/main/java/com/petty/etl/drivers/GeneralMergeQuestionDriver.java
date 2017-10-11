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

import com.petty.etl.mappers.GeneralMergeQuestionMapper;
import com.petty.etl.reducers.GeneralMergeQuestionReducer;

public class GeneralMergeQuestionDriver {
	
	private static Configuration conf;
	static{
		conf = new Configuration(); 
	}

	public static void main(String[] args) throws Exception{
		String input = null;
		String output = null;
		String history = null;
		String symbollist = null;
		String jobName = null;
		
		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("symbollist", "symbollist", true, "symbol list file");
			opts.addOption("input", "input", true, "input path");
			opts.addOption("history", "history", true, "history path");
			opts.addOption("output", "output", true, "output path");
			opts.addOption("jobname", "jobname", true, "job name and source name");
			
			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			symbollist = cl.getOptionValue("symbollist");
			input = cl.getOptionValue("input");
			history = cl.getOptionValue("history");
			output = cl.getOptionValue("output");
			jobName = cl.getOptionValue("jobname");
			
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
		job.setJobName("MergeQuestion_" + jobName);
		
		if(jobName==null){
			System.out.println("jobName is null,error");
			return;
		}
		
		job.setJarByClass(GeneralMergeQuestionDriver.class); 
		job.setMapperClass(GeneralMergeQuestionMapper.class);
		job.setReducerClass(GeneralMergeQuestionReducer.class);
		
		job.setNumReduceTasks(10); 
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.addCacheFile(new Path(symbollist).toUri());
		
		FileInputFormat.addInputPath(job, new Path(input));
		if(history!=null){
			System.out.println("history:\t"+history);
			FileInputFormat.addInputPath(job, new Path(history));
		}
		FileOutputFormat.setOutputPath(job,new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
