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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.petty.etl.constant.Constants;
import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class GeneralFilterDriver{
	
	private static Configuration conf;
	
	static{
		conf = new Configuration(); 
	}

	public static void main(String[] args) throws Exception{
		String blacklist = null;
		String input = null;
		String output = null;
		String jobName = null;
		String mapper = null;
		String url = null;

		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("blacklist", "blacklist", true, "black list files");
			opts.addOption("input", "input", true, "input path");
			opts.addOption("output", "output", true, "output path");
			opts.addOption("jobname", "jobname", true, "jobName");
			opts.addOption("mapper", "mapper", true, "mapper");
			opts.addOption("url", "url", true, "url");
			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			blacklist = cl.getOptionValue("blacklist");
			input = cl.getOptionValue("input");
			output = cl.getOptionValue("output");
			jobName = cl.getOptionValue("jobname");
			mapper = cl.getOptionValue("mapper");
			url = cl.getOptionValue("url");
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		final FileSystem filesystem = FileSystem.get(new URI(output), conf);  
        if(filesystem.exists(new Path(output))){  
            filesystem.delete(new Path(output), true);  
        }  
        
        conf.set("jobname", jobName);
		if(url != null && !"".equalsIgnoreCase(url)){
			conf.set(Constants.URL, url);
		}
		Job job = Job.getInstance(conf);
		job.setJobName("Filter_" + jobName);
		
		
		job.setJarByClass(GeneralFilterDriver.class); 
		Class mapperClass = Class.forName(mapper);
		job.setMapperClass(mapperClass);
		
		job.setNumReduceTasks(20); 
		job.setInputFormatClass(LzoTextInputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if(blacklist != null && !"".equalsIgnoreCase(blacklist)){
			String[] listArray = blacklist.split(",");
			for(int i=0; i<listArray.length; i++){
				job.addCacheFile(new Path(listArray[i].trim()).toUri());
			}
		}
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output));
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
		
		lzoIndexer.setConf(conf);
		lzoIndexer.run(new String[] { output });
		System.exit(result);

	}

}
