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

import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class WeiboJsonMergeDriver{
	
	private static Configuration conf = null; 
	static{
		conf = new Configuration(); 
	}

	public static void main(String[] args) throws Exception{
		String input = null;
		String output = null;
		String history = null;
		String mapper = null;
		String reducer = null;
		
		GenericOptionsParser optionParser;
		try {
			optionParser = new GenericOptionsParser(conf, args);
			String[] remainingArgs = optionParser.getRemainingArgs();
			Options opts = new Options();
			opts.addOption("input", "input", true, "input path");
			opts.addOption("history", "history", true, "history path");
			opts.addOption("output", "output", true, "output path");
			opts.addOption("mapper", "mapper", true, "mapper class");
			opts.addOption("reducer", "reducer", true, "reducer class");
			BasicParser parser = new BasicParser();
			CommandLine cl = parser.parse(opts, remainingArgs);
			input = cl.getOptionValue("input");
			history = cl.getOptionValue("history");
			output = cl.getOptionValue("output");
			mapper = cl.getOptionValue("mapper");
			reducer = cl.getOptionValue("reducer");
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
		
		job.setJarByClass(WeiboJsonMergeDriver.class); 
		
		Class mapperClass = Class.forName(mapper);
		job.setMapperClass(mapperClass);
		
		Class reducerClass = Class.forName(reducer);
		job.setReducerClass(reducerClass);

		job.setJobName("MergeQuestion_weibojson");
		
		if(history != null && !"".equalsIgnoreCase(history)){
			FileInputFormat.addInputPath(job, new Path(history));
		}
		
		job.setNumReduceTasks(10); 
		job.setInputFormatClass(LzoTextInputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output));
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
		
		lzoIndexer.setConf(conf);
		lzoIndexer.run(new String[] { output });
		System.exit(result);

	}

}
