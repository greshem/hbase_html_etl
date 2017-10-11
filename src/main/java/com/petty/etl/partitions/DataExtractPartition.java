package com.petty.etl.partitions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class DataExtractPartition extends HashPartitioner<Text,IntWritable>{ 

	@Override  
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {  
        int p=0;  
        if(key.toString().length()!=11){  
            p=1;  
        }  
        return p;  
    }  

}
