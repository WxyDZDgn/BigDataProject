package org.whania.experiment5.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task3_1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private final static LongWritable ONE = new LongWritable(1L);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(value, ONE);
	}
}
