package org.whania.experiment5.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task3_1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	private final LongWritable RES = new LongWritable();
	@Override
	protected void reduce(
			Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context
	) throws IOException, InterruptedException {
		long res = 0;
		for(LongWritable ignored : values) {
			++ res;
		}
		RES.set(res);
		context.write(key, RES);
	}
}
