package org.whania.finalEx.mrhbase.averageAppearance;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	private final Text VAL = new Text();
	@Override
	protected void reduce(
			IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context
	) throws IOException, InterruptedException {
		HashMap<String, Long> hash = new HashMap<>();
		for(Text value : values) {
			String[] ls = value.toString().split(" ");
			String date = ls[0], time = ls[1];
			long val = hash.getOrDefault(date, 0L);
			val += Long.parseLong(time);
			hash.put(date, val);
		}
		AtomicLong count = new AtomicLong(0L), total = new AtomicLong(0L);
		hash.forEach((k, v) -> {
			count.addAndGet(1L);
			total.addAndGet(v);
		});
		VAL.set(String.format("%.2f", 1.0 * total.get() / count.get()));
		context.write(key, VAL);
	}
}
