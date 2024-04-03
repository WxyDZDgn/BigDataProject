package org.whania.test.mapreduce;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whania.util.BasicJobConfig;

import java.io.IOException;

public class MapReduceTest {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		BasicJobConfig config = new BasicJobConfig(MapperTest.class, ReducerTest.class, "/input", "/output");
		config.start();
	}
}
class MapperTest extends Mapper<LongWritable, Text, Text, LongWritable> {
	private final Logger logger = LoggerFactory.getLogger(MapperTest.class);
	private final LongWritable ONE = new LongWritable(1L);
	private final Text RES = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String[] ls = value.toString().split(" ");
		for(String l : ls) {
			RES.set(l);
			context.write(RES, ONE);
			logger.info(String.valueOf(RES));
		}
	}
}
class ReducerTest extends Reducer<Text, LongWritable, Text, LongWritable> {
	private final Logger logger = LoggerFactory.getLogger(ReducerTest.class);

	private final LongWritable RES = new LongWritable();
	@Override
	protected void reduce(
			Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context
	) throws IOException, InterruptedException {
		long res = 0L;
		for(LongWritable value : values) {
			res += value.get();
		}
		RES.set(res);
		logger.info(String.valueOf(RES));
		context.write(key, RES);
	}
}