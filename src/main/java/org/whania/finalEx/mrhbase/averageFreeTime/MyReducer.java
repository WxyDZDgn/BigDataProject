package org.whania.finalEx.mrhbase.averageFreeTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MyReducer extends Reducer<NullWritable, Text, Text, NullWritable> {
	private final SimpleDateFormat FORMAT = new SimpleDateFormat("M/d/yyyy H:m");

	private final Text KEY = new Text();
	private final NullWritable VAL = NullWritable.get();

	@Override
	protected void reduce(
			NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, Text, NullWritable>.Context context
	) throws IOException, InterruptedException {
		List<String> ls = new ArrayList<>();
		for(Text value : values) {
			ls.add(value.toString());
		}
		if(ls.size() <= 1) {
			KEY.set("0");
		} else {
			ls.sort(null);
			long res = 0;
			for(int i = 1; i < ls.size(); ++ i) {
				try {
					res += fun(ls.get(i - 1), ls.get(i));
				} catch(ParseException ignored) {
				}
			}
			KEY.set(String.format("%.2f", 1.0 * res / (ls.size() - 1)));
		}
		context.write(KEY, VAL);
	}
	private long fun(String left, String right) throws ParseException {
		String start = left.split(",")[1], end = right.split(",")[0];
		return (FORMAT.parse(end).getTime() - FORMAT.parse(start).getTime()) / (1000 * 60);
	}
}
