package org.whania.experiment5.mrhbase;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task3_4Reducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
	@Override
	protected void reduce(
			ImmutableBytesWritable key, Iterable<Put> values,
			Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Mutation>.Context context
	) throws IOException, InterruptedException {
		for(Put value : values) {
			context.write(null, value);
		}
	}
}
