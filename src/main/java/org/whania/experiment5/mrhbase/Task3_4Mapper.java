package org.whania.experiment5.mrhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task3_4Mapper extends TableMapper<ImmutableBytesWritable, Put> {

	@Override
	protected void map(
			ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context
	) throws IOException, InterruptedException {
		Put put = new Put(key.get());
		for(Cell cell : value.listCells()) {
			put.add(cell);
		}
		context.write(key, put);
	}
}
