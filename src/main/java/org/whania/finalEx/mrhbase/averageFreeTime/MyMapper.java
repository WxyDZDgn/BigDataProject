package org.whania.finalEx.mrhbase.averageFreeTime;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

public class MyMapper extends TableMapper<NullWritable, Text> {
	private final NullWritable KEY = NullWritable.get();
	private final Text VAL = new Text();

	@Override
	protected void map(
			ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context
	) throws IOException, InterruptedException {
		List<Cell> cells = value.listCells();
		String start = null, end = null;
		for(Cell cell : cells) {
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			String val = new String(CellUtil.cloneValue(cell));
			if("start_time".equals(qualifier)) {
				start = val;
			} else if("end_time".equals(qualifier)) {
				end = val;
			}
		}
		if(start != null && end != null) {
			VAL.set(start + "," + end);
			context.write(KEY, VAL);
		}
	}
}
