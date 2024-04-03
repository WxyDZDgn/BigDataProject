package org.whania.finalEx.mrhbase.averageAppearance;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyMapper extends TableMapper<IntWritable, Text> {
	private final SimpleDateFormat FORMAT = new SimpleDateFormat("M/d/yyyy H:m");
	private final IntWritable KEY = new IntWritable();
	private final Text VAL = new Text();

	@Override
	protected void map(
			ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, IntWritable, Text>.Context context
	) throws IOException, InterruptedException {
		String date = null;
		Integer carId = null;
		for(Cell cell : value.listCells()) {
			String qualifier = new String(CellUtil.cloneQualifier(cell));
			String val = new String(CellUtil.cloneValue(cell));
			switch(qualifier) {
				case "start_time":
					date = val.split(" ")[0];
					break;
				case "car_id":
					carId = Integer.parseInt(val);
					break;
			}
		}
		if(date != null && carId != null) {
			KEY.set(carId);
			VAL.set(date + " 1");
			context.write(KEY, VAL);
		}
	}
}
