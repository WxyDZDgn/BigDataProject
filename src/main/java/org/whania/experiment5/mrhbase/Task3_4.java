package org.whania.experiment5.mrhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Task3_4 {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration);
		job.setJarByClass(Task3_4.class);
		job.setJar("./target/Hadoop-1.0-SNAPSHOT.jar");

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(
				"english",
				scan,
				Task3_4Mapper.class,
				ImmutableBytesWritable.class,
				Put.class,
				job
		);
		TableMapReduceUtil.initTableReducerJob(
				"englishScore",
				Task3_4Reducer.class,
				job
		);

		job.waitForCompletion(true);

	}
}
