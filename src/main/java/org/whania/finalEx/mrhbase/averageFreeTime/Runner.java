package org.whania.finalEx.mrhbase.averageFreeTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Runner {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration);
		job.setJarByClass(Runner.class);
		job.setJar("./target/Hadoop-1.0-SNAPSHOT.jar");

		Filter filter = new SingleColumnValueFilter("info".getBytes(), "car_id".getBytes(), CompareOperator.EQUAL, new BinaryComparator("5795".getBytes()));
		Scan scan = new Scan().setFilter(filter);
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(
				"bike_filter",
				scan,
				MyMapper.class,
				NullWritable.class,
				Text.class,
				job
		);

		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output"));
		job.waitForCompletion(true);

	}
}
