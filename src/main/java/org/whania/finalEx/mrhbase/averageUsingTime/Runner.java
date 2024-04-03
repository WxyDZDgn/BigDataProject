package org.whania.finalEx.mrhbase.averageUsingTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
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

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(
				"bike_filter",
				scan,
				MyMapper.class,
				IntWritable.class,
				Text.class,
				job
		);

		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output"));
		job.waitForCompletion(true);

	}
}
