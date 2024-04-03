package org.whania.finalEx.mrhbase.averageAppearance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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

		Filter filter1 = new SingleColumnValueFilter("info".getBytes(), "origin".getBytes(), CompareOperator.EQUAL, new RegexStringComparator(".*河北省保定市雄县.*"));
		Filter filter2 = new SingleColumnValueFilter("info".getBytes(), "destination".getBytes(), CompareOperator.EQUAL, new RegexStringComparator(".*韩庄村.*"));
		FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
		Scan scan = new Scan().setFilter(filter);
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
