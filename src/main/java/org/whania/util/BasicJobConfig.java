package org.whania.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Scanner;

public class BasicJobConfig {
	private final Job JOB;
	private final String OUTPUT;
	private final String LOCATION = "hdfs://master:9000";

	public BasicJobConfig(Class<? extends Mapper<?, ?, ?, ?>> mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass,
			String inputPath, String outputPath) throws IOException {

		Configuration configuration = new Configuration();
		OUTPUT = outputPath;

		JOB = Job.getInstance(configuration);
		JOB.setJarByClass(BasicJobConfig.class);
		JOB.setJar("./target/Hadoop-1.0-SNAPSHOT.jar");
		JOB.setMapperClass(mapperClass);
		JOB.setReducerClass(reducerClass);

		Type[] types;
		types = ((ParameterizedType) mapperClass.getGenericSuperclass()).getActualTypeArguments();
		JOB.setMapOutputKeyClass((Class<?>) types[2]);
		JOB.setMapOutputValueClass((Class<?>) types[3]);

		types = ((ParameterizedType) reducerClass.getGenericSuperclass()).getActualTypeArguments();
		JOB.setOutputKeyClass((Class<?>) types[2]);
		JOB.setOutputValueClass((Class<?>) types[3]);

		FileInputFormat.setInputPaths(JOB, new Path(LOCATION + inputPath));
		FileOutputFormat.setOutputPath(JOB, new Path(LOCATION + outputPath));
	}

	public void start() throws IOException, InterruptedException, ClassNotFoundException {
		clearOutput();
		JOB.waitForCompletion(true);
		printOutput();
	}

	private void clearOutput() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", LOCATION);

		FileSystem fileSystem = FileSystem.get(conf);
		Path toDelete = new Path(OUTPUT);
		if (fileSystem.exists(toDelete)) {
			fileSystem.delete(toDelete, true);
		}
	}

	private void printOutput() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", LOCATION);

		FileSystem fileSystem = FileSystem.get(conf);
		Path toRead = new Path(OUTPUT + "/part-r-00000");
		try (Scanner scanner = new Scanner(fileSystem.open(toRead))) {
			System.out.println("Results:");
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				System.out.println(line);
			}
		}
	}
}

