package org.whania.experiment5.mapreduce;

import org.whania.util.BasicJobConfig;

import java.io.IOException;

public class Task3_1 {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		BasicJobConfig config =
				new BasicJobConfig(Task3_1Mapper.class, Task3_1Reducer.class, "/input/name.txt", "/output");
		config.start();
	}
}
