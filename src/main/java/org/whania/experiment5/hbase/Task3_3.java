package org.whania.experiment5.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.whania.util.MyHBase;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class Task3_3 {
	public static void main(String[] args) throws IOException {
		Scanner scanner = new Scanner(new File("./data/Score.txt"));

		MyHBase.init();

		String[] tables = new String[] {"chinese", "math", "english"};
		for(String table : tables) {
			if(MyHBase.isExist(table)) MyHBase.dropTable(table);
			MyHBase.createTable(table, "score");
		}
		while(scanner.hasNextLine()) {
			String[] ls = scanner.nextLine().split(" ", -1);
			for(int i = 0; i < tables.length; i++) {
				MyHBase.insertRow(tables[i], ls[0], "score", "data", ls[i + 1]);
			}
		}

		for(String tableString : tables) {
			System.out.printf("-----%s-----%n", tableString);
			Table table = MyHBase.getTable(tableString);
			ResultScanner res = table.getScanner(new Scan());
			for(Result re : res) {
				MyHBase.printResult(re);
			}
		}

		MyHBase.close();
		scanner.close();
	}
}
