package org.whania.experiment4.hbase;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.whania.util.MyHBase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TaskTest {
	@Before
	public void init() throws IOException {
		MyHBase.init();
		System.out.println("--------INIT--------");
		if(MyHBase.isExist("ORDER_INFO")) MyHBase.dropTable("ORDER_INFO");
		MyHBase.createTable("ORDER_INFO", "C1");
		try(Scanner scanner = new Scanner(new File("./data/data.tsv"))) {
			String [] hash = new String[] {"id", "status", "money", "pay_way", "user_id", "operation_time", "category"};
			while(scanner.hasNext()) {
				String[] ls = scanner.nextLine().split("\t", -1);
				for(int i = 1; i < hash.length; ++ i) {
					MyHBase.insertRow("ORDER_INFO", ls[0], "C1", hash[i], ls[i]);
				}
			}
		}
		System.out.println("--------TASK--------");
	}
	@After
	public void close() {
		MyHBase.close();
		System.out.println("--------DONE--------");
	}
	@Test
	public void task1() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			MyHBase.printResult(result);
		}
	}
	@Test
	public void task2() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		Scan scan = new Scan().withStartRow("000010".getBytes()).withStopRow("000015".getBytes(), true);
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			MyHBase.printResult(result);
		}
	}
	@Test
	public void task3() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		List<Delete> ls = new ArrayList<>();
		for(int i = 10; i <= 15; ++i ) {
			ls.add(new Delete(("0000" + i).getBytes()));
		}
		table.delete(ls);
	}
	@Test
	public void task4() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		Scan scan = new Scan().setCaching(10).withStartRow("000015".getBytes()).withStopRow("000359".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			MyHBase.printResult(result);
		}
	}
	@Test
	public void task5() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		Filter payWay = new SingleColumnValueFilter("C1".getBytes(), "pay_way".getBytes(), CompareOperator.EQUAL, "1".getBytes());
		Filter amount = new SingleColumnValueFilter("C1".getBytes(), "money".getBytes(), CompareOperator.GREATER, new BinaryComparator("4000".getBytes()));
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, payWay, amount);
		Scan scan = new Scan().setFilter(filterList).withStartRow("000035".getBytes(), true).withStopRow("000080".getBytes(), true);
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			MyHBase.printResult(result);
		}
	}
	@Test
	public void task6() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		Filter userId = new SingleColumnValueFilter("C1".getBytes(), "user_id".getBytes(), CompareOperator.EQUAL, new RegexStringComparator(".*9.*"));
		Scan scan = new Scan().setFilter(userId).addColumn("C1".getBytes(), "category".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			MyHBase.printResult(result);
		}
	}
	@Test
	public void task7() throws IOException {
		Table table = MyHBase.getTable("ORDER_INFO");
		Filter filter = new PageFilter(5);
		Scan scan = new Scan().setFilter(filter);
		byte[] last = null;
		ResultScanner scanner = table.getScanner(scan);
		int totalPage = 0;
		while(true) {
			int count = 0;
			for(Result result : scanner) {
				++ count;
				MyHBase.printResult(result);
				last = result.getRow();
			}
			if(count <= 0) break;
			scan.withStartRow(last, false);
			scanner = table.getScanner(scan);
			++ totalPage;
			System.out.printf("++ Page %s ++%n", totalPage);
		}
	}
}
