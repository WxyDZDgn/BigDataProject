package org.whania.finalEx.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.whania.util.MyHBase;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

public class TaskTest {
	private static final String ORIGIN_TABLE = "bike";
	private static final String TABLE = "bike_filter";
	private static final String FAMILY = "info";
	private static final String[] QUALIFIER = {
			"trip_id",
			"start_time",
			"end_time",
			"car_id",
			"origin",
			"destination",
			"city",
			"start_longitude",
			"start_latitude",
			"end_longitude",
			"end_latitude"
	};
	@Before
	public void init() {
		System.out.println("--------INIT--------");
		MyHBase.init();
		System.out.println("--------INIT--------");
	}
	@After
	public void close() {
		System.out.println("--------CLOSE--------");
		MyHBase.close();
		System.out.println("--------CLOSE--------");
	}
	@Test
	public void createTable() throws IOException {
		if(MyHBase.isExist(TaskTest.ORIGIN_TABLE)) {
			MyHBase.dropTable(TaskTest.ORIGIN_TABLE);
		}
		MyHBase.createTable(TaskTest.ORIGIN_TABLE, TaskTest.FAMILY);
		Scanner scanner = new Scanner(new File("./data/dataResources.csv"));
		while(scanner.hasNextLine()) {
			String[] items = scanner.nextLine().split(",", -1);
			for(int i = 1; i < items.length; ++ i) {
				MyHBase.insertRow(TaskTest.ORIGIN_TABLE, items[0], TaskTest.FAMILY, TaskTest.QUALIFIER[i], items[i]);
			}
		}
		scanner.close();
	}
	@Test
	public void filterTable() throws IOException {
		if(MyHBase.isExist(TaskTest.TABLE)) {
			MyHBase.dropTable(TaskTest.TABLE);
		}
		MyHBase.createTable(TaskTest.TABLE, TaskTest.FAMILY);
		Table oriTable = MyHBase.getTable(TaskTest.ORIGIN_TABLE);
		Table tarTable = MyHBase.getTable(TaskTest.TABLE);
		Filter filter = new SingleColumnValueFilter(TaskTest.FAMILY.getBytes(), "car_id".getBytes(), CompareOperator.EQUAL, new RegexStringComparator(".*95$"));
		Scan scan = new Scan().setFilter(filter);
		ResultScanner scanner = oriTable.getScanner(scan);
		for(Result result : scanner) {
			List<Cell> cells = result.listCells();
			for(Cell cell : cells) {
				Put put = new Put(result.getRow()).
						addColumn(CellUtil.cloneFamily(cell),
								CellUtil.cloneQualifier(cell),
								CellUtil.cloneValue(cell)
						);
				tarTable.put(put);
			}
		}
		tarTable.close();
	}
	@Test
	public void getUsedCount() throws IOException {
		Table table = MyHBase.getTable(TaskTest.TABLE);
		Filter filter1 = new SingleColumnValueFilter(TaskTest.FAMILY.getBytes(), "start_time".getBytes(), CompareOperator.GREATER_OR_EQUAL, new BinaryComparator("8/1/2017".getBytes()));
		Filter filter2 = new SingleColumnValueFilter(TaskTest.FAMILY.getBytes(), "end_time".getBytes(), CompareOperator.LESS_OR_EQUAL, new BinaryComparator("9/1/2017~".getBytes()));
		FilterList filter = new FilterList(filter1, filter2);
		Scan scan = new Scan().setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		AtomicLong result = new AtomicLong(0);
		scanner.forEach(each -> {
			result.addAndGet(1L);
		});
		System.out.println("数据总数: " + result.get());
	}
}
