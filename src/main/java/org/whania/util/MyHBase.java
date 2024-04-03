package org.whania.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyHBase {
	public static Connection connection = null;
	public static Admin admin = null;
	public static void init() {
		Configuration configuration = HBaseConfiguration.create();
		try {
			MyHBase.connection = ConnectionFactory.createConnection(configuration);
			MyHBase.admin = connection.getAdmin();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
	public static void close() {
		if(MyHBase.admin != null) {
			try {
				MyHBase.admin.close();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		if(MyHBase.connection != null) {
			try {
			MyHBase.connection.close();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	public static void createTable(String tName, String defaultFamily, String...families) throws IOException {
		if(MyHBase.isExist(tName)) {
			System.out.printf("%s is already exist.%n", tName);
			return;
		}
		TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tName));
		tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(defaultFamily.getBytes()).build());
		for(String family : families) {
			tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build());
		}
		MyHBase.admin.createTable(tableDescriptorBuilder.build());
		System.out.printf("%s created.%n", tName);
	}
	public static boolean isExist(String tName) throws IOException {
		return MyHBase.admin.tableExists(TableName.valueOf(tName));
	}
	public static Table getTable(String tName) throws IOException {
		return connection.getTable(TableName.valueOf(tName));
	}
	public static void getData(String tName, String rowKey, String family, String qualifier) throws IOException {
		if(!MyHBase.isExist(tName)) {
			System.out.printf("%s is not exist.%n", tName);
			return;
		}
		Table table = MyHBase.getTable(tName);
		Get get = new Get(rowKey.getBytes());
		if(!qualifier.isEmpty()) {
			get.addColumn(
					family.getBytes(),
					qualifier.getBytes()
			);
		} else if(!family.isEmpty()) {
			get.addFamily(family.getBytes());
		}
		Result result = table.get(get);
		MyHBase.printResult(result);
	}
	public static void deleteData(String tName, String rowKey, String family, String qualifier) throws IOException {
		Table table = MyHBase.getTable(tName);
		Delete delete = new Delete(rowKey.getBytes());
		if(!qualifier.isEmpty()) {
			delete.addColumn(family.getBytes(), qualifier.getBytes());
		} else if(!family.isEmpty()) {
			delete.addFamily(family.getBytes());
		}
		table.delete(delete);
		System.out.println("delete data done.");
	}
	public static void dropTable(String tName) throws IOException {
		if(!MyHBase.isExist(tName)) {
			System.out.printf("%s is not exist.%n", tName);
			return;
		}
		MyHBase.admin.disableTable(TableName.valueOf(tName));
		MyHBase.admin.deleteTable(TableName.valueOf(tName));

		System.out.printf("%s deleted.%n", tName);
	}
	public static void printResult(Result result) throws IOException {
		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()) {
			Cell cell = result.current();
			System.out.printf("%s\t%s:%s\t%s%n",
					Bytes.toString(CellUtil.cloneRow(cell)),
					Bytes.toString(CellUtil.cloneFamily(cell)),
					Bytes.toString(CellUtil.cloneQualifier(cell)),
					Bytes.toString(CellUtil.cloneValue(cell))
			);
		}
	}
	public static void insertRow(String tName, String rowKey, String family, String qualifier, String value) throws IOException {
		if(!MyHBase.isExist(tName)) {
			System.out.printf("%s is not exist.%n", tName);
			return;
		}
		Table table = MyHBase.getTable(tName);
		Put put = new Put(rowKey.getBytes()).
				addColumn(family.getBytes(),
						qualifier.getBytes(),
						value.getBytes()
				);
		table.put(put);
		table.close();
		System.out.println("put done.");
	}
	public static void modifyFamilyProperty(String tName, ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder)
			throws IOException {
		TableName tableName = TableName.valueOf(tName);
		MyHBase.admin.modifyColumnFamily(tableName, columnFamilyDescriptorBuilder.build());
		System.out.println("modify done.");
	}
	public static void addFamilyProperty(String tName, ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder)
			throws IOException {
		TableName tableName = TableName.valueOf(tName);
		MyHBase.admin.addColumnFamily(tableName, columnFamilyDescriptorBuilder.build());
		System.out.println("add done.");
	}
	public static ColumnFamilyDescriptor getColumnFamilyDescriptor(String tName, String family) throws IOException {
		TableName tableName = TableName.valueOf(tName);
		TableDescriptor tableDescriptor = MyHBase.admin.getDescriptor(tableName);
		return tableDescriptor.getColumnFamily(family.getBytes());
	}
	public static ColumnFamilyDescriptor getColumnFamilyDescriptor(String family) throws IOException {
		ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
				ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes());
		return  columnFamilyDescriptorBuilder.build();
	}
	public static void printTableDescriptor(String tName) throws IOException {
		TableDescriptor descriptor = MyHBase.admin.getDescriptor(TableName.valueOf(tName));
		System.out.println(descriptor);
	}
	public static void deleteColumnFamily(String tName, String family) throws IOException {
		MyHBase.admin.deleteColumnFamily(TableName.valueOf(tName), family.getBytes());
		System.out.println("delete done.");
	}
}
