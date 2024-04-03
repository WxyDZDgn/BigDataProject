package org.whania.test.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Case1 {

	public static void createEducoderTable1() throws SQLException, ClassNotFoundException {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
		String sql = "CREATE TABLE EDUCODER_TABLE1(INFO.BEGINTIME VARCHAR, INFO.ENDTIME VARCHAR, ID BIGINT PRIMARY KEY, INFO.SALARY INTEGER, INFO.CITY VARCHAR) BLOCKSIZE = 32000, DATA_BLOCK_ENCODING='DIFF', VERSIONS = 3, MAX_FILESIZE = 10000000;";
		connection.createStatement().execute(sql);
		connection.commit();
		connection.close();
	}

	public static void createEducoderTable2() throws SQLException, ClassNotFoundException {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
		String sql = "CREATE TABLE EDUCODER_TABLE2(\"info\".\"beginTime\" VARCHAR, \"info\".\"endTime\" VARCHAR, \"id\" BIGINT PRIMARY KEY, \"info\".\"salary\" INTEGER, \"info\".\"city\" VARCHAR) BLOCKSIZE = 65536, VERSIONS = 5, COMPRESSION = 'GZ', MAX_FILESIZE = 20000000;";
		connection.createStatement().execute(sql);
		connection.commit();
		connection.close();
	}

}
