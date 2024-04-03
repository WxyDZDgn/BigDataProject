package org.whania.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task {

	static String[] chars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

	/**
	 * MD5 加密
	 * @param str 需要加密的文本
	 * @return 加密后的内容
	 */
	public static String StringInMd5(String str) {
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("md5");
			byte[] result = md5.digest(str.getBytes());
			StringBuilder sb = new StringBuilder(32);
			for (int i = 0; i < result.length; i++) {
				byte x = result[i];
				int h = 0x0f & (x >>> 4);
				int l = 0x0f & x;
				sb.append(chars[h]).append(chars[l]);
			}
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * 生成 row
	 *
	 * @param carId     汽车ID
	 * @param timestamp 时间戳
	 * @return rowkey
	 */
	public String createRowKey(String carId, String timestamp) {
		/********** begin **********/

		return StringInMd5(carId).substring(0, 5) + "-" + carId + "-" + timestamp;
		/********** end **********/
	}


	/**
	 * 查询某辆车在某个时间范围的交易记录
	 *
	 * @param carId          车辆ID
	 * @param startTimestamp 开始时间戳
	 * @param endTimestamp   截止时间戳
	 * @return map 存储 (rowkey,value)
	 */
	public Map<String, String> findLogByTimestampRange(String carId, String startTimestamp, String endTimestamp) throws Exception {
		Map<String, String> map = new HashMap<>();
		/********** begin **********/
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		Scan scan = new Scan().withStartRow(createRowKey(carId, startTimestamp).getBytes(), true)
							  .withStopRow((createRowKey(carId, endTimestamp) + "~").getBytes(), true);
		Table table = connection.getTable(TableName.valueOf("deal"));
		ResultScanner scanner = table.getScanner(scan);
		for(Result each : scanner) {
			String key = Bytes.toString(each.getRow());
			List<Cell> cells = each.listCells();
			for(Cell cell : cells) {
				map.put(key, Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		/********** end **********/
		return map;
	}


}
