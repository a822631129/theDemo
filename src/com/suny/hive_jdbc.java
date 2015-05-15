package com.suny;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class hive_jdbc {
	private static ResultSet rs = null;
	
	public static void main(String[] args) throws Exception {
		HiveConf hiveconf = new HiveConf();
		hiveconf.set("hive.metastore.uris", "thrift://mr203:9083");
		HiveMetaStoreClient hivemsclient = new HiveMetaStoreClient(hiveconf);
		List<FieldSchema> fieldschemas1 = hivemsclient.getFields("mytest", "par_table");
		List<FieldSchema> fieldschemas2 = hivemsclient.getSchema("mytest", "par_table");
		//String result = fieldschema.get(0);
		System.out.println(fieldschemas1);
		System.out.println(fieldschemas2);
		
		
		//getDesc();
	}

	/**
	 * 连接hive
	 * @param urlHive 地址
	 * @param userHive 用户名 
	 * @param passHive 密码
	 * @return 返回连接
	 * @throws Exception 抛出异常
	 */
	public static Connection getconn(String urlHive, String userHive, String passHive) throws Exception{
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection conn = DriverManager.getConnection(urlHive, userHive, passHive);
		return conn;
	}
	
	private static void getDesc() throws Exception {
		Connection conn = getconn("jdbc:hive2://192.168.8.203:10000/mytest", "", "");
		Statement stmt = conn.createStatement();
		String sql = "describe par_table";
		rs = stmt.executeQuery(sql);
		String result = "";
		while (rs.next()) {
			result += rs.getString(1) + rs.getString(2) + "\n";
		}
		rs = null;
        System.out.println(result);
	}
	
	
}
