package com.nestoop.yelibar.cluster.storm.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.nestoop.yelibar.cluster.storm.conf.YLBConfig;
import com.nestoop.yelibar.cluster.storm.conf.YLBContants;

import backtype.storm.tuple.Tuple;

/**
 * 
 * @author 数据库处理
 *
 */
public class DBManager {
	
	public static String dirver=YLBConfig.getProperty(YLBContants.MYSQL_DRIVER);
	public static String url=YLBConfig.getProperty(YLBContants.MYSQL_URL);
	public static String password=YLBConfig.getProperty(YLBContants.MYSQL_PASSWORD);
	public static String user=YLBConfig.getProperty(YLBContants.MYSQL_USER);
	
	public  static Connection getConnection(){
		Connection conn = null;
		try {
			Class.forName(dirver);
			conn=DriverManager.getConnection(url,user, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return conn;
	}
	
	public static String getSql(Tuple tuple){
		
		
		return null;
	}
	
	public  static  Statement getStatement(){
		//链接
		Connection conn=getConnection();
		try {
			Statement st=conn.createStatement();
			return st;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
		
	}

}
