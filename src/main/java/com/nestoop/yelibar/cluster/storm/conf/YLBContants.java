package com.nestoop.yelibar.cluster.storm.conf;


public class YLBContants {
	
	//mysql 信息
	public static final String MYSQL_DRIVER="mysql.driver";
	public static final String MYSQL_URL="mysql.url";
	public static final String MYSQL_USER="mysql.user";
	public static final String MYSQL_PASSWORD="mysql.password";
	
	//kafka cluster 配置：
	 public static final String ZOOKEEPER_CLUSTER="zookeeper.cluser";
	 //hdfs uri storm 数据文件
	 public static final String HADOOP_HDFS_STORM_URI="hdfs.storm";
	 
	 //定义一个kafka的topic 
	 public static final String KAFKA_APPLOG_TOPIC = "appLogTopic";
	 //字节转换成字符串字符集
	 public static final String CONFIG_BYTE_ENCODE = "utf8";
	 //流读取字符集
	 public static final String CONFIG_STREAM_ENCODE = "UTF-8";
	 
	 

}
