package com.nestoop.yelibar.cluster.storm.conf;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;


/**
 * 读取配置文件
 * @author xbao
 *
 */
public class YLBConfig {
	
	public static Map<String,String> dataMap = new HashMap<String,String>();
	
	/**
	 * 获取配置文件信息
	 * @param key
	 * @return
	 */
	public static String getProperty(String key){
		if(dataMap.containsKey(key)){
			return dataMap.get(key);
		}else{
			ResourceBundle bundle=ResourceBundle.getBundle("datasources",Locale.getDefault());    
	        //put add
	        String value=bundle.getString(key);
	        dataMap.put(key, value);
			return value;
		}
	}
	/**
	 * 读取storm配置
	 * @param key
	 * @return
	 */
	public static String getStormProperty(String key){
		if(dataMap.containsKey(key)){
			return dataMap.get(key);
		}else{
			ResourceBundle bundle=ResourceBundle.getBundle("storm",Locale.getDefault());    
			//put add
			String value=bundle.getString(key);
			dataMap.put(key, value);
			return value;
		}
	}
	
	public static void main(String[] args) {
		System.out.println(getStormProperty(YLBContants.ZOOKEEPER_CLUSTER));
	}
	

}
