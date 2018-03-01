package com.nestoop.yelibar.cluster.storm.hbase.service;

import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * 
 * ²Ù×÷hbase
 * @author xbao
 *
 */
public interface BaseHBaseService {
	
	public void save(Put put,String tableName) ;
	public void insert(String tableName,String rowKey,String family,String quailifer,String value) ;
	public void insert(String tableName,String rowKey,String family,String quailifer[],String value[]) ;
	
	public void save(List<Put>Put ,String tableName) ;
	
	public Result getOneRow(String tableName,String rowKey) ;
	
	public List<Result> getRows(String tableName,String rowKey_like) ;
	
	public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) ;
	
	public List<Result> getRows(String tableName,String startRow,String stopRow) ;
	
	public void deleteRecords(String tableName, String rowKeyLike);
	public   void deleteTable(String tableName);
	public   void createTable(String tableName, String[] columnFamilys);
	public List<Result> getRowsByOneKey(String tableName, String rowKeyLike,
			String[] cols);
	public Result getOneRowAndMultiColumn(String tableName, String rowKey,
			String[] cols);

}
