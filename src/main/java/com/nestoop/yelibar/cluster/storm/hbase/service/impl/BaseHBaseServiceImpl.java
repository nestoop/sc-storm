package com.nestoop.yelibar.cluster.storm.hbase.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.nestoop.yelibar.cluster.storm.hbase.service.BaseHBaseService;

public class BaseHBaseServiceImpl implements BaseHBaseService{
	//hbase �����ӳ���
	public HConnection hTablePool = null;
	public static Configuration conf =null;
	public BaseHBaseServiceImpl(){
		conf = new Configuration();
		String zk_list = "node4:2181,node5:2181,node6:2181";
		conf.set("hbase.zookeeper.quorum", zk_list);
		try {
			hTablePool = HConnectionManager.createConnection(conf) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(put) ;
			
		} catch (Exception e) {
			e.printStackTrace() ;
		}finally{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public void insert(String tableName, String rowKey, String family,
			String quailifer, String value) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes()) ;
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public void insert(String tableName,String rowKey,String family,String quailifer[],String value[])
	{
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Put put = new Put(rowKey.getBytes());
			// �������
			for (int i = 0; i < quailifer.length; i++) {
				String col = quailifer[i];
				String val = value[i];
				put.add(family.getBytes(), col.getBytes(), val.getBytes());
			}
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public void save(List<Put> Put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(Put) ;
		}
		catch (Exception e) {
			// TODO: handle exception
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}


	@Override
	public Result getOneRow(String tableName, String rowKey) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Get get = new Get(rowKey.getBytes()) ;
			rsResult = table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}
	
	@Override
	public Result getOneRowAndMultiColumn(String tableName, String rowKey,String[] cols) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Get get = new Get(rowKey.getBytes()) ;
			for (int i = 0; i < cols.length; i++) {
				get.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			rsResult = table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKeyLike) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	@Override
	public List<Result> getRows(String tableName, String rowKeyLike ,String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	@Override
	public List<Result> getRowsByOneKey(String tableName, String rowKeyLike ,String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	@Override
	public List<Result> getRows(String tableName,String startRow,String stopRow)
	{
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Scan scan = new Scan() ;
			scan.setStartRow(startRow.getBytes()) ;
			scan.setStopRow(stopRow.getBytes()) ;
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rsResult : scanner) {
				list.add(rsResult) ;
			}
			
		}catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	@Override
	public void deleteRecords(String tableName, String rowKeyLike){
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			List<Delete> list = new ArrayList<Delete>() ;
			for (Result rs : scanner) {
				Delete del = new Delete(rs.getRow());
				list.add(del) ;
			}
			table.delete(list);
		}
		catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public   void createTable(String tableName, String[] columnFamilys){
		try {
			// admin ����
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				System.err.println("�˱��Ѵ��ڣ�");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(
						TableName.valueOf(tableName));

				for (String columnFamily : columnFamilys) {
					tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				}

				admin.createTable(tableDesc);
				System.err.println("����ɹ�!");

			}
			admin.close();// �ر��ͷ���Դ
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * ɾ��һ����
	 * 
	 * @param tableName
	 *            ɾ���ı���
	 * */
	public   void deleteTable(String tableName)   {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);// ���ñ�
				admin.deleteTable(tableName);// ɾ����
				System.err.println("ɾ����ɹ�!");
			} else {
				System.err.println("ɾ���ı����ڣ�");
			}
			admin.close();
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 
	/**
	 * ��ѯ����������
	 * @param tablename
	*/
	public void scaner(String tablename) {
	try {
	        HTable table =new HTable(conf, tablename);
	        Scan s =new Scan();
//	        s.addColumn(family, qualifier)
//	        s.addColumn(family, qualifier)
	        ResultScanner rs = table.getScanner(s);
	for (Result r : rs) {
	            
		   for(Cell cell:r.rawCells()){   
			    System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
			    System.out.println("Timetamp:"+cell.getTimestamp()+" ");
			    System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
			    System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
			    System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
			   }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	public void scanerByColumn(String tablename) {
		
		try {
			HTable table =new HTable(conf, tablename);
			Scan s =new Scan();
	        s.addColumn("cf".getBytes(), "201504052237".getBytes());
	        s.addColumn("cf".getBytes(), "201504052237".getBytes());
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				
				for(Cell cell:r.rawCells()){   
					System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
					System.out.println("Timetamp:"+cell.getTimestamp()+" ");
					System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
					System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
					System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
