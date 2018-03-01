package com.nestoop.yelibar.cluster.storm.batch;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.nestoop.yelibar.cluster.storm.base.YLBEventHandlerBlot;
import com.nestoop.yelibar.cluster.storm.db.DBManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * storm 批处理数据
 * 实现：批处理处理sql 数据,出现问题：超时如何处理，队列满了怎么处理，队列不满时间过长怎么处理
 * @author xbao
 *
 */
public class YLBStormBaseBatchBlot extends YLBEventHandlerBlot{
	
	private static final long serialVersionUID = 1L;
	
	//输出收集器
	private OutputCollector collector = null;
	
	//创建缓存并发队列
	private Queue<Tuple> tupleQueue = new ConcurrentLinkedQueue<Tuple>();
	
	//最后时间
	private long lastTime;
	
	//连接数据
	private Connection connection;
	
	//计数
	private int count;
	
	public  YLBStormBaseBatchBlot(int sqlNum){
		count=sqlNum;
		connection = DBManager.getConnection();
		lastTime=System.currentTimeMillis();
	}
	
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		
	}

	@Override
	public void execute(Tuple input) {
		
		try{
			
			//向queue添加tuple
			tupleQueue.add(input);
			//当前时间
			long current = System.currentTimeMillis();
			
			//判断在1秒一内批量提交数据库,队列中的记录大于规定的sql数据
			if(tupleQueue.size()>=  count  || current >= lastTime +1000){
				
				//创建sql statement
				Statement st= connection.createStatement();
				//不能自动提交
				connection.setAutoCommit(false);
				//循环批量提交
				for(int i=0;i<count;i++){
					//从队列拿出数据
					Tuple tuple = tupleQueue.poll();
					//生成sql
					String sql=DBManager.getSql(tuple);
					//添加批量
					st.addBatch(sql);
					//汇报成功
					collector.ack(tuple);
				}
				//批量执行
				st.executeBatch();
				//提交
				connection.commit();
				//开启自动提交
				connection.setAutoCommit(true);
				//改变时间
				lastTime= current;
				
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
