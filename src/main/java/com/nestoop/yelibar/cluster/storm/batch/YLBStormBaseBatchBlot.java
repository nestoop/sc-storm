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
 * storm ����������
 * ʵ�֣���������sql ����,�������⣺��ʱ��δ�������������ô�������в���ʱ�������ô����
 * @author xbao
 *
 */
public class YLBStormBaseBatchBlot extends YLBEventHandlerBlot{
	
	private static final long serialVersionUID = 1L;
	
	//����ռ���
	private OutputCollector collector = null;
	
	//�������沢������
	private Queue<Tuple> tupleQueue = new ConcurrentLinkedQueue<Tuple>();
	
	//���ʱ��
	private long lastTime;
	
	//��������
	private Connection connection;
	
	//����
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
			
			//��queue���tuple
			tupleQueue.add(input);
			//��ǰʱ��
			long current = System.currentTimeMillis();
			
			//�ж���1��һ�������ύ���ݿ�,�����еļ�¼���ڹ涨��sql����
			if(tupleQueue.size()>=  count  || current >= lastTime +1000){
				
				//����sql statement
				Statement st= connection.createStatement();
				//�����Զ��ύ
				connection.setAutoCommit(false);
				//ѭ�������ύ
				for(int i=0;i<count;i++){
					//�Ӷ����ó�����
					Tuple tuple = tupleQueue.poll();
					//����sql
					String sql=DBManager.getSql(tuple);
					//�������
					st.addBatch(sql);
					//�㱨�ɹ�
					collector.ack(tuple);
				}
				//����ִ��
				st.executeBatch();
				//�ύ
				connection.commit();
				//�����Զ��ύ
				connection.setAutoCommit(true);
				//�ı�ʱ��
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
