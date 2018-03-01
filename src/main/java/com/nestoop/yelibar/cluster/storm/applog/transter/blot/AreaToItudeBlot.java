package com.nestoop.yelibar.cluster.storm.applog.transter.blot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.nestoop.yelibar.cluster.storm.base.YLBEventHandlerBlot;
import com.nestoop.yelibar.cluster.storm.conf.YLBConfig;
import com.nestoop.yelibar.cluster.storm.conf.YLBContants;
import com.nestoop.yelibar.cluster.storm.db.DBManager;

/**
 * ����;���γ��ת��
 * 
 * @author xbao
 *
 */
public class AreaToItudeBlot implements IBasicBolt{

	private static final long serialVersionUID = 1L;

	// �������沢������
	private Queue<Tuple> tupleQueue = new ConcurrentLinkedQueue<Tuple>();
	
	private HashMap<String, String> longitude = new HashMap<String, String>();

	// ���ʱ��
	private long lastTime;

	// ��������
	private Connection connection;

	// ����
	private int count;
	
	
	

	public AreaToItudeBlot(int num) {
		count = num;
		lastTime = System.currentTimeMillis();
	}
	
	

	private String getSql(Tuple tuple) {
		String country = tuple.getString(0);
		String sql= "";
		if(longitude.containsKey(country)){
			sql = "insert into (country,lng,lat) value ('"+longitude.get(country).split("\t",-1)[0]+"','"+longitude.get(country).split("\t",-1)[1]+"','"+longitude.get(country).split("\t",-1)[2]+"')";
		}
		return sql;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("country","lng","lat"));
	}

	@Override
	public void cleanup() {
	}

	

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//��ȡ���� ���� γ���ı�
		String uri = YLBConfig.getStormProperty(YLBContants.HADOOP_HDFS_STORM_URI)+"/lng-lat-mapping.txt";
		InputStream in = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(uri), new Configuration());
			in = fs.open(new Path(uri));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(in,"UTF-8"));
			String line = null;
			while (null != (line = br.readLine())) {
				longitude.put(line.split("\t", -1)[0], line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		System.out.println("���в������ݿ�...............");
		
		System.out.println("tupleQueue...:"+tupleQueue.size());

		try {
			// ��queue���tuple
			tupleQueue.add(input);
			// ��ǰʱ��
			long current = System.currentTimeMillis();

			// �ж���1��һ�������ύ���ݿ�,�����еļ�¼���ڹ涨��sql����
			if (tupleQueue.size() >= count || current >= lastTime + 1000) {

				// ����sql statement
				Statement st = connection.createStatement();
				// �����Զ��ύ
				connection.setAutoCommit(false);
				// ѭ�������ύ
				for (int i = 0; i < count; i++) {
					// �Ӷ����ó�����
					Tuple tuple = tupleQueue.poll();
					// ����sql
					String sql = getSql(tuple);
					// �������
					if(sql != null && !"".equals(sql)){
						st.addBatch(sql);
					}
				}
				// ����ִ��
				st.executeBatch();
				// �ύ
				connection.commit();
				// �����Զ��ύ
				connection.setAutoCommit(true);
				// �ı�ʱ��
				lastTime = current;

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
