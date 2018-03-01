package com.nestoop.yelibar.cluster.storm.applog.kafka.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.nestoop.yelibar.cluster.storm.base.YLBEventHandlerSpout;
import com.nestoop.yelibar.cluster.storm.conf.YLBConfig;
import com.nestoop.yelibar.cluster.storm.conf.YLBContants;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 数据来源
 * @author xbao
 *
 */
public class AppLogSpout extends YLBEventHandlerSpout{
	
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	public void ack(Object msgId) {
		super.ack(msgId);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		super.close();
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		super.deactivate();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ip"));
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		super.fail(msgId);
	}

	@Override
	public void nextTuple() {
		
		System.out.println("开始读取hdfs...............");
		//读取hdfs 上的文件
		InputStream in = null;
		//HDFS文件地址
		String ip_area_isp = "ip_area_isp.txt";
		String  uri = YLBConfig.getStormProperty(YLBContants.HADOOP_HDFS_STORM_URI)+"/"+ip_area_isp;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			BufferedReader br = new BufferedReader(new InputStreamReader(in, YLBContants.CONFIG_STREAM_ENCODE));
			String line = null;
			while (null != (line = br.readLine())) {
				//发送数据
				collector.emit(new Values(line));
				//等待100毫秒
				Utils.sleep(100);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void activate() {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return super.getComponentConfiguration();
	}
	
	


}
