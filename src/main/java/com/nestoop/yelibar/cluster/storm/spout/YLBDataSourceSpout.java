package com.nestoop.yelibar.cluster.storm.spout;

import java.util.Map;
import java.util.UUID;

import com.nestoop.yelibar.cluster.storm.base.YLBEventHandlerSpout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 数据入口
 * @author xbao
 *
 */
public class YLBDataSourceSpout extends YLBEventHandlerSpout{
	
	
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	
	String code="";

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.collector = collector;
		code=UUID.randomUUID().toString();
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		//像下游发送数据
		Values  tuple=new Values();
		collector.emit(tuple);
		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("randomcode"));
	}

}
