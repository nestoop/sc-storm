package com.nestoop.yelibar.cluster.storm.base;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class YLBEventHandlerSpout extends BaseRichSpout implements ISpout{

	public Log logger=LogFactory.getLog(this.getClass());
	
	private static final long serialVersionUID = 1L;
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}

	@Override
	public void nextTuple() {
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		super.close();
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		super.activate();
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		super.deactivate();
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		super.fail(msgId);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		
		return super.getComponentConfiguration();
	}
	
	

}
