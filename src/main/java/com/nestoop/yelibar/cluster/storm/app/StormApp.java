package com.nestoop.yelibar.cluster.storm.app;


import com.nestoop.yelibar.cluster.storm.applog.kafka.spout.AppLogSpout;
import com.nestoop.yelibar.cluster.storm.applog.transter.blot.AreaToItudeBlot;
import com.nestoop.yelibar.cluster.storm.applog.transter.blot.IPToAreaBlot;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * storm ²âÊÔ
 * @author xbao
 *
 */
public class StormApp {
	
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new AppLogSpout(), 4);
		//4 the number of tasks that should be assigned to execute this spout
		builder.setBolt("area-bolt", new IPToAreaBlot(),8).shuffleGrouping("spout");
		builder.setBolt("longitude-bolt", new AreaToItudeBlot(100),12).shuffleGrouping("area-bolt");

		Config config = new Config();
		config.setDebug(true);
		// set producer properties.
		 if (args != null && args.length > 0) {
			 config.setNumWorkers(4);
			 config.setMaxSpoutPending(1000);
			 config.put(Config.NIMBUS_HOST, "node2");
			 System.setProperty("storm.jar","F:\\applog.jar");  
			 try {
				 StormSubmitter.submitTopology("area-topology", config,
						 builder.createTopology());
			 } catch (AlreadyAliveException | InvalidTopologyException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
			 
		 }else{
			  config.setMaxTaskParallelism(3);
		      LocalCluster cluster = new LocalCluster();
		      cluster.submitTopology("area-topology", config, builder.createTopology());
			 
		 }
//		LocalCluster localCluster = new LocalCluster();
//		localCluster.submitTopology("area-topology", config, builder.createTopology());
	}

}
