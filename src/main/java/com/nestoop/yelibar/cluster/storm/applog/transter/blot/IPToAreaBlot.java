package com.nestoop.yelibar.cluster.storm.applog.transter.blot;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.nestoop.yelibar.cluster.storm.base.YLBEventHandlerBlot;
import com.nestoop.yelibar.cluster.storm.db.DBManager;

/**
 * ip 转换成地域
 * @author Administrator
 *
 */
public class IPToAreaBlot implements IBasicBolt{

	private static final long serialVersionUID = 1L;

	private Statement st = null;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//声明下游blot 接受的国家
		declarer.declare(new Fields("country"));
		
	}

	/**
	 * 1413276006	18540852316	71-77-16-4c-41-b4:CMCC	10.116.136.202	alipay.com	支付	15	9	7161	4269	200
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		//获取每Tuple
		String line = input.toString();
		//得到ip
		String all[] =line.split("\t",-1);
		//获取ip
		long iplong = ipToLong(all[3]);
		//发送下blot
		collector.emit(new Values(getSelectArea(iplong)));
		
		
		
	}

	@SuppressWarnings("unused")
	private String getSelectArea(long ipp){
		StringBuffer sql=new StringBuffer("select country from app_ip_area where 1=1 ");
		sql.append(" and ( ").append(ipp).append(" between minip and  maxip )");
		try {
			ResultSet rs = st.executeQuery(sql.toString());
			String area=rs.getString(0);
			return area;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static long ipToLong(String strip){
		
		//ip数组
		Long[] ip  =  new Long[4];
		
		int par1 = strip.indexOf(".");
		int par2 = strip.indexOf(".",par1+1);
		int par3 = strip.indexOf(".",par2+1);
		
		//找到ip 第一位
		ip[0] = Long.parseLong(strip.substring(0, par1));
		//第二位
		ip[1] = Long.parseLong(strip.substring(par1+1, par2));
		//第三位
		ip[2] = Long.parseLong(strip.substring(par2+1, par3));
		//第四位
		ip[3] = Long.parseLong(strip.substring(par3+1));

		return (ip[0] <<24) +(ip[1] << 16)+(ip[2] << 8)+ip[3];
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void prepare(Map conf, TopologyContext ctx) {
		st = DBManager.getStatement();
		
	}
	
	

}
