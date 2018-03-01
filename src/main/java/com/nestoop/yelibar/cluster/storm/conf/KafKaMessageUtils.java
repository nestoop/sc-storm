package com.nestoop.yelibar.cluster.storm.conf;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import kafka.message.Message;

/**
 * kafka message
 * @author xbao
 *
 */
public class KafKaMessageUtils {
	
	/**
	 * 消息转化成字符串
	 * @param message
	 * @return
	 */
	
	public static String getKafKaMessage(Message message){
		 
		ByteBuffer byteBuffer = message.payload();
		
		byte[] buffer= new byte[byteBuffer.remaining()];
		
		byteBuffer.get(buffer);
		
		try {
			return new String(buffer,YLBContants.CONFIG_BYTE_ENCODE);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
		 
	 }

}
