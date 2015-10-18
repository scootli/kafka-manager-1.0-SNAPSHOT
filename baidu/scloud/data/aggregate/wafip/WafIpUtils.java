package baidu.scloud.data.aggregate.wafip;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import net.sf.json.JSONArray;
import baidu.scloud.data.aggregate.base64.Base64;
import baidu.scloud.data.aggregate.heap.WafTopIp;
import baidu.scloud.data.aggregate.utils.IPInfo;

public class WafIpUtils {
	static Logger logger = Logger.getLogger(WafIpUtils.class);
	/**
	 * Get top waf ip json and base64 result from topk minHeap
	 * @param data: domain topk waf ip
	 * @param topk: 
	 * @param dataLen: data actual size
	 * 
	 * @return topk waf ip json and base64 result
	 */
	public static String getTopWafIpJsonContent(WafTopIp[] data,int topk,int dataLen){
		long errorIp = 0L;
		int size = (dataLen > topk)? topk : dataLen;
		List<Map> list = new ArrayList<Map>();
		try{
			for(int index = 0;index < size;index++){
				Map<String,String> topIp2Count = new HashMap<String,String>();
				long curIp = data[index].get_ip();
				errorIp = curIp;
				topIp2Count.put("ip",curIp + "");
				topIp2Count.put("sum",data[index].get_attack_count() + "");
				String region = IPInfo.getIpRegion(curIp);
				topIp2Count.put("location",region);
				list.add(topIp2Count);
			}
		}catch(Exception ex){
			logger.error("Get region with ip error. ip is: " + errorIp + " Exception: " + ex);
		}
		//JSONArray jsonArray = JSONArray.fromObject(list);
		String content = JSONArray.fromObject(list).toString();
		return Base64.getBase64(content);
	}
}
