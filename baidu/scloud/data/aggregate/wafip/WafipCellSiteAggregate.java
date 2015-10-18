package baidu.scloud.data.aggregate.wafip;

import java.util.Vector;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.client.PhoenixClient;
import baidu.scloud.data.aggregate.heap.MinHeap;
import baidu.scloud.data.aggregate.heap.WafTopIp;

/**
 * concurrent process visitor page
 * 
 *@param: PhoenixClient: the Phoenix operator Client object
 *@param domainStr: where condition domain list
 *@param date:
 *@param topk:
 */

public class WafipCellSiteAggregate implements Runnable{
	static Logger logger = Logger.getLogger(WafipCellDomainAggregate.class);
	private PhoenixClient phoenixClient;
	private static int BATCHUNIT = 100;
	private String domainStr;
	private int date;
	private int topk;
	
	public WafipCellSiteAggregate(PhoenixClient phoenix_client,String domain_str,int date,int topk){
		phoenixClient = phoenix_client;
		domainStr = domain_str;
		this.date = date;
		this.topk = topk;
	}
	
	public void run() {
		String whereConds = "date=" + date + " and domain in " + domainStr +  " group by domain,site,ip";
		String projectField = "domain,site,ip, SUM(attack_count)";
		Vector<String> wafIpSumDomain = phoenixClient.getExecuteResult("cf_rt_waf_v2",projectField,whereConds);
		if(wafIpSumDomain.isEmpty()){
			logger.error("No waf ip site data. date: " + date);
			return;
		}
		
		String preDomain = "";
		String curDomain = "";
		String preSite = "";
		String curSite = "";
		int current = 0;
		WafTopIp[] data = new WafTopIp[topk];
		MinHeap<WafTopIp> heap = null;
		
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		//traverse sql result
		for(String vpData : wafIpSumDomain){
			String[] topIpArr = vpData.split("#");
			int len = topIpArr.length;
			curDomain = topIpArr[0].trim();
			curSite = topIpArr[1].trim();
			if(len != 4 || curDomain == "" || curSite == ""){
				continue;
			}
			
			//when saw next domain site, write preDomain preSite to phoenix
			if(!curDomain.equalsIgnoreCase(preDomain) || !curSite.equalsIgnoreCase(preSite)){
				if(!preDomain.equalsIgnoreCase("") && !preSite.equalsIgnoreCase("")){
					sqls.add(getInsertWafIpSiteSql(data,preDomain,preSite,date,topk,current));
					count++;
					if(count >= BATCHUNIT){//execute sql
						int result = phoenixClient.executeBatch(sqls);
						if(result > 0){
							logger.error("phoenix connect error. waf ip site. date: " + date + " topk: " + topk);
							return;
						}
						count = 0;
						sqls.clear();
					}
					data = new WafTopIp[topk];
					heap = null;
					current = 0;
				}
				preDomain = curDomain;
				preSite = curSite;
			}
			//Get topk uri with minHeap
			WafTopIp value = new WafTopIp(Long.parseLong(topIpArr[2].trim()),
				Long.parseLong(topIpArr[3].trim()));
			if(current < topk){
				data[current] = value;
				if(current == (topk - 1)){
					//convert min heap
					heap = new MinHeap<WafTopIp>(data);
				}
			}/*else if(current == topk){
				//convert min heap
				heap = new MinHeap<WafTopIp>(data);
			}*/else{
				//when value great than root, replace the root and retidy the heap
				WafTopIp root = heap.getRoot();
				if(root.less_override(value))
				{
				  heap.setRoot(value);
				}
			}
			current++;
		}
		
		if(count > 0){
			//execute last times
			sqls.add(getInsertWafIpSiteSql(data,curDomain,curSite,date,topk,current));
			int result = phoenixClient.executeBatch(sqls);
			if(result > 0){
				if(result > 0){
					logger.error("phoenix connect error. waf ip site. date: " + date + " topk: " + topk);
					return;
				}
			}
		}
	}
	
	/**
	 * insert waf ip(site) data into phoenix
	 * @param data: domain topk waf ip
	 * @param domain:
	 * @param site:
	 * @param date:
	 * @param topk:
	 * @param dataLen : data actual size
	 * 
	 * @return the insert sql
	 */
	public String getInsertWafIpSiteSql(WafTopIp[] data,String domain,String site,int date,int topk,int dataLen){
		String content = WafIpUtils.getTopWafIpJsonContent(data,topk,dataLen);
		String sql = "upsert into cf_rt_summary_waf_ip_of_day_site values(" + 
					domain + "," + date + "," + site + ",'" + content + "')";
		
		return sql;
	}
}
