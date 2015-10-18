package baidu.scloud.data.aggregate.wafip;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.DayAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;

/**
 * Get top waf ip hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 */
public class WafipDayAggregate implements DayAggregate{
	static Logger logger = Logger.getLogger(WafipDayAggregate.class);
	private static int DOMAIN_QUERY_LIMIT = 100;
	private PhoenixClient phoenixClient;
	
	public WafipDayAggregate(org.apache.commons.configuration.PropertiesConfiguration config){
		phoenixClient = new PhoenixClient(config);
		DOMAIN_QUERY_LIMIT = phoenixClient.getConf().getInt("domain_query_limit");
	}
	
	/**
	 * Get Top Waf ip domain hour data from hour data table. default yesterday
	 * @param date:
	 * @param topk: calculate topk waf ip with heap 
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryDayDomainData(int date,int topk){
		logger.info("enter wafip domain aggregate");
		//divide all domain into many parts
		Vector<String> domainVect = phoenixClient.getDomainByDay("cf_rt_waf_v2",date);
		if(domainVect.isEmpty()){
			logger.error("No waf ip domain data. date: " + date);
			return 1;
		}
		int domainLen = domainVect.size();
		
		
		int current = 0;
		String domainStr = "("; 
		
		int poolSize = 0;
		Vector<String> domainStrVect  = new Vector<String>();
		
		logger.info("waf ip domain start top ip");
		while(current < domainLen){
			int end = (domainLen > (current + DOMAIN_QUERY_LIMIT)?  current + DOMAIN_QUERY_LIMIT: domainLen);
			for(int index = current;index < end;index++){
				if(index != end - 1){
					domainStr += (domainVect.get(index) + ",");
				}else{
					domainStr += (domainVect.get(index) + ")");
				}
			}
			poolSize++;
			domainStrVect.add(domainStr);
			
			current += DOMAIN_QUERY_LIMIT;
			domainStr = "(";
		}
		
		//start all waf ip domain aggregate task
		ExecutorService wafIpDomainPool = Executors.newFixedThreadPool(poolSize);
		for(int index = 0;index < poolSize;index++){
			wafIpDomainPool.execute(new WafipCellDomainAggregate(phoenixClient,domainStrVect.get(index),date,topk));
		}
		
		//wait all thread tasks complete
		wafIpDomainPool.shutdown();
		try {
			wafIpDomainPool.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("waf ip domain Generate a Interrupted Requests");
		}
		
		logger.info("enter wafip domain aggregate");
		return 0;
	}
	
	/**
	 * Get Top Waf ip domain hour data from hour data table. default yesterday
	 * @param date:
	 * @param topk:
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryDaySiteData(int date,int topk){
		logger.info("enter wafip site aggregate");
		//divide all domain into many parts
		Vector<String> domainVect = phoenixClient.getDomainByDay("cf_rt_waf_v2",date);
		if(domainVect.isEmpty()){
			logger.error("No waf ip site data. date: " + date);
			return 1;
		}
		int domainLen = domainVect.size();
		
		int current = 0;
		String domainStr = "("; 
		
		int poolSize = 0;
		Vector<String> domainStrVect  = new Vector<String>();
		
		logger.info("waf ip site start top uri");
		while(current < domainLen){
			int end = (domainLen > (current + DOMAIN_QUERY_LIMIT)?  current + DOMAIN_QUERY_LIMIT: domainLen);
			for(int index = current;index < end;index++){
				if(index != end - 1){
					domainStr += (domainVect.get(index) + ",");
				}else{
					domainStr += (domainVect.get(index) + ")");
				}
			}
			
			poolSize++;
			domainStrVect.add(domainStr);
			
			current += DOMAIN_QUERY_LIMIT;
			domainStr = "(";
		}
		
		//start all waf ip site aggregate task
		ExecutorService wafIpSitePool = Executors.newFixedThreadPool(poolSize);
		for(int index = 0;index < poolSize;index++){
			wafIpSitePool.execute(new WafipCellSiteAggregate(phoenixClient,domainStrVect.get(index),date,topk));
		}
		
		//wait all thread tasks complete
		wafIpSitePool.shutdown();
		try {
			wafIpSitePool.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("Waf ip site Generate a Interrupted Requests");
		}
		
		logger.info("leave wafip site aggregate");
		return 0;
	}
}
