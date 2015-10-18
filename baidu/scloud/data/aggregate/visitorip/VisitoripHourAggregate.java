package baidu.scloud.data.aggregate.visitorip;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.mapreduce.visitorip.domain.CountDistnctJob;

/**
 * Get visitorip hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 * @param hbaseClient: the hbase operator Client object
 */
public class VisitoripHourAggregate implements HourAggregate{
	static Logger logger = Logger.getLogger(VisitoripHourAggregate.class);
	private PhoenixClient phoenixClient;
	private static int DOMAIN_QUERY_LIMIT = 500;
	
	public VisitoripHourAggregate(PhoenixClient phoenix_client){
		this.phoenixClient = phoenix_client;
		DOMAIN_QUERY_LIMIT = phoenixClient.getConf().getInt("domain_query_limit");
	}
	
	/**
	 * Get visitorip hour domain data from domain hour data table. default yesterday
	 * @param date
	 * @param hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourDomainData(int date,int hour){
		//plan A: hbase mapreduce + write from hbase data to phoenix  
		/*CountDistnctJob cdj = new CountDistnctJob();
		logger.info("visitor ip mapreduce start");
		cdj.getDistnctCount(hbaseClient.conf,"cf_rt_visitor_ip_v2");
		logger.info("visitor ip mapreduce end");*/
		
		//plan B: hbase coprocessor
		/*String condition = "date=" + date + " and hour=" + hour + " group by domain,region";
		String projectField = "domain, region";
		Vector<String> visitorIpDomain = phoenixClient.getExecuteResult("cf_rt_visitor_ip_v2",projectField,condition);
		
		logger.info("Visitor Ip Get domain and region successfully. date: " + date + " hour: " + hour);
		
		//HBase coprocessor get domain+date+hour+region ip_num
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		for(String domainRegion : visitorIpDomain){
			String[] tmpArray = domainRegion.split("-");
			String domain = tmpArray[0].trim();
			String region = tmpArray[1].trim();
			//logger.info("start Get ip_num from cf_rt_visitor_ip_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region);
			long ipNum = hbaseClient.getVisitorIpNum("cf_rt_visitor_ip_v2",domain,date,hour,region,"");
			//logger.info("end Get ip_num from cf_rt_visitor_ip_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region);
			sqls.add(getInsertVisitorIpDomainSql(domain,date,hour,region,ipNum));
			count++;
			if(count >= BATCHUNIT){//execute sql
				phoenixClient.executeBatch(sqls);
				logger.info("Visitor Ip domain insert phoenix successfully. one sql is: " + sqls.get(0));
				count = 0;
				sqls.clear();
			}
		}
		if(count > 0 ){
			//execute last times
			phoenixClient.executeBatch(sqls);
			sqls.clear();
		}*/
		
		logger.info("enter visitorip domain aggregate");
		//plan C: divide all domains into many parts
		Vector<String> domainVect = phoenixClient.getDomainByHour("cf_rt_access_v2",date,hour);
		if(domainVect.isEmpty()){
			logger.error("No vistitor ip domain data. date: " + date + " hour: " + hour);
			return 1;
		}
		int domainLen = domainVect.size();
		int current = 0;
		String domainStr = "(";
		
		int poolSize = 0;
		Vector<String> domainStrVect  = new Vector<String>();
		
		logger.info("visitor ip domain start count distinct");
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
		
		//start all visitor ip domain aggregate task
		ExecutorService vipDomainPool = Executors.newFixedThreadPool(poolSize);
		for(int index = 0;index < poolSize;index++){
			vipDomainPool.execute(new VisitoripCellDomainAggregate(phoenixClient,domainStrVect.get(index),date,hour));
		}
		
		//wait all thread tasks complete
		vipDomainPool.shutdown();
		try {
			vipDomainPool.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("visitor ip domain Generate a Interrupted Requests");
		}
		
		logger.info("leave visitorip domain aggregate");
		
		return 0;
	}
	
	/**
	 * Get visitorip hour site data from site hour data table. default yesterday
	 * @param date
	 * @param hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourSiteData(int date,int hour){
		//plan A: hbase mapreduce + write from hbase data to phoenix  
		/*CountDistnctJob cdj = new CountDistnctJob();
		logger.info("visitor ip mapreduce start");
		cdj.getDistnctCount(hbaseClient.conf,"cf_rt_visitor_ip_v2");
		logger.info("visitor ip mapreduce end");*/
		
		//plan B: hbase coprocessor 
		/*String condition = "date=" + date + " and hour=" + hour + " group by domain,site,region";
		String projectField = "domain, site,region";
		Vector<String> visitorIpSite = phoenixClient.getExecuteResult("cf_rt_visitor_ip_v2",projectField,condition);
		
		logger.info("Visitor Ip Get domain site and region successfully. date: " + date + " hour: " + hour);
		
		//HBase coprocessor get domain+date+hour+region+site ip_num
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		for(String domainRegion : visitorIpSite){
			String[] tmpArray = domainRegion.split("-");
			String domain = tmpArray[0].trim();
			String site = tmpArray[1].trim();
			String region = tmpArray[2].trim();
			//logger.info("start Get ip_num from cf_rt_visitor_ip_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region + " site=" + site);
			long ipNum = hbaseClient.getVisitorIpNum("cf_rt_visitor_ip_v2",domain,date,hour,region,site);
			//logger.info("end Get ip_num from cf_rt_visitor_ip_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region + " site=" + site);
			sqls.add(getInsertVisitorIpSiteSql(domain,site,date,hour,region,ipNum));
			count++;
			if(count >= BATCHUNIT){//execute sql
				phoenixClient.executeBatch(sqls);
				logger.info("Visitor Ip site insert phoenix successfully. one sql is: " + sqls.get(0));
				count = 0;
				sqls.clear();
			}
		}
		if(count > 0 ){
			//execute last times
			phoenixClient.executeBatch(sqls);
			sqls.clear();
		}*/
		
		logger.info("enter visitorip site aggregate");
		//plan C: divide all domains into many parts
		Vector<String> domainVect = phoenixClient.getDomainByHour("cf_rt_access_v2",date,hour);
		if(domainVect.isEmpty()){
			logger.error("No vistitor ip site data. date: " + date + " hour: " + hour);
			return 1;
		}
		int domainLen = domainVect.size();
		int current = 0;
		String domainStr = "(";
		
		int poolSize = 0;
		Vector<String> domainStrVect  = new Vector<String>();
		
		logger.info("visitor ip site start count distinct");
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
		
		//start all visitor ip site aggregate task
		ExecutorService vipSitePool = Executors.newFixedThreadPool(poolSize);
		for(int index = 0;index < poolSize;index++){
			vipSitePool.execute(new VisitoripCellSiteAggregate(phoenixClient,domainStrVect.get(index),date,hour));
		}
		
		//wait all thread tasks complete
		vipSitePool.shutdown();
		try {
			vipSitePool.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("visitor ip site Generate a Interrupted Requests");
		}
		
		logger.info("leave visitorip site aggregate");
		
		return 0;
	}
}
