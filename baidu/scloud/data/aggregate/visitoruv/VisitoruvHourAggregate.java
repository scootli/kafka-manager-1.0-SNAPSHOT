package baidu.scloud.data.aggregate.visitoruv;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.aggregate.utils.UPVData;

/**
 * Get visitoruv hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 * @param hbaseClient: the hbase operator Client object
 */
public class VisitoruvHourAggregate implements HourAggregate{
	static Logger logger = Logger.getLogger(VisitoruvHourAggregate.class);
	private PhoenixClient phoenixClient;
	private static int DOMAIN_QUERY_LIMIT = 500;
	
	public VisitoruvHourAggregate(PhoenixClient phoenix_client){
		this.phoenixClient = phoenix_client;
		DOMAIN_QUERY_LIMIT = phoenixClient.getConf().getInt("domain_query_limit");
	}
	
	/**
	 * Get visitoruv hour domain data from domian hour data table. default yesterday
	 * @param date
	 * @param hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourDomainData(int date,int hour){
		//plan B: hbase coprocessor
		/*String condition = "date=" + date + " and hour=" + hour + " group by domain,region";
		String projectField = "domain, region";
		Vector<String> visitorIpDomain = phoenixClient.getExecuteResult("cf_rt_visitor_uv_v2",projectField,condition);
		
		logger.info("Visitor Uv Get domain and region successfully. date: " + date + " hour: " + hour);
		
		//HBase coprocessor get domain+date+hour+region ip_num
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		for(String domainRegion : visitorIpDomain){
			String[] tmpArray = domainRegion.split("-");
			String domain = tmpArray[0].trim();
			String region = tmpArray[1].trim();
			//logger.info("start Get uv_num and pv_num from cf_rt_visitor_uv_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region);
			UPVData upvNum = hbaseClient.getVisitorUvNum("cf_rt_visitor_uv_v2",domain,date,hour,region,"");
			//logger.info("end Get uv_num and pv_num from cf_rt_visitor_uv_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region);
			sqls.add(getInsertVisitorUvDomainSql(domain,date,hour,region,upvNum.getUv(),upvNum.getPv()));
			count++;
			if(count >= BATCHUNIT){//execute sql
				phoenixClient.executeBatch(sqls);
				logger.info("Visitor Uv domain insert phoenix successfully. one sql is: " + sqls.get(0));
				count = 0;
				sqls.clear();
			}
		}
		if(count > 0 ){
			//execute last times
			phoenixClient.executeBatch(sqls);
			sqls.clear();
		}*/
		
		logger.info("enter visitoruv domain aggregate");
		//plan C: divide all domains into many parts and parallel process
		Vector<String> domainVect = phoenixClient.getDomainByHour("cf_rt_access_v2",date,hour);
		if(domainVect.isEmpty()){
			logger.error("No vistitor uv domain data. date: " + date + " hour: " + hour);
			return 1;
		}
		int domainLen = domainVect.size();
		int current = 0;
		String domainStr = "(";
		
		int poolSize = 0;
		Vector<String> domainStrVect  = new Vector<String>();
		
		logger.info("visitor uv domain start count uv_key pv_num");
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
		
		//start all visitor uv domain aggregate task
		ExecutorService uvpDomainPool = Executors.newFixedThreadPool(poolSize);
		for(int index = 0;index < poolSize;index++){
			uvpDomainPool.execute(new VisitoruvCellDomainAggregate(phoenixClient,domainStrVect.get(index),date,hour));
		}
		
		//wait all thread tasks complete
		uvpDomainPool.shutdown();
		try {
			uvpDomainPool.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("visitor uv domain Generate a Interrupted Requests");
		}
		
		logger.info("leave visitoruv domain aggregate");
		return 0;
	}
	
	/**
	 * Get visitoruv hour site data from site hour data table. default yesterday
	 * @param date
	 * @param hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourSiteData(int date,int hour){
		//plan B: hbase coprocessor
		/*String condition = "date=" + date + " and hour=" + hour + " group by domain,site,region";
		String projectField = "domain, site,region";
		Vector<String> visitorIpDomain = phoenixClient.getExecuteResult("cf_rt_visitor_uv_v2",projectField,condition);
		
		logger.info("Visitor Uv Get domain site and region successfully. date: " + date + " hour: " + hour);
		
		//HBase coprocessor get domain+date+hour+region ip_num
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		for(String domainRegion : visitorIpDomain){
			String[] tmpArray = domainRegion.split("-");
			String domain = tmpArray[0].trim();
			String site = tmpArray[1].trim();
			String region = tmpArray[2].trim();
			//logger.info("start Get uv_num and pv_num from cf_rt_visitor_uv_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region + " site=" + site);
			UPVData upvNum = hbaseClient.getVisitorUvNum("cf_rt_visitor_uv_v2",domain,date,hour,region,site);
			//logger.info("end Get uv_num and pv_num from cf_rt_visitor_uv_v2 and date=" + date + " hour=" + 
			//		hour + " domain=" + domain + " region=" + region + " site=" + site);
			sqls.add(getInsertVisitorUvSiteSql(domain,site,date,hour,region,upvNum.getUv(),upvNum.getPv()));
			count++;
			if(count >= BATCHUNIT){//execute sql
				phoenixClient.executeBatch(sqls);
				logger.info("Visitor Uv site insert phoenix successfully. one sql is: " + sqls.get(0));
				count = 0;
				sqls.clear();
			}
			
		}
		if(count > 0 ){
			//execute last times
			phoenixClient.executeBatch(sqls);
			sqls.clear();
		}*/
		
		logger.info("enter visitoruv site aggregate");
		//plan C: divide all domains into many parts and parallel process
		Vector<String> domainVect = phoenixClient.getDomainByHour("cf_rt_access_v2",date,hour);
		if(domainVect.isEmpty()){
			logger.error("No vistitor uv site data. date: " + date + " hour: " + hour);
			return 1;
		}
		int domainLen = domainVect.size();
		int current = 0;
		String domainStr = "(";
		
		int poolSize = 0;
		Vector<String> domainStrVect  = new Vector<String>();
		
		logger.info("visitor uv site start count uv_key pv_num");
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
		
		
		//start all visitor uv site aggregate task
		ExecutorService uvpSitePool = Executors.newFixedThreadPool(poolSize);
		for(int index = 0;index < poolSize;index++){
			uvpSitePool.execute(new VisitoruvCellSiteAggregate(phoenixClient,domainStrVect.get(index),date,hour));
		}
		
		//wait all thread tasks complete
		uvpSitePool.shutdown();
		try {
			uvpSitePool.awaitTermination(Long.MAX_VALUE,TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("visitor uv site Generate a Interrupted Requests");
		}
	
		logger.info("leave visitoruv site aggregate");	
		return 0;
	}
}
