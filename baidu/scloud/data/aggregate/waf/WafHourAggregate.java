package baidu.scloud.data.aggregate.waf;

import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;

/**
 * Get waf hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 */
public class WafHourAggregate implements HourAggregate{
	static Logger logger = Logger.getLogger(WafHourAggregate.class);
	private static int BATCHUNIT = 100;
	private PhoenixClient phoenixClient;
	
	public WafHourAggregate(PhoenixClient phoenix_client){
		phoenixClient = phoenix_client;
	}
	
	/**
	 * Get waf domain hour data from domain hour data table. default yesterday
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourDomainData(int date,int hour){
		logger.info("enter waf domain aggregate");
		//Get hour aggregate data
		String whereConds = "date=" + date + " and hour=" + hour + " group by domain,type";
		String projectField = "domain, type, sum(attack_count)";
		Vector<String> wafHourDomainData = phoenixClient.getExecuteResult("cf_rt_waf_v2",projectField,whereConds);
		if(wafHourDomainData.isEmpty()){
			logger.error("No waf domain data. date: " + date + " hour: " + hour);
			return 1;
		}
		
		//traverse aggregate data
		Iterator<String> it = wafHourDomainData.iterator();
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		while(it.hasNext()){
			String domainData = it.next();
			sqls.add(getHourSummaryDomainSql(domainData,date,hour));
			count++;
			if(count >= BATCHUNIT){
				int result = phoenixClient.executeBatch(sqls);
				if(result > 0){
					return result;
				}
				count = 0;
				sqls.clear();
			}
		}
		if(count > 0){
			//execute last times
			int result = phoenixClient.executeBatch(sqls);
			if(result > 0){
				return result;
			}
			sqls.clear();
		}
		logger.info("leave waf domain aggregate");
		return 0;
	}
	
	/**
	 * Get waf site hour data from site hour data table. default yesterday
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 */
	public int summaryHourSiteData(int date,int hour){	
		logger.info("enter waf site aggregate");
		//Get hour aggregate data
		String whereConds = "date=" + date + " and hour=" + hour + " group by domain,type,site";
		String projectField = "domain,site,type,sum(attack_count)";
		Vector<String> wafHourSiteData = phoenixClient.getExecuteResult("cf_rt_waf_v2",projectField,whereConds);
		if(wafHourSiteData.isEmpty()){
			logger.error("No waf site data. date: " + date + " hour: " + hour);
			return 1;
		}
		
		//traverse aggregate data
		Iterator<String> it = wafHourSiteData.iterator();
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		while(it.hasNext()){
			String siteData = it.next();
			sqls.add(getHourSummarySiteSql(siteData,date,hour));
			count++;
			if(count >= BATCHUNIT){
				int result = phoenixClient.executeBatch(sqls);
				if(result > 0){
					return result;
				}
				count = 0;
				sqls.clear();
			}
		}
		if(count > 0){
			//execute last times
			int result = phoenixClient.executeBatch(sqls);
			if(result > 0){
				return result;
			}
			sqls.clear();
		}
		logger.info("leave waf site aggregate");
		return 0;
	}
	
	/**
	 * get insert domain hour aggregating data sql
	 * @param domainData :  a aggregating record 
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 */
	private String getHourSummaryDomainSql(String domainData,int date,int hour) {
		String[] dataArr = domainData.split("#");
		String insertValue = "";
		int dataLen = dataArr.length;
		for(int index = 0; index < dataLen;index++){
			//type
			if(index == 1){
				insertValue += ( "'" + dataArr[index].trim() + "',");
			}else{
				insertValue += (dataArr[index].trim() + ",");
			}
		}
		
		insertValue += (date + "," + hour);
		String sql = "upsert into cf_summary_of_hour_domain_waf_v2 values(" + insertValue + ")"; 
		
		return sql;
	}
	
	/**
	 * get insert site hour aggregating data sql
	 * @param domainData :  a aggregating record 
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 */
	private String getHourSummarySiteSql(String siteData,int date,int hour) {
		String[] dataArr = siteData.split("#");
		String insertValue = "";
		int dataLen = dataArr.length;
		for(int index = 0; index < dataLen;index++){
			//type
			if(index == 2){
				insertValue += ( "'" + dataArr[index].trim() + "',");
			}else{
				insertValue += (dataArr[index].trim() + ",");
			}
		}
		
		insertValue += (date + "," + hour);
		String sql = "upsert into cf_summary_of_hour_site_waf_v2 values(" + insertValue + ")"; 
		
		return sql;
	}
}
