package baidu.scloud.data.aggregate.access;

import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.aggregate.utils.CommonUtils;

/**
 * Get access hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 */
public class AccessHourAggregate implements HourAggregate{
	static Logger logger = Logger.getLogger(AccessHourAggregate.class);
	private static int BATCHUNIT = 100;
	private static long CURRENTHOUR = System.currentTimeMillis();//CommonUtils.TimeStamp2Date(System.currentTimeMillis());
	private PhoenixClient phoenixClient;
	
	public AccessHourAggregate(PhoenixClient phoenix_client){
		phoenixClient = phoenix_client;
	}
	
	/**
	 * Get access domain hour data from domain hour data table. default yesterday
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourDomainData(int date,int hour){
		logger.info("enter access domain aggregate");
		//Get hour aggregate data
		String whereConds = "date=" + date + " and hour=" + hour + " group by domain";
		String projectField = "domain, sum(req_hit_count), sum(req_total_count),sum(req_refer_count), " +
				"sum(req_direct_count), sum(req_hit_size),sum(req_total_size), sum(req_hit_time), " +
				"sum(req_total_time),max(req_hit_bandwidth), max(req_total_bandwidth)";
		Vector<String> accessHourDomainData = phoenixClient.getExecuteResult("cf_rt_access_v2",projectField,whereConds);
		if(accessHourDomainData.isEmpty()){
			logger.error("No access domain data. date: " + date + " hour: " + hour);
			return 1;
		}
		//traverse aggregate data
		Iterator<String> it = accessHourDomainData.iterator();
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
		logger.info("leave access domain aggregate");
		return 0;
	}
	
	/**
	 * Get access site hour data from site hour data table. default yesterday
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourSiteData(int date,int hour){
		logger.info("enter access site aggregate");
		//Get hour aggregate data
		String whereConds = "date=" + date + " and hour=" + hour + " group by domain,site";
		String projectField = "domain, site,sum(req_hit_count), sum(req_total_count),sum(req_refer_count), " +
				"sum(req_direct_count), sum(req_hit_size),sum(req_total_size), sum(req_hit_time), " +
				"sum(req_total_time),max(req_hit_bandwidth), max(req_total_bandwidth)";
		Vector<String> accessHourSiteData = phoenixClient.getExecuteResult("cf_rt_access_v2",projectField,whereConds);
		if(accessHourSiteData.isEmpty()){
			logger.error("No access site data. date: " + date + " hour: " + hour);
			return 1;
		}
		//traverse aggregate data
		Iterator<String> it = accessHourSiteData.iterator();
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
		logger.info("leave access site aggregate");
		return 0;
	}
	
	/**
	 * get insert domain hour aggregating data sql
	 * @param domainData :  a aggregating record 
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return insertSql
	 */
	private String getHourSummaryDomainSql(String domainData,int date,int hour) {
		String[] dataArr = domainData.split("#");
		String insertValue = "";
		int dataLen = dataArr.length;
		if(dataLen != 11)
			logger.info("dataLen is : " + dataLen);
		for(int index = 1; index < dataLen;index++){
			insertValue += (dataArr[index].trim() + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num attack_size attack_num
		insertValue += ("'ALL'" + "," + 0 + "," + "'ALL'" + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + ",");
		//domain  date  hour time
		insertValue += (dataArr[0].trim() + "," + date + "," + hour + "," + CURRENTHOUR);
		String sql = "upsert into cf_rt_summary_of_hour_domain values(" + insertValue + ")"; 
		return sql;
	}
	
	/**
	 * get insert site hour aggregating data sql
	 * @param domainData :  a aggregating record 
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return insertSql
	 */
	private String getHourSummarySiteSql(String siteData,int date,int hour) {
		String[] dataArr = siteData.split("#");
		String insertValue = "";
		int dataLen = dataArr.length;
		for(int index = 2;index < dataLen;index++){
			insertValue += (dataArr[index].trim() + ",");
		}
		
		//se_type req_se_num region ip_num pv_num uv_num attack_size attack_num
		insertValue += ("'ALL'" + "," + 0 + "," + "'ALL'" + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + ",");
		//domain  site, date  hour time
		insertValue += (dataArr[0].trim() + "," + dataArr[1].trim() + "," + date + "," + hour + "," + CURRENTHOUR);
		
		String sql = "upsert into cf_rt_summary_of_hour_site values(" + insertValue + ")"; 
		
		return sql;
	}
}
