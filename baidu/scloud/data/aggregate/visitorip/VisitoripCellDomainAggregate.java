package baidu.scloud.data.aggregate.visitorip;

import java.util.Vector;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.aggregate.utils.CommonUtils;

/**
 * concurrent process visitor ip
 * 
 *@param: PhoenixClient: the Phoenix operator Client object
 *@param domainStr: where condition domain list
 *@param date:
 *@param hour
 */
public class VisitoripCellDomainAggregate implements Runnable{
	static Logger logger = Logger.getLogger(VisitoripCellDomainAggregate.class);
	private PhoenixClient phoenixClient;
	private static long CURRENTHOUR = System.currentTimeMillis();//CommonUtils.TimeStamp2Date(System.currentTimeMillis());
	private static int BATCHUNIT = 100;
	private String domainStr;
	private int date;
	private int hour;
	
	public VisitoripCellDomainAggregate(PhoenixClient phoenix_client,String domain_str,int date,int hour){
		phoenixClient = phoenix_client;
		domainStr = domain_str;
		this.date = date;
		this.hour = hour;
	}
	
	public void run() {
		logger.info("child thread start. Thead id is : " + Thread.currentThread().getId());
		
		String condition = "date=" + date + " and hour=" + hour + " and domain in" + domainStr + " group by domain,region";
		String projectField = "domain, region,count(distinct ip)";
		Vector<String> visitorIpResult = phoenixClient.getExecuteResult("cf_rt_visitor_ip_v2",projectField,condition);
		if(visitorIpResult.isEmpty()){
			logger.error("No vistitor ip domain data. date: " + date + " hour: " + hour);
			return;
		}
		
		logger.info("SQL finished. Thead id is : " + Thread.currentThread().getId());
		
		//batch insert phoenix
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		//write to phoenix
		for(String vipResult : visitorIpResult){
			String[] tmpArray = vipResult.split("#");
			String domain = tmpArray[0].trim();
			String region = tmpArray[1].trim();
			String ipNum = tmpArray[2].trim();
			sqls.add(getInsertVisitorIpDomainSql(domain,date,hour,region,ipNum));
			count++;
			if(count >= BATCHUNIT){//execute sql
				int result = phoenixClient.executeBatch(sqls);
				if(result > 0){
					logger.error("phoenix connect error. vistitor ip domain. date: " + date + " hour: " + hour);
					return;
				}
				//logger.info("Visitor Ip domain insert phoenix successfully. one sql is: " + sqls.get(0));
				count = 0;
				sqls.clear();
			}
		}
		
		logger.info("SQL process finished. Thead id is : " + Thread.currentThread().getId());
		
		if(count > 0 ){
			//execute last times
			int result =  phoenixClient.executeBatch(sqls);
			if(result > 0){
				logger.error("phoenix connect error. vistitor ip domain. date: " + date + " hour: " + hour);
				return;
			}
			sqls.clear();
		}
		
		logger.info("child thread finished. Thead id is : " + Thread.currentThread().getId());
	}
	
	/**
	 * get insert visitor ip(domain) data into phoenix sql
	 * @param domain
	 * @param date
	 * @param hour
	 * @param region
	 * @param ipNum
	 * 
	 * @return sql : insert sql
	 */
	public String  getInsertVisitorIpDomainSql(String domain,int date,int hour,String region,String ipNum){
		String insertVal = "";
		//access aggregating data: from req_hit_num to req_total_bandwidth
		for(int index = 0;index < 10;index++){
			insertVal += (0 + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num 
		insertVal += ("'ALL'" + "," + 0 + "," + "'" + region + "'" + "," + ipNum + "," + 0 + "," + 0 + ",");
		//attack_size attack_num domain date hour time
		insertVal += (0 + "," + 0 + "," + domain + "," + date + "," + hour + "," + CURRENTHOUR);
		
		String sql = "upsert into cf_rt_summary_of_hour_domain values(" + insertVal + ")";
		
		return sql;
	}
}
