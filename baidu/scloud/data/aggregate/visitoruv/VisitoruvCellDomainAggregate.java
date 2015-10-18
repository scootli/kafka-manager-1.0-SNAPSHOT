package baidu.scloud.data.aggregate.visitoruv;

import java.util.Vector;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.aggregate.utils.CommonUtils;

/**
 * concurrent process visitor uv 
 * 
 *@param: PhoenixClient: the Phoenix operator Client object
 *@param domainStr: where condition domain list
 *@param date:
 *@param hour
 */
public class VisitoruvCellDomainAggregate implements Runnable {
	static Logger logger = Logger.getLogger(VisitoruvCellDomainAggregate.class);
	private PhoenixClient phoenixClient;
	private static long CURRENTHOUR = System.currentTimeMillis();//CommonUtils.TimeStamp2Date(System.currentTimeMillis());
	private static int BATCHUNIT = 100;
	private String domainStr;
	private int date;
	private int hour;
	
	public VisitoruvCellDomainAggregate(PhoenixClient phoenix_client,String domain_str,int date,int hour){
		phoenixClient = phoenix_client;
		domainStr = domain_str;
		this.date = date;
		this.hour = hour;
	}
	
	public void run() {
		String condition = "date=" + date + " and hour=" + hour + " and domain in" + domainStr + " group by domain,region";
		String projectField = "domain, region,count(distinct uv_key),sum(req_pv_count)";
		Vector<String> visitorIpResult = phoenixClient.getExecuteResult("cf_rt_visitor_uv_v2",projectField,condition);
		if(visitorIpResult.isEmpty()){
			logger.error("No vistitor uv domain data. date: " + date + " hour: " + hour);
			return;
		}
		
		//batch insert phoenix
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		//write to phoenix
		for(String uvpResult : visitorIpResult){
			String[] tmpArray = uvpResult.split("#");
			String domain = tmpArray[0].trim();
			String region = tmpArray[1].trim();
			String uvNum = tmpArray[2].trim();
			String pvNum = tmpArray[3].trim();
			sqls.add(getInsertVisitorUvDomainSql(domain,date,hour,region,uvNum,pvNum));
			count++;
			if(count >= BATCHUNIT){//execute sql
				int result =  phoenixClient.executeBatch(sqls);
				if(result > 0){
					logger.error("phoenix connect error. vistitor uv domain. date: " + date + " hour: " + hour);
					return;
				}
				//logger.info("Visitor Ip domain insert phoenix successfully. one sql is: " + sqls.get(0));
				count = 0;
				sqls.clear();
			}
		}
		
		if(count > 0 ){
			//execute last times
			int result = phoenixClient.executeBatch(sqls);
			if(result > 0){
				logger.error("phoenix connect error. vistitor uv domain. date: " + date + " hour: " + hour);
				return;
			}
			sqls.clear();
		}
	}
	
	/**
	 * insert visitor Uv(domain) data into phoenix
	 * @param domain
	 * @param date
	 * @param hour
	 * @param region
	 * @param uvNum
	 * @param pvNum
	 * 
	 * @return sql :  insert uv domain sql
	 */
	public String getInsertVisitorUvDomainSql(String domain,int date,int hour,String region,String uvNum,String pvNum){
		String insertVal = "";
		//access aggregating data: from req_hit_num to req_total_bandwidth
		for(int index = 0;index < 10;index++){
			insertVal += (0 + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num 
		insertVal += ("'ALL'" + "," + 0 + "," + "'" + region + "'" + "," + 0 + "," + pvNum + "," + uvNum + ",");
		//attack_size attack_num domain date hour time
		insertVal += (0 + "," + 0 + "," + domain + "," + date + "," + hour + "," + CURRENTHOUR);
		
		String sql = "upsert into cf_rt_summary_of_hour_domain values(" + insertVal + ")";
		
		return sql;
	}
}
