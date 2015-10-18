package baidu.scloud.data.aggregate.attack;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.aggregate.utils.CommonUtils;

/**
 * Get attack domain hour data from middle attack domain hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 */
public class AttackHourAggregate implements HourAggregate{
	static Logger logger = Logger.getLogger(AttackHourAggregate.class);
	private static int BATCHUNIT = 100;
	private static long CURRENTHOUR = System.currentTimeMillis();//CommonUtils.TimeStamp2Date(System.currentTimeMillis());
	private PhoenixClient phoenixClient;
	
	public AttackHourAggregate(PhoenixClient phoenix_client){
		phoenixClient = phoenix_client;
	}
	
	/**
	 * Get attack domain hour data from middle attack domain hour data table. default yesterday
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourDomainData(int date,int hour){
		logger.info("enter attack domain aggregate");
		String condition = "date=" + date + " and hour=" + hour + " group by domain";
		String projectField = "domain,SUM(attack_count)";
		Map<Integer,String> domainWafDataMap = getDomainDataByHour("cf_summary_of_hour_domain_waf_v2",projectField,condition);
		projectField = "domain,SUM(attack_size),SUM(attack_count)";
		Map<Integer,String> domainCcDataMap = getDomainDataByHour("cf_summary_of_hour_domain_cc_v2",projectField,condition);
		if(domainWafDataMap == null || domainCcDataMap == null){
			logger.error("No attack domain data. date: " + date + " hour: " + hour);
			return 1;
		}
			
		//set union	
		Set<Integer> domainSet = new HashSet<Integer>();
		domainSet.addAll(domainWafDataMap.keySet());
		domainSet.addAll(domainCcDataMap.keySet());
		
		//aggregate attack num from waf and cc to summary table
		Iterator<Integer> it = domainSet.iterator();
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		boolean containWaf = false;
		boolean containCc = false;
		
		while(it.hasNext()){
			int domain = it.next();
			long attackSize = 0L;
			long attackNum = 0L;
			
			long ccAttackSize = 0L;
			long ccAttackNum = 0L;
			long wafAttackCount = 0L;
			
			containWaf = domainWafDataMap.containsKey(domain);
			containCc = domainCcDataMap.containsKey(domain);
			if(containWaf){
				wafAttackCount = Long.parseLong(domainWafDataMap.get(domain));
			}
			if(containCc){
				String[] ccDataArr = domainCcDataMap.get(domain).split("#");
				ccAttackSize = Long.parseLong(ccDataArr[0].trim());
				ccAttackNum = Long.parseLong(ccDataArr[1].trim());
			}
			
			attackSize = ccAttackSize;
			attackNum = ccAttackNum + wafAttackCount;
			//add sql for executing batch
			sqls.add(getInsertAttackDataSql(domain,date,hour,attackSize,attackNum));
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
		logger.info("leave attack domain aggregate");
		return 0;
	}
	
	
	/**
	 * Get attack site hour data from middle attack site hour data table. default yesterday
	 * @param date: aggregating data
	 * @param hour: aggregating hour
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourSiteData(int date,int hour){
		logger.info("enter attack site aggregate");
		String condition = "date=" + date + " and hour=" + hour + " group by domain,site";
		String projectField = "domain,site,SUM(attack_count)";
		Map<String,String> siteWafDataMap = getSiteDataByHour("cf_summary_of_hour_site_waf_v2",projectField,condition);
		projectField = "domain,site,SUM(attack_size),SUM(attack_count)";
		Map<String,String> siteCcDataMap = getSiteDataByHour("cf_summary_of_hour_site_cc_v2",projectField,condition);
		if(siteWafDataMap == null || siteCcDataMap == null){
			logger.error("No attack site data. date: " + date + " hour: " + hour);
			return 1;
		}
		
		//set union
		Set<String> domainSet = new HashSet<String>();
		domainSet.addAll(siteWafDataMap.keySet());
		domainSet.addAll(siteCcDataMap.keySet());
		
		//aggregate attack num from waf and cc to summary table
		Iterator<String> it = domainSet.iterator();
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		boolean containWaf = false;
		boolean containCc = false;
		
		while(it.hasNext()){
			String domainAndSite = it.next();
			long attackSize = 0L;
			long attackNum = 0L;
			
			long ccAttackSize = 0L;
			long ccAttackNum = 0L;
			long wafAttackCount = 0L;
			
			containWaf = siteWafDataMap.containsKey(domainAndSite);
			containCc = siteCcDataMap.containsKey(domainAndSite);
			
			if(containWaf){
				wafAttackCount = Long.parseLong(siteWafDataMap.get(domainAndSite));
			}
			if(containCc){
				String[] ccDataArr = siteCcDataMap.get(domainAndSite).split("#");
				ccAttackSize = Long.parseLong(ccDataArr[0].trim());
				ccAttackNum = Long.parseLong(ccDataArr[1].trim());
			}
			attackSize = ccAttackSize;
			attackNum = ccAttackNum + wafAttackCount;
			
			//add sql for executing batch
			sqls.add(getInsertAttackSql(domainAndSite,date,hour,attackSize,attackNum));
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
		logger.info("leave attack site aggregate");
		return 0;
	}
	
	/**
	 * get insert domain attack data sql
	 * @param domain: 
	 * @param date:
	 * @param hour:
	 * @param attackSize: attack size in date hour domain
	 * @param attackNum: attack num in date hour domain
	 */
	public String getInsertAttackDataSql(int domain,int date,int hour,long attackSize,long attackNum){
		String insertVal = "";
		//access aggregating data: from req_hit_num to req_total_bandwidth
		for(int index = 0;index < 10;index++){
			insertVal += (0 + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num 
		insertVal += ("'ALL'" + "," + 0 + "," + "'ALL'" + "," + 0 + "," + 0 + "," + 0 + ",");
		//attack_size attack_num domain date hour time
		insertVal += (attackSize + "," + attackNum + "," + domain + "," + date + "," + hour + "," + CURRENTHOUR);
		String sql = "upsert into cf_rt_summary_of_hour_domain values(" + insertVal + ")";
		
		return sql;
	}
	
	/**
	 * get insert site attack data sql
	 * @param domain: 
	 * @param date:
	 * @param hour:
	 * @param attackSize: attack size in date hour domain site
	 * @param attackNum: attack num in date hour domain site
	 */
	public String getInsertAttackSql(String domainAndSite,int date,int hour,long attackSize,long attackNum){
		String[] domain_site = domainAndSite.split("#");
		if(domain_site.length !=  2){
			logger.error("Get site domain and site failed, domain and site length must be 2");
			System.exit(0);
		}
		String domain = domain_site[0].trim();
		String site  = domain_site[1].trim();
		
		String insertVal = "";
		//access aggregating data: from req_hit_num to req_total_bandwidth
		for(int index = 0;index < 10;index++){
			insertVal += (0 + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num 
		insertVal += ("'ALL'" + "," + 0 + "," + "'ALL'" + "," + 0 + "," + 0 + "," + 0 + ",");
		//attack_size attack_num domain site date hour time
		insertVal += (attackSize + "," + attackNum + "," + domain + "," + site + "," + date + "," + hour + "," + CURRENTHOUR);
		
		String sql = "upsert into cf_rt_summary_of_hour_site values(" + insertVal + ")";
		
		return sql;
	}
	
	
	/**
	 * get waf and cc hour aggregating data
	 * @param tableName: 
	 * @param projectField: select fields
	 * @param whereCond: where conditions
	 * 
	 * @return Map<Integer,String> : domain to value map
	 */
	public Map<Integer,String> getDomainDataByHour(String tableName, String projectField,String whereCond){
		Map<Integer,String> domainDataMap = new HashMap<Integer,String>();
		String sql = "select " + projectField + " from " + tableName + " where " + whereCond;
		try {
			ResultSet rs  = phoenixClient.executeSQL(sql);
			if(rs == null){
				logger.error("in AttackHourAggregate getDomainDataByHour connection Exception");
				return null;
			}
			ResultSetMetaData rsm = rs.getMetaData();
			int columnNum = rsm.getColumnCount();
			while(rs.next()){
				String value = "";
				int key = 0;
				for(int index = 1; index <= columnNum; index++){
					//domain
					if(index == 1){
						key = Integer.parseInt(rs.getString(index));
					}else{//other value
						if(index != columnNum){
							value += (rs.getString(index) + "#");
						}else{
							value += rs.getString(index);
						}
					}
				}
				domainDataMap.put(key,value);
			}
		} catch (SQLException ex) {
			logger.error("in SummaryOfHourDomain, getDomainDataByHour failed! Exception: " + ex);
		}
		return domainDataMap;
	}
	

	/**
	 * get waf and cc hour aggregating data
	 * @param tableName: 
	 * @param projectField: select fields
	 * @param whereCond: where conditions
	 * 
	 * @return Map<String,String> : domain-site to value map
	 */
	public Map<String,String> getSiteDataByHour(String tableName, String projectField,String whereCond){
		Map<String,String> siteDataMap = new HashMap<String,String>();
		String sql = "select " + projectField + " from " + tableName + " where " + whereCond;
		try {
			ResultSet rs  = phoenixClient.executeSQL(sql);
			if(rs == null){
				logger.error("in AttackHourAggregate getSiteDataByHour connection Exception");
				return null;
			}
			ResultSetMetaData rsm = rs.getMetaData();
			int columnNum = rsm.getColumnCount();
			while(rs.next()){
				String value = "";
				String key = "";
				for(int index = 1; index <= columnNum; index++){
					//domain
					if(index == 1){
						key += (rs.getString(index) + "#");
					}else if(index == 2){
						key += rs.getString(index);
					}else{//other value
						if(index != columnNum){
							value += (rs.getString(index) + "#");
						}else{
							value += rs.getString(index);
						}
					}
				}
				siteDataMap.put(key,value);
			}
		} catch (SQLException ex) {
			logger.error("in SummaryOfHourSite, getSiteDataByHour failed! Exception: " + ex);
		}
		return siteDataMap;
	}
}
