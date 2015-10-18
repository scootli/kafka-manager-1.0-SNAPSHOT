package baidu.scloud.data.aggregate.se;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.HourAggregate;
import baidu.scloud.data.aggregate.client.PhoenixClient;
//import baidu.scloud.data.aggregate.utils.CommonUtils;

/**
 * Get searchengine hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 */
public class SeHourAggregate implements HourAggregate{
	static Logger logger = Logger.getLogger(SeHourAggregate.class);
	private static int BATCHUNIT = 100;
	private static long CURRENTHOUR = System.currentTimeMillis();//CommonUtils.TimeStamp2Date(System.currentTimeMillis());
	private PhoenixClient phoenixClient;
	
	public SeHourAggregate(PhoenixClient phoenix_client){
		this.phoenixClient = phoenix_client;
	}
	
	/**
	 * Get se domain hour data from domain hour data table. default yesterday
	 * @param date: 
	 * @param hour:
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourDomainData(int date,int hour){
		logger.info("enter se domain aggregate");
		//Get searchengine domain data
		String condition = "date=" + date + " and hour=" + hour +  " group by domain,type";
		String projectField = "domain, type, SUM(req_count)";
		Set<String> seHourDomainRes = getSeTypeHour("cf_rt_searchengine_v2",projectField,condition);
		if(seHourDomainRes == null || seHourDomainRes.isEmpty()){
			logger.error("No se domain data. date: " + date + " hour: " + hour);
			return 1;
		}
		
		Iterator<String> it = seHourDomainRes.iterator();
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		while(it.hasNext()){
			String seData = it.next();
			sqls.add(getInsertSeDomainSql(seData,date,hour));
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
		logger.info("leave se domain aggregate");
		return 0;
	}
	
	/**
	 * Get se site hour data from site hour data table. default yesterday
	 * @param date: 
	 * @param hour:
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryHourSiteData(int date,int hour){
		logger.info("enter se site aggregate");
		//Get searchengine site data
		String condition = "date=" + date + " and hour=" + hour +  " group by domain,type,site";
		String projectField = "domain,site,type, SUM(req_count)";
		Set<String> seHourSiteRes = getSeTypeHour("cf_rt_searchengine_v2",projectField,condition);
		if(seHourSiteRes == null || seHourSiteRes.isEmpty()){
			logger.error("No se site data. date: " + date + " hour: " + hour);
			return 1;
		}
		
		Iterator<String> it = seHourSiteRes.iterator();
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		while(it.hasNext()){
			String seData = it.next();
			sqls.add(getInsertSeSiteSql(seData,date,hour));
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
		logger.info("leave se site aggregate");
		
		return 0;
	}
	
	/**
	 * get insert se hour domain data sql
	 * @param seData: one searchengine data in the phoenix
	 * @param date: 
	 * @hour hour
	 * 
	 * @return the sql
	 */
	public String getInsertSeDomainSql(String seData,int date,int hour){
		String[] seArr = seData.split("#");
		if(seArr.length != 3){
			logger.info("in SummarySeHour getInsertSeDomainSql, query param number error: " + seArr.length);
		}
		String domain = seArr[0].trim();
		String se_type = seArr[1].trim();
		String req_se_num = seArr[2].trim();
		
		String insertVal = "";
		//access aggregating data: from req_hit_num to req_total_bandwidth
		for(int index = 0;index < 10;index++){
			insertVal += (0 + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num 
		insertVal += ("'" + se_type + "'" + "," + req_se_num + "," + "'ALL'" + "," + 0 + "," + 0 + "," + 0 + ",");
		//attack_size attack_num domain date hour time
		insertVal += (0 + "," + 0 + "," + domain + "," + date + "," + hour + "," + CURRENTHOUR);
		String sql = "upsert into cf_rt_summary_of_hour_domain values(" + insertVal + ")";
		
		return sql;
	}
	
	/**
	 * get insert se hour site data sql
	 * @param seData: one searchengine data in the phoenix
	 * @param date: 
	 * @hour hour
	 * 
	 * @return the sql
	 */
	public String getInsertSeSiteSql(String seData,int date,int hour){
		String[] seArr = seData.split("#");
		if(seArr.length != 4){
			logger.info("in SummarySeHour getInsertSeSiteSql, query param number error: " + seArr.length);
		}
		String domain = seArr[0].trim();
		String site = seArr[1].trim();
		String se_type = seArr[2].trim();
		String req_se_num = seArr[3].trim();
		
		String insertVal = "";
		//access aggregating data: from req_hit_num to req_total_bandwidth
		for(int index = 0;index < 10;index++){
			insertVal += (0 + ",");
		}
		//se_type req_se_num region ip_num pv_num uv_num 
		insertVal += ("'" + se_type + "'" + "," + req_se_num + "," + "'ALL'" + "," + 0 + "," + 0 + "," + 0 + ",");
		//attack_size attack_num domain site date hour time
		insertVal += (0 + "," + 0 + "," + domain + "," + site + "," + date + "," + hour + "," + CURRENTHOUR);
		String sql = "upsert into cf_rt_summary_of_hour_site values(" + insertVal + ")";
		
		return sql;
	}
	
	/**
	 * Get searchengine data by type hour domain from phoenix
	 * @param tableName:
	 * @param projectField: select fields
	 * @param condition: where conditions
	 * 
	 * @return sql query result
	 */
	public Set<String> getSeTypeHour(String tableName,String projectField,String condition){
		Set<String> seDomainResult = new HashSet<String>();
		String sql = "select " + projectField + " from " + tableName + " where " + condition;
		try {
			ResultSet rs  = phoenixClient.executeSQL(sql);
			if(rs == null){
				logger.error("in SeHourAggregate getSeTypeHour connection Exception");
				return null;
			}
			ResultSetMetaData rsm = rs.getMetaData();
			int columnNum = rsm.getColumnCount();
			while(rs.next()){
				String curResult = "";
				for(int index = 1; index <= columnNum; index++){
					if(index != columnNum){
						curResult += (rs.getString(index) + "#");
					}else{
						curResult += rs.getString(index);
					}
				}
				seDomainResult.add(curResult);
			}
		} catch (SQLException ex) {
			logger.error("in SummarySeHour, getSeTypeHour failed! Exception: " + ex);
		}
		
		return seDomainResult;
 	}
}
