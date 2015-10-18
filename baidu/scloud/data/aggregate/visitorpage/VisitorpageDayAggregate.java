package baidu.scloud.data.aggregate.visitorpage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import net.sf.json.JSONArray;
import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.base.DayAggregate;
import baidu.scloud.data.aggregate.base64.Base64;
import baidu.scloud.data.aggregate.client.MySQLClient;
import baidu.scloud.data.aggregate.client.PhoenixClient;
import baidu.scloud.data.aggregate.heap.MinHeap;
import baidu.scloud.data.aggregate.heap.VisitorPage;

/**
 * Get visitorpage hour data from hour data table
 * @param PhoenixClient: the Phoenix operator Client object
 */
public class VisitorpageDayAggregate implements DayAggregate{
	static Logger logger = Logger.getLogger(VisitorpageDayAggregate.class);
	private static int BATCHUNIT = 100;
	private static int DOMAIN_QUERY_LIMIT = 1000;
	private PhoenixClient phoenixClient;
	private MySQLClient mysqlClient;
	
	public VisitorpageDayAggregate(org.apache.commons.configuration.PropertiesConfiguration config){
		phoenixClient = new PhoenixClient(config);
		mysqlClient = new MySQLClient(config);
	}
	
	/**
	 * Get visitor page domain hour data from hour data table. default yesterday
	 * @param date
	 * @param topk:  calculate topk visitor uri with heap 
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryDayDomainData(int date,int topk){
		logger.info("enter visitorpage domain aggregate");
		//divide all sites into many parts
		Vector<String> domainVect = phoenixClient.getDomainByDay("cf_rt_visitor_page_v2",date);
		if(domainVect.isEmpty()){
			logger.error("No vistitor page domain data. date: " + date);
			return 1;
		}
		int domainLen = domainVect.size();
		Vector<String> tmpSiteVect = new Vector<String>();
		
		int current = 0;
		String domainStr = "(";
		
		logger.info("visitor page domain start top uri");
		while(current < domainLen){
			int end = (domainLen > (current + DOMAIN_QUERY_LIMIT)?  current + DOMAIN_QUERY_LIMIT: domainLen);
			for(int index = current;index < end;index++){
				String curData = domainVect.get(index);
				if(index != end - 1){
					domainStr += (curData + ",");
				}else{
					domainStr += (curData + ")");
				}
				tmpSiteVect.add(curData);
			}
			Map<Integer,String> siteMap = mysqlClient.GetSiteValue(tmpSiteVect);
			if(siteMap.isEmpty()){
				logger.error("Get domain site from domain_map mysql table failed! visitorpage domain");
				return 1;
			}
			//execute topk uri
			int result = getVisitorPageByDayDomain(date,topk,domainStr,siteMap);
			if(result > 0){
				return result;
			}
			current += DOMAIN_QUERY_LIMIT;
			domainStr = "(";
		}
		
		logger.info("leave visitorpage domain aggregate");
		return 0;
	}
	
	/**
	 * execute visitor page top uri
	 * @param date
	 * @param topk
	 * @param domainListStr
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int getVisitorPageByDayDomain(int date,int topk,String domainStr,Map<Integer,String> siteMap){
		String whereConds = "date=" + date + " and domain in " + domainStr + " group by domain,site,uri";
		String projectField = "domain,site,uri, SUM(req_pv_count)";
		Vector<String> visitorPageDomain = phoenixClient.getExecuteResult("cf_rt_visitor_page_v2",projectField,whereConds);
		if(visitorPageDomain.isEmpty()){
			logger.error("No vistitor page site data. date: " + date);
			return 1;
		}
		String preDomain = "";
		String curDomain = "";
		int current = 0;
		VisitorPage[] data = new VisitorPage[topk];
		MinHeap<VisitorPage> heap = null;
		
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		//traverse sql result
		for(String vpData : visitorPageDomain){
			String[] topVpArr = vpData.split("#");
			int len = topVpArr.length;
			curDomain = topVpArr[0].trim();
			if(len != 4 || curDomain == ""){
				continue;
			}
			
			//when saw next domain, write preDomain to phoenix
			if(!curDomain.equalsIgnoreCase(preDomain)){
				if(!preDomain.equalsIgnoreCase("")){
					sqls.add(getInsertVisitorPageDomainSql(data,preDomain,date,topk,current));
					count++;
					if(count >= BATCHUNIT){//execute sql
						int result = phoenixClient.executeBatch(sqls);
						if(result > 0){
							return result;
						}
						count = 0;
						sqls.clear();
					}
					data = new VisitorPage[topk];
					heap = null;
					current = 0;
				}
				preDomain = curDomain;
			}
			
			//Get topk uri with minHeap
			VisitorPage value = new VisitorPage(siteMap.get(Integer.parseInt(topVpArr[1].trim())) + Base64.getFromBase64(topVpArr[2].trim()),
				Long.parseLong(topVpArr[3].trim()));
			if(current < topk){
				data[current] = value;
				if(current == (topk - 1)){
					//convert min heap
					heap = new MinHeap<VisitorPage>(data);
				}
			}/*else if(current == topk){
				//convert min heap
				heap = new MinHeap<VisitorPage>(data);
			}*/else{
				//when value great than root, replace the root and retidy the heap
				VisitorPage root = heap.getRoot();
				if(root.less_override(value))
				{
				  heap.setRoot(value);
				}
			}
			current++;
		}
		if(count > 0 ){
			//execute last times
			sqls.add(getInsertVisitorPageDomainSql(data,curDomain,date,topk,current));
			int result  = phoenixClient.executeBatch(sqls);
			if(result > 0){
				return result;
			}
		}
		return 0;
	}
	
	
	/**
	 * Get visitor page site hour data from hour data table. default yesterday
	 * @param date
	 * @param topk: calculate topk visitor uri with heap 
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int summaryDaySiteData(int date,int topk){
		logger.info("enter visitorpage site aggregate");
		//divide all sites into many parts
		Vector<String> domainVect = phoenixClient.getDomainByDay("cf_rt_visitor_page_v2",date);
		if(domainVect.isEmpty()){
			return 1;
		}
		int domainLen = domainVect.size();
		Vector<String> tmpSiteVect = new Vector<String>();
		
		int current = 0;
		String domainStr = "(";
		
		logger.info("visitor page site start top uri");
		while(current < domainLen){
			int end = (domainLen > (current + DOMAIN_QUERY_LIMIT)?  current + DOMAIN_QUERY_LIMIT: domainLen);
			for(int index = current;index < end;index++){
				String curData = domainVect.get(index);
				if(index != end - 1){
					domainStr += (curData + ",");
				}else{
					domainStr += (curData + ")");
				}
				tmpSiteVect.add(curData);
			}
			
			Map<Integer,String> siteMap = mysqlClient.GetSiteValue(tmpSiteVect);
			if(siteMap.isEmpty()){
				logger.error("Get domain site from domain_map mysql table failed! visitorpage site");
				return 1;
			}
			//execute topk uri
			int result = getVisitorPageByDaySite(date,topk,domainStr,siteMap);
			if(result > 0){
				return result;
			}
			current += DOMAIN_QUERY_LIMIT;
			domainStr = "(";
		}
		
		logger.info("leave visitorpage site aggregate");
		return 0;
	}
	
	/**
	 * execute visitor page top uri
	 * @param date
	 * @param topk
	 * @param siteListStr
	 * @param siteMap
	 * 
	 * @return execute result status: 0 success >0 failed
	 */
	public int getVisitorPageByDaySite(int date,int topk,String domainStr,Map<Integer,String> siteMap){
		String whereConds = "date=" + date + " and domain in " + domainStr +  " group by domain, site, uri";
		String projectField = "domain, site, uri, SUM(req_pv_count)";
		Vector<String> visitorPageDomain = phoenixClient.getExecuteResult("cf_rt_visitor_page_v2",projectField,whereConds);
		if(visitorPageDomain.isEmpty()){
			return 1;
		}
		String curDomain = "";
		String curSite = "";
		String preDomain = "";
		String preSite = "";
		int current = 0;
		VisitorPage[] data = new VisitorPage[topk];
		MinHeap<VisitorPage> heap = null;
		
		int count = 0;
		Vector<String> sqls  = new Vector<String>();
		
		//traverse sql result
		for(String vpData : visitorPageDomain){
			String[] topVpArr = vpData.split("#");
			int len = topVpArr.length;
			curDomain = topVpArr[0].trim();
			curSite = topVpArr[1].trim();
			if(len != 4 || curDomain == "" || curSite == ""){
				continue;
			}
			
			//when saw next domain site , write preDomain preSite to phoenix
			if(!curDomain.equalsIgnoreCase(preDomain) || !curSite.equalsIgnoreCase(preSite)){
				if(!preDomain.equalsIgnoreCase("") && !preSite.equalsIgnoreCase("")){
					sqls.add(getInsertVisitorPageSiteSql(data,preDomain,preSite,date,topk,current));
					count++;
					if(count >= BATCHUNIT){//execute sql
						int result = phoenixClient.executeBatch(sqls);
						if(result > 0){
							return result;
						}
						count = 0;
						sqls.clear();
					}
					data = new VisitorPage[topk];
					heap = null;
					current = 0;
				}
				preDomain = curDomain;
				preSite = curSite;
			}
			//Get topk uri with minHeap
			VisitorPage value = new VisitorPage(siteMap.get(Integer.parseInt(topVpArr[1].trim())) + Base64.getFromBase64(topVpArr[2].trim()),
				Long.parseLong(topVpArr[3].trim()));
			if(current < topk){
				data[current] = value;
				if(current == (topk - 1)){
					//convert min heap
					heap = new MinHeap<VisitorPage>(data);
				}
			}/*else if(current == topk){
				//convert min heap
				heap = new MinHeap<VisitorPage>(data);
			}*/else{
				//when value great than root, replace the root and retidy the heap
				VisitorPage root = heap.getRoot();
				if(root.less_override(value))
				{
				   heap.setRoot(value);
				}
			}
			current++;
		}
		
		if(count > 0 ){
			//execute last times
			sqls.add(getInsertVisitorPageSiteSql(data,curDomain,curSite,date,topk,current));
			int result = phoenixClient.executeBatch(sqls);
			if(result > 0){
				return result;
			}
		}
		return 0;
	}
	
	/**
	 * get insert visitor page(domain) data into phoenix sql
	 * @param data
	 * @param domain
	 * @param date
	 * @param topk
	 * @param dataLen
	 * @return : the insert sql
	 */
	public String getInsertVisitorPageDomainSql(VisitorPage[] data,String domain,int date,int topk,int dataLen){
		String content = getVistorPageJsonContent(data,topk,dataLen);
		String sql = "upsert into cf_rt_summary_visitor_page_of_day_domain values(" + 
					domain + "," + date + ",'" + content + "')";
		
		return sql;
	}
	
	
	/**
	 * get insert visitor page(site) data into phoenix sql
	 * @param data
	 * @param domain
	 * @param site
	 * @param date
	 * @param topk
	 * @param dataLen : data actual size
	 * 
	 * @return the insert sql
	 */
	public String getInsertVisitorPageSiteSql(VisitorPage[] data,String domain,String site, int date,int topk,int dataLen){
		String content = getVistorPageJsonContent(data,topk,dataLen);
		String sql = "upsert into cf_rt_summary_visitor_page_of_day_site values(" + 
					domain + "," + date + "," + site + ",'" + content + "')";
		
		return sql;
	}
	
	/**
	 * Get visitor page json and base64 result from topk minHeap
	 * @param data
	 * @param topk
	 * @param dataLen: data actual size
	 * @return: topk visitor uri json and base64 result
	 */
	public String getVistorPageJsonContent(VisitorPage[] data,int topk,int dataLen){
		int size = (dataLen > topk)? topk : dataLen;
		List<Map> list = new ArrayList<Map>();
		for(int index = 0;index < size;index++){
			Map<String,String> topUri2Count = new HashMap<String,String>();
			topUri2Count.put("url","http://" + data[index].get_uri());
			topUri2Count.put("sum",data[index].get_req_pv_count() + "");
			list.add(topUri2Count);
		}
		
		//JSONArray jsonArray = JSONArray.fromObject(list);
		String content = JSONArray.fromObject(list).toString();
		return Base64.getBase64(content);
	}
}
