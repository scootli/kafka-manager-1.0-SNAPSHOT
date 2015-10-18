package baidu.scloud.data.aggregate.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/**
 * Mysql operator client
 *
 */
public class MySQLClient {
	static Logger logger = Logger.getLogger(MySQLClient.class);
	private Configuration conf;
	private Connection conn;
	private int CONNECTMAXRETRYTIMES = 5;
	private Statement statement;
	
	public MySQLClient(Configuration config){
		conf = config;
		CONNECTMAXRETRYTIMES = conf.getInt("connect_retry_times");
		initStatement();
	}
	
	/**
	 * init connection
	 */
	public void initStatement(){
		if(conn != null){
			try {
				statement.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		String driver = "com.mysql.jdbc.Driver";
		try{
           Class.forName(driver);
           conn = DriverManager.getConnection(conf.getString("connection_url"), 
				   							  conf.getString("user_name"),
				   							  conf.getString("password"));
		 if(!conn.isClosed()){
			 logger.info("in initStatement,connect mysql successfully");
		 }
         statement = conn.createStatement();
		}catch(Exception Ex){
			logger.error("in MySQLClient initStatement,load jdbc driver failed. Exception is: " + Ex);
		}
	}
	
	/**
	 * @param domain
	 * @param isDomain
	 * @return
	 */
	public Map<Integer,String> GetSiteValue(Vector<String> SiteVect){
		int len = SiteVect.size();
		String site_str = "(";
		for(int index = 0;index < len;index++){
			if(index != len -1){
				site_str += (SiteVect.get(index) + ",");
			}else{
				site_str += (SiteVect.get(index) + ")");
			}
		}
		
		Map<Integer,String> siteMap = new HashMap<Integer,String>();
		String sql = "select domain_id,domain from domain_map where domain_id in " + site_str;
		int count = 0; 
		while(true){
			try {
				ResultSet rs = statement.executeQuery(sql);
				while(rs.next()){
					siteMap.put(rs.getInt("domain_id"),rs.getString("domain"));
				}
				break;
			} catch (SQLException ex) {
				logger.error("in MySQLClient GetSiteValue, SQL failed!. Exception: " + ex);
				try {
	                Thread.sleep(count * 2000);
	            } catch (InterruptedException ie) {
	            }
				count++;
				initStatement();
				if(count >= CONNECTMAXRETRYTIMES){
					logger.error("Connect mysql error. retry " + CONNECTMAXRETRYTIMES + " times");
					return null;
				}
			}
		}
		return siteMap;
	}
}
