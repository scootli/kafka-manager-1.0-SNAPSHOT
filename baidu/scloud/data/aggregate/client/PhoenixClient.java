package baidu.scloud.data.aggregate.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.log4j.Logger;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;

/**
 *phoenix operator Utilities
 * @param zk_server: comma split zookeeper address(must be hostname)
 * @param zk_post: zookeeper port
 * @param keytab: the keytab of executing procedure server
 * @param principal: the principal corresponding to keytab
 */
public class PhoenixClient {
	static Logger logger = Logger.getLogger(PhoenixClient.class);
	private AbstractFileConfiguration conf;
	private Statement statement;
	private int CONNECTMAXRETRYTIMES = 5;
	private Connection cc;
	
	public PhoenixClient(AbstractFileConfiguration config){
		conf = config;
		CONNECTMAXRETRYTIMES = conf.getInt("connect_retry_times");
		this.statement = GetStatement();
	}
	
	/**
	 * Get phoenix connection
	 * @return statement
	 */
	public Statement GetStatement(){	
		Properties props = new Properties();
		props.setProperty("hadoop.security.authentication", "kerberos");
		props.setProperty("hbase.security.authentication","kerberos");
		
		//the next two statement to not put hbase-site.xml into classpath 
		props.setProperty("hbase.regionserver.kerberos.principal","hbase/_HOST@DATA.SCLOUD");
		props.setProperty("hbase.master.kerberos.principal","hbase/_HOST@DATA.SCLOUD");

		props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"10000000");
		props.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"10000000");
		
		props.setProperty(QueryServices.RPC_TIMEOUT_ATTRIB,"1200000");
		props.setProperty(QueryServices.REGIONSERVER_LEASE_PERIOD_ATTRIB,"1200000");
		props.setProperty(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,"1200000");
		props.setProperty(QueryServices.MAX_MEMORY_PERC_ATTRIB,"30");
		props.setProperty(QueryServices.MAX_MEMORY_WAIT_MS_ATTRIB,"100000");
		props.setProperty(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB,"40960000");
		props.setProperty(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB,"600000");
		props.setProperty(QueryServices.MAX_SERVER_METADATA_CACHE_SIZE_ATTRIB,"81920000");
		
		String keytab = "conf/" + conf.getString("hindex_keytab");
		String principal = conf.getString("hindex_principal");
		props.setProperty(QueryServices.HBASE_CLIENT_KEYTAB,keytab); //rootPath + "/conf/work_00.keytab"
		props.setProperty(QueryServices.HBASE_CLIENT_PRINCIPAL,principal);
		
		String url = "jdbc:phoenix:" + conf.getString("zk_server") + ":" + conf.getInt("zk_port");
		try {
			PhoenixDriver phxDriver = new PhoenixDriver();
			logger.info("load Driver successfully. url is: " + url + " keytab: " + keytab + " principal: " + principal);
			cc = phxDriver.connect(url,props);
			cc.setAutoCommit(false);
			if(!cc.isClosed()){
				logger.info("connect phoenix successfully. url is: " + url);
			}
			statement = cc.createStatement();
	    } catch (Exception ex) {
			try {
				if(statement != null){
					statement.close();
					statement = null;
				}
				if(cc != null){
					cc.close();
					cc = null;
				}
			} catch (SQLException e) {
			}
			//ex.printStackTrace();
			logger.error("GetConnection from Phoenix failed. Exception: " + ex);
	    }
	    return statement;
	}
	
	/**
	 * execute select operation. if statement invalid, it will reconnect
	 * @param sql
	 * @return
	 */
	public ResultSet executeSQL(String sql){
		//Statement statement = GetStatement();
		ResultSet rs = null;
		//if execute sql failed, rebuild new connection and execute continue and until success
		int count = 0;
		while(true){
			try {
				//logger.info("in PhoenixClient executeSQL sql,this sql is: " + sql); 
				rs = statement.executeQuery(sql);
				break;
			} catch (Exception ex) {
				logger.error("in PhoenixClient executeSQL, the sql: " + sql + " failed: Exception: " + ex);
				 try {
	                    Thread.sleep(count * 2000);
	                } catch (InterruptedException ie) {
	                }
				count++;
				statement = GetStatement();
				if(count >= CONNECTMAXRETRYTIMES){
					logger.error("Connect phoenix error. retry " + CONNECTMAXRETRYTIMES + " times");
					return null;
				}
			}
		}
		return rs;
	}
	
	/**
	 * execute upsert,delete,update operation. if statement invalid, it will reconnect
	 * @param sql
	 */
	public int execute(String sql){
		//Statement statement = GetStatement();
		//if execute sql failed, rebuild new connection and execute continue and until success
		int count = 0;
		while(true){
			try {
				//logger.info("in PhoenixClient executeSQL sql,this sql is: " + sql); 
				int lines = statement.executeUpdate(sql);
				if(lines > 0)
					cc.commit();
				break;
			} catch (Exception ex) {
				logger.error("in PhoenixClient executeSQL, the sql: " +  sql  + " failed: Exception: " + ex);
				 try {
	                    Thread.sleep(count * 2000);
	                } catch (InterruptedException ie) {
	                }
					count++;
					statement = GetStatement();
					if(count >= CONNECTMAXRETRYTIMES){
						logger.error("Connect phoenix error. retry " + CONNECTMAXRETRYTIMES + " times");
						return 1;
					}
			}
		}
		return 0;
	}
	
	/**
	 * batch execute sql operation. if statement invalid, it will reconnect
	 * @param sqls
	 */
	public synchronized int executeBatch(Vector<String> sqls){
		//if execute sql failed, rebuild new connection and execute continue and until success
		int count = 0;
		while(true){
			try {
				for(String sql : sqls){
					statement.addBatch(sql);
				}
				statement.executeBatch();
				cc.commit();
				break;
			} catch (SQLException ex) {
				logger.error("in PhoenixClient executeBatch, the sql: " + ((sqls.size() > 0 ? sqls.get(0) : "")) + " failed: Exception: " + ex);
				try {
	                Thread.sleep(count * 2000);
	            } catch (InterruptedException ie) {
	            }
				count++;
				statement = GetStatement();
				if(count >= CONNECTMAXRETRYTIMES){
					logger.error("Connect phoenix error. retry " + CONNECTMAXRETRYTIMES + " times");
					return 1;
				}
			}
		}
		return 0;
	}
	
	/**
	 * Get result according to condition
	 * @param tableName
	 * @param projectField
	 * @param condition
	 * @return : the sql result
	 */
	public Vector<String> getExecuteResult(String tableName,String projectField,String condition){
		Vector<String> resultVect = new Vector<String>();
		String sql = "select " + projectField + " from " + tableName + " where " + condition;
		try {
			ResultSet rs  = executeSQL(sql);
			if(rs == null){
				logger.error("in PhoenixClient getExecuteResult connection Exception");
				return resultVect;
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
				resultVect.add(curResult);
			}
		} catch (SQLException ex) {
			logger.error("in PhoenixClient, getExecuteResult failed! the sql is: " + sql + " Exception: " + ex);
		}
		
		return resultVect;
	}
	
	/**
	 * Get all domain by the date and the hour
	 * @param data
	 * @param hour
	 * 
	 * @return : all domain
	 */
	public Vector<String> getDomainByHour(String tableName,int date,int hour){		
		String condition = "date=" + date + " and hour=" + hour + " group by domain";
		String projectField = "domain";
		Vector<String> domainVect = getExecuteResult(tableName,projectField,condition);
		return domainVect;
	}
	
	/**
	 * Get all domain by the date
	 * @param tableName
	 * @param date
	 * @return
	 */
	public Vector<String> getDomainByDay(String tableName,int date){
		String condition = "date=" + date  + " group by domain";
		String projectField = "domain";
		Vector<String> domainVect = getExecuteResult(tableName,projectField,condition);
		return domainVect;
	}
	
	/**
	 * Get all site by the date
	 * @param tableName
	 * @param date
	 * @return
	 */
	public Vector<String> getSiteByDay(String tableName,int date){
		String condition = "date=" + date  + " group by site";
		String projectField = "site";
		Vector<String> siteVect = getExecuteResult(tableName,projectField,condition);
		return siteVect;
	}

	public AbstractFileConfiguration getConf() {
		return conf;
	}
	
}
