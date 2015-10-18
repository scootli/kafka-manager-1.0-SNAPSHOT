package baidu.scloud.data.aggregate.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.LongMsg;
import org.apache.log4j.Logger;

import baidu.scloud.data.aggregate.utils.UPVData;

/**
 *hbase operator Utilities
 * @param zk_server: comma split zookeeper address(must be hostname)
 * @param zk_post: zookeeper port
 * @param keytab: the keytab of executing procedure server
 * @param principal: the principal corresponding to keytab
 */
public class HBaseClient {
	static Logger logger = Logger.getLogger(HBaseClient.class);
	public Configuration conf = null;
	private org.apache.commons.configuration.Configuration hindexConf;
	
	public HBaseClient(org.apache.commons.configuration.Configuration conf){
		hindexConf = conf;
		initHBase();
	}
	
	/**
	 * init secure hbase with principal,keytab and zkServer and zkPort
	 * Note: hbase-site.xml must be in classpath
	 * 
	 */
	public void initHBase(){
		try{
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", hindexConf.getString("zk_server"));
			conf.set("hbase.zookeeper.property.clientPort", hindexConf.getString("zk_port"));
			
			conf.set("hadoop.security.authentication", "kerberos");
			conf.set("hbase.security.authentication", "kerberos");
			
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(hindexConf.getString("hindex_principal"),
					hindexConf.getString("hindex_keytab"));
		}catch(Exception ex){
			logger.error("in HBaseClient,initHBase failed! Exception: " + ex);
		}
	}
	
  /**
   * Get Visitor Ip num from tableName. 
   * domain + date + hour(2 bytes) + region + site + "-" + ip
   * calculate rowcount 
   * if rowcount failed, return ip_num is 0
   * @param tableName: hbase table name 
   * @param domain: query domain.
   * @param date:  query date. 
   * @param hour:   query hour. 
   * @param region:  query region. 
   * @param site:    query site. 
   * @return: ip_num
   */
	public long getVisitorIpNum(String tableName,String domain,int date,int hour,String region,String site){
		AggregationClient aClient = new AggregationClient(conf);
		
		//set scan conditions
		String startRowKey = domain + date + String.format("%02d",hour) + region + site;
		String endRowKey = domain + date + String.format("%02d",hour + 1) + region + site;
		Scan scan = new Scan(Bytes.toBytes(startRowKey),Bytes.toBytes(endRowKey));
		scan.addColumn(Bytes.toBytes("visitor_ip"),Bytes.toBytes("ip"));
		
		final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
		        new LongColumnInterpreter();
		
		TableName visitorIpTable = TableName.valueOf(tableName);
		long ip_num = 0L;
		try {
			ip_num = aClient.rowCount(visitorIpTable,ci,scan);
		} catch (Throwable ex) {
			logger.error("Get ip num from hbase AggregateImplementation coprocessor failed! domain=" + domain + 
				" date=" + date + " hour=" + hour + " region=" + region + " site=" + site + " Exception: " + ex);
		}
		return ip_num;
	}
	
	 /**
	   * Get Visitor uv and pv num from tableName 
	   * domain + date + hour(2 bytes) + region + site + "-" + uv_key
	   * calculate rowcount 
	   * if rowcount failed, return ip_num is 0
	   * @param tableName: hbase table name 
	   * @param domain: query domain.
	   * @param date:  query date. 
	   * @param hour:   query hour. 
	   * @param region:  query region. 
	   * @param site:    query site. 
	   * @return: UPVData which contains uv_num and pv_num
	   */
	public UPVData getVisitorUvNum(String tableName,String domain,int date,int hour,String region,String site){
		AverageCoprocessorClient aClient = new AverageCoprocessorClient();
		
		//set scan conditions
		String startRowKey = domain + date + String.format("%02d",hour) + region + site;
		String endRowKey = domain + date + String.format("%02d",hour + 1) + region + site;
		Scan scan = new Scan(Bytes.toBytes(startRowKey),Bytes.toBytes(endRowKey));
		scan.addColumn(Bytes.toBytes("visitor_uv"),Bytes.toBytes("uv"));
		
		final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
	        new LongColumnInterpreter();
		TableName visitorIpTable = TableName.valueOf(tableName);
		
		//Get uv and pv data from hbase regionserver,if failed, uv and pv num is 0
		UPVData upvData = new UPVData(0,0);
		try {
			HTable table = new HTable(conf,tableName);
			Pair<Long, Long> p = aClient.getAvgArgs(table, ci, scan);
			upvData.setPv(p.getFirst());
			upvData.setUv(p.getSecond());
		} catch (Throwable ex) {
			logger.error("Get uv and pv num from hbase AggregateImplementation coprocessor AverageCoprocessorClient failed! domain=" + domain + 
					" date=" + date + " hour=" + hour + " region=" + region + " site=" + site + " Exception: " + ex);
		}
		return upvData;
	}
}
