package baidu.scloud.data.aggregate.client;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.log4j.Logger;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class PhoenixConnectionPool {
	static Logger logger = Logger.getLogger(PhoenixConnectionPool.class);
	private AbstractFileConfiguration conf;
	
	public PhoenixConnectionPool(AbstractFileConfiguration config){
		this.conf = config;
	}
	
	public BoneCP getConnectionPool(){
		BoneCP connectionPool = null;
		String url = "jdbc:phoenix:" + conf.getString("zk_server") + ":" + conf.getInt("zk_port");
		PhoenixDriver phxDriver = new PhoenixDriver();
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (Exception e) {
			logger.error("In PhoenixConnectionPool, getConnectionPool failed. Exception: " +  e);
			return null;
		}
		
		try {
            BoneCPConfig config = new BoneCPConfig();
            Properties prp = new Properties();
            config.setJdbcUrl(url);
			
			String keytab = "conf/" + conf.getString("hindex_keytab");
			String principal = conf.getString("hindex_principal");
			
            prp.setProperty("hadoop.security.authentication", "kerberos");
            prp.setProperty("hbase.security.authentication", "kerberos");
            prp.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"10000000");
            prp.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"10000000");
            prp.setProperty(QueryServices.HBASE_CLIENT_KEYTAB,keytab);
            prp.setProperty(QueryServices.HBASE_CLIENT_PRINCIPAL, principal);
			
            config.setDriverProperties(prp);
            config.setMinConnectionsPerPartition(5);
            config.setMaxConnectionsPerPartition(10);
            config.setPartitionCount(2);
            //config.setIdleConnectionTestPeriod(80);
            connectionPool = new BoneCP(config);
			
        } catch (SQLException ex) {
			logger.error("In PhoenixConnectionPool, construction ConnectionPool failed. Exception: " +  ex);
            return null;
        }
		
		return connectionPool;
	}
}
