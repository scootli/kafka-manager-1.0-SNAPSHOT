package baidu.scloud.data.aggregate.client;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.sql.Connection;
import com.jolbox.bonecp.BoneCP;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.log4j.Logger;

public class PoolClient {
	static Logger logger = Logger.getLogger(PoolClient.class);
	private AbstractFileConfiguration conf;
	
	public PoolClient(AbstractFileConfiguration config){
		this.conf = config;
	}
	
	public Vector<String> getExecuteResult(String tableName,String projectField,String condition){
		Vector<String> resultVect = new Vector<String>();
		BoneCP connectionPool = new PhoenixConnectionPool(conf).getConnectionPool();
		if(connectionPool == null){
			return resultVect;
		}
		
		Connection connection = null;
		String sql = "select " + projectField + " from " + tableName + " where " + condition;
		try {
			connection = connectionPool.getConnection();
			Statement stmt = connection.createStatement();
			ResultSet rs  = stmt.executeQuery(sql);
			if(rs == null){
				logger.error("in PoolClient getExecuteResult connection Exception");
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
			logger.error("in PoolClient, getExecuteResult failed! the sql is: " + sql + " Exception: " + ex);
		}finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error("in PoolClient, close  connection failed!  Exception: " + e);
				}
			}
		}
		
		return resultVect;
	}
	
	
	public int executeBatch(Vector<String> sqls){
		//if execute sql failed, rebuild new connection and execute continue and until success
		BoneCP connectionPool = new PhoenixConnectionPool(conf).getConnectionPool();
		if(connectionPool == null){
			return 1;
		}
		Connection connection = null;
		try{
			connection = connectionPool.getConnection();
			connection.setAutoCommit(false);
			Statement stmt = connection.createStatement();
			for(String sql : sqls){
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
			connection.commit();
		}catch (SQLException ex) {
			logger.error("in PoolClient executeBatch, the sql: " + ((sqls.size() > 0 ? sqls.get(0) : "")) + " failed: Exception: " + ex);
		}finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error("in PoolClient, close  connection failed!  Exception: " + e);
				}
			}
		}
		return 0;
	}
}
