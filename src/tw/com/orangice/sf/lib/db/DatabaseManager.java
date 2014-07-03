package tw.com.orangice.sf.lib.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import tw.com.orangice.sf.lib.db.constant.SQLErrorCode;
import tw.com.orangice.sf.lib.utility.LogUtility;

public class DatabaseManager {
	Connection conn = null;
	Logger logger = null;

	public DatabaseManager(Logger logger, Connection conn) {
		this.conn = conn;
		// LogUtility.initial(logger);
		// logger = LogUtility.getLogger();
		this.logger = logger;
	}
	public Logger getLogger() {
		return logger;
	}
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	public DatabaseManager(Logger logger, String host,
			int port,
			String username,
			String password,
			String database) throws ClassNotFoundException, SQLException {
		this.conn =  DatabaseUtility.initial(host, port, username, password, database);
		this.logger = logger;
	}
	public Connection getConnection(){
		return conn;
	}
	public void executeQuery(String query) throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute(query);
		stat.close();
	}
	public void createTable(String tableSchema) throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute(tableSchema);
		stat.close();
	}
	public void createDatabase(String database) throws SQLException{
		
		DatabaseUtility.createDatabase(conn, database);
	}

	public long insertSQL(String table, String[] columns, Object[] values,
			String keyColumn) throws SQLException {

		try {
			logger.info("DB:Insert:"+table+"("+columns.length+","+values.length+")");
			String sql = SQLUtility.convertInsertSQL(table, columns, values);
			logger.info("insertSQL:" + sql);
			Statement stat = conn.createStatement();
			// stat.execute(sql, Statement.RETURN_GENERATED_KEYS);

			long count = stat.executeUpdate(sql,
					Statement.RETURN_GENERATED_KEYS);
			// long keyId = stat.getGeneratedKeys().getLong(keyColumn);

			ResultSet keys = stat.getGeneratedKeys();

			// int count = 0;

			keys.next();
			int keyId = keys.getInt(1);

			logger.info("insertSQL id:" + keyId);
			stat.close();
			// return SQLErrorCode.SQL_INSERT_SUCCESS_CODE;
			return keyId;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info("insertSQL:SQLException");
			e.printStackTrace();
			conn.rollback();
			// return SQLErrorCode.SQL_INSERT_FAIL_CODE;
			return -1;
		}

	}

	public int updateSQL(String table, String[] columns, Object[] values,
			CriteriaCompo condition) throws SQLException {
		try {
			String sql = SQLUtility.convertUpdateSQL(table, columns, values,
					condition);
			Statement stat = conn.createStatement();
			stat.execute(sql, Statement.RETURN_GENERATED_KEYS);
			stat.close();
			return SQLErrorCode.SQL_UPDATE_SUCCESS_CODE;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			conn.rollback();
			return SQLErrorCode.SQL_UPDATE_FAIL_CODE;
		}
	}

	public int deleteSQL(String table, CriteriaCompo condition)
			throws SQLException {
		String sql = SQLUtility.convertDeleteSQL(table, condition);
		logger.info("deleteSQL:"+sql);
		Statement stat = conn.createStatement();
		int result = stat.executeUpdate(sql);
		logger.info("deleteSQL:"+sql);
		return result;

	}

	public ResultSet getObjects(String table, CriteriaCompo condition)
			throws SQLException {
		String sql = SQLUtility.convertSelectSQL(table, condition);

		Statement stat = conn.createStatement();
		logger.info("getObjects:"+sql);
		ResultSet rs = stat.executeQuery(sql);
		return rs;
	}
	
	public int  getCount(String table, CriteriaCompo condition)
			throws SQLException {
		String sql = SQLUtility.convertCountSQL(table, condition);

		Statement stat = conn.createStatement();
		logger.info("getCount:"+sql);
		ResultSet rs = stat.executeQuery(sql);
		int count = 0;
		while(rs.next()) {
			count = rs.getInt("COUNT");
			   //System.out.println("The count is " + rs.getInt("COUNT"));
		}
		return count;
	}
	

	public ResultSet getObjects(TableCompo tableCompo, CriteriaCompo condition)
			throws SQLException {
		String sql = SQLUtility.convertSelectSQL(tableCompo, condition);
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(sql);
		return rs;
	}
	public void close() throws SQLException{
		conn.close();
	}

}
