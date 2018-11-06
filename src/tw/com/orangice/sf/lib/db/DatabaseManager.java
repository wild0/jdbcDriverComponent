package tw.com.orangice.sf.lib.db;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apache.tomcat.jdbc.pool.DataSource;

import tw.com.orangice.sf.lib.db._interface.DatabaseManagerInterface;
import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.QueryObjectsModel;
import tw.com.orangice.sf.lib.db.component.ScriptRunner;
import tw.com.orangice.sf.lib.db.component.TableCompo;
import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.db.constant.SQLErrorCode;
import tw.com.orangice.sf.lib.log.LogService;
import tw.com.orangice.sf.lib.utility.DatabaseUtility;
import tw.com.orangice.sf.lib.utility.SQLUtility;

public class DatabaseManager implements DatabaseManagerInterface {
	// Connection conn = null;
	DataSource ds;
	LogService logger = null;

	public DatabaseManager(LogService logger, DataSource datasource) {
		this.ds = datasource;

		this.logger = logger;
	}

	public DataSource getDataSource() {
		return ds;
	}

	/*
	 * public Logger getLogger() { return logger; } public void setLogger(Logger
	 * logger) { this.logger = logger; }
	 */
	public DatabaseManager(LogService logger, String host, int port,
			String username, String password, String database)
			throws ClassNotFoundException, Exception {
		// this.conn = DatabaseUtility.getConnection(host, port, username,
		// password, database);
		//this.ds = DatabaseUtility.getTomcatDataSource(host, port, username,
		//		password, database);
		this.ds = DatabaseUtility.getTomcatDataSource(host, port, username,
				password);
		
		logger.info("jdbc", "DatabaseManager", "DatabaseManager", "Connect to:"
				+ host + "[" + username + "," + password + "]");

		int result = DatabaseUtility.createDatabase(ds.getConnection(), database);
		
		this.ds = DatabaseUtility.getTomcatDataSource(host, port, username,
				password, database);
		System.out.println("db connect success");
		
		//Statement stmt = ds.getConnection().createStatement();
		//String sql = "CREATE DATABASE " + database;
		// To delete database: sql = "DROP DATABASE DBNAME";
		//try{
		//	stmt.executeUpdate(sql);
		//}
		//catch(Exception e){
		//	e.printStackTrace();
		//}

		this.logger = logger;
	}

	

	/*
	 * public DatabaseManager( String host, int port, String username, String
	 * password, String database) throws ClassNotFoundException, SQLException {
	 * // this.conn = DatabaseUtility.getConnection(host, port, username, //
	 * password, database); Connection connection =
	 * DatabaseUtility.getConnection(host, port, username, password, database);
	 * logger.info("jdbc", "DatabaseManager", "DatabaseManager",
	 * "Connect to:"+host+"["+username+","+password+"]");
	 * 
	 * 
	 * }
	 */

	/*
	 * public Connection getConnection(){ return conn; } public boolean
	 * isClosed() throws SQLException{ return conn.isClosed(); } public boolean
	 * isValid() throws SQLException{ return conn.isValid(0); }
	 */

	public void executeSchema(File dbSchema) throws Exception {
		// URI uri =
		// getClass().getResource("/tw/com/orangice/sf/paperless/res/db.sql").toURI();
		// System.out.println("uri:"+uri);

		// File dbSchema = new File(uri);
		try {
			FileReader reader = new FileReader(dbSchema);

			ScriptRunner sqlScript = new ScriptRunner(newConnection(), false,
					false);
			sqlScript.runScript(reader);
			LogService.debug(DatabaseServiceConstant.TAG,
					DatabaseManager.class.getName(), "executeSchema",
					"init table complete");
			// System.out.println(schema);
			// dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG,
					DatabaseManager.class.getName(), "executeSchema",
					"init table fail", e);
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG,
					DatabaseManager.class.getName(), "executeSchema",
					"init table fail", e);
			e.printStackTrace();
		}

	}

	public Connection newConnection() throws Exception {
		return ds.getConnection();
	}

	public void executeQuery(String query) throws Exception {
		Connection conn = ds.getConnection();
		try {
			Statement stat = conn.createStatement();
			stat.execute(query);
			stat.close();
		} catch (Exception e) {
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void createTable(String tableSchema) throws Exception {
		Connection conn = ds.getConnection();
		try {

			Statement stat = conn.createStatement();
			stat.execute(tableSchema);
			stat.close();
		} catch (Exception e) {
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void createDatabase(String database) throws Exception {
		Connection conn = ds.getConnection();
		DatabaseUtility.createDatabase(conn, database);
	}

	public long insertSQL(String table, String[] columns, Object[] values,
			String keyColumn) throws Exception {
		Connection conn = ds.getConnection();
		try {
			// logger.info("DB:Insert:"+table+"("+columns.length+","+values.length+")");

			String sql = SQLUtility.convertInsertSQL(table, columns, values);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "insertSQL", "INSERT SQL:" + sql);
			Statement stat = conn.createStatement();
			// stat.execute(sql, Statement.RETURN_GENERATED_KEYS);

			long count = stat.executeUpdate(sql,
					Statement.RETURN_GENERATED_KEYS);
			ResultSet keys = stat.getGeneratedKeys();

			keys.next();
			int keyId = keys.getInt(1);

			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "insertSQL", "Insert key:" + keyId);
			stat.close();

			return keyId;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "insertSQL", "Insert fail", e);
			e.printStackTrace();
			// conn.rollback();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			// return SQLErrorCode.SQL_INSERT_FAIL_CODE;
			return -1;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public int updateSQL(String table, String[] columns, Object[] values,
			CriteriaCompo condition) throws SQLException {
		Connection conn = ds.getConnection();
		try {
			String sql = SQLUtility.convertUpdateSQL(table, columns, values,
					condition);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "updateSQL", "UPDATE SQL:" + sql);

			Statement stat = conn.createStatement();
			int result = stat.executeUpdate(sql);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "updateSQL", "UPDATE result:" + result);
			// stat.execute(sql, Statement.RETURN_GENERATED_KEYS);
			stat.close();
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "updateSQL", "UPDATE fail", e);
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			// return SQLErrorCode.SQL_INSERT_FAIL_CODE;
			return -1;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public int deleteSQL(String table, CriteriaCompo condition)
			throws SQLException {
		Connection conn = ds.getConnection();
		try {
			String sql = SQLUtility.convertDeleteSQL(table, condition);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "deleteSQL", "DELETE SQL:" + sql);
			Statement stat = conn.createStatement();
			int result = stat.executeUpdate(sql);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "deleteSQL", "DELETE result:" + result);
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "deleteSQL", "DELETE fail", e);
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			// return SQLErrorCode.SQL_INSERT_FAIL_CODE;
			return -1;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public QueryObjectsModel getObjects(String table, CriteriaCompo condition)
			throws SQLException {
		Connection conn = ds.getConnection();
		String sql = SQLUtility.convertSelectSQL(table, condition);

		Statement stat = conn.createStatement();
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects", "QUERY sql:" + sql);
		ResultSet rs = stat.executeQuery(sql);

		return new QueryObjectsModel(rs, conn);
	}

	public long getCount(String table, CriteriaCompo condition)
			throws SQLException {
		Connection conn = ds.getConnection();
		String sql = SQLUtility.convertCountSQL(table, condition);

		Statement stat = conn.createStatement();
		LogService
				.debug(DatabaseServiceConstant.TAG, this.getClass().getName(),
						"getCount", "QUERY_COUNT sql:" + sql);
		ResultSet rs = stat.executeQuery(sql);
		int count = 0;
		while (rs.next()) {
			count = rs.getInt("COUNT");
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "getCount", "QUERY_COUNT result:" + count);
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return count;
	}

	public QueryObjectsModel getObjects(TableCompo tableCompo,
			CriteriaCompo condition) throws SQLException {
		Connection conn = ds.getConnection();
		String sql = SQLUtility.convertSelectSQL(tableCompo, condition);
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects",
				"QUERY TableCompo sql:" + sql);
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery(sql);

		// LogService.debug(DatabaseServiceConstant.TAG,
		// this.getClass().getName(), "getObjects",
		// "QUERY TableCompo result:" + rs.get);

		return new QueryObjectsModel(rs, conn);
	}

	public long getCount(TableCompo tableCompo, CriteriaCompo condition)
			throws SQLException {
		Connection conn = ds.getConnection();
		// String sql = SQLUtility.convertCountSQL(table, condition);
		String sql = SQLUtility.convertCountSQL(tableCompo, condition);
		Statement stat = conn.createStatement();
		LogService
				.debug(DatabaseServiceConstant.TAG, this.getClass().getName(),
						"getCount", "QUERY_COUNT sql:" + sql);
		ResultSet rs = stat.executeQuery(sql);
		int count = 0;
		while (rs.next()) {
			count = rs.getInt("COUNT");
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "getCount", "QUERY_COUNT result:" + count);
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return count;
	}
	// public void close() throws SQLException{
	// conn.close();
	// }

	@Override
	public boolean isMongoDB() {
		// TODO Auto-generated method stub
		return false;
	}

}
