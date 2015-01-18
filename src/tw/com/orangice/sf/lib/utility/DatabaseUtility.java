package tw.com.orangice.sf.lib.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.log.LogService;

public class DatabaseUtility {
	// static ArrayList<Connection> connections = new ArrayList<Connection>();
	// private static String dropdbSQL = "DROP TABLE ";

	/*
	 * public static Connection initial() throws SQLException,
	 * ClassNotFoundException { Class.forName("com.mysql.jdbc.Driver");
	 * Connection connection = (Connection) DriverManager
	 * .getConnection("jdbc:mysql://localhost/?user=root&password=rootpassword"
	 * ); return connection; }
	 */
	/*
	 * public static void checkDatabase(Connection conn, String database) throws
	 * SQLException { //Connection connection = null; Statement statement =
	 * null;
	 * 
	 * //try { //Class.forName("com.mysql.jdbc.Driver"); //connection =
	 * DriverManager.getConnection("jdbc:mysql://localhost/", // "root",
	 * "admin"); statement = conn.createStatement(); String sql =
	 * "CREATE DATABASE "+database; // To delete database: sql =
	 * "DROP DATABASE DBNAME"; statement.executeUpdate(sql);
	 * System.out.println("Database created!");
	 * 
	 * //catch (ClassNotFoundException e) { // No driver class found! //} }
	 */
	public static void createDatabase(Connection conn, String database)
			throws SQLException {
		Statement stat = null;

		stat = conn.createStatement();
		stat.executeUpdate("CREATE DATABASE IF NOT EXISTS " + database);
		stat.close();
		LogService.debug(DatabaseServiceConstant.TAG,
				DatabaseUtility.class.getName(), "createDatabase",
				"create database complete:"+"CREATE DATABASE IF NOT EXISTS " + database);

	}
	public static Connection getTomcatConnection(String host, int port,
			String username, String password) throws SQLException{
		return getTomcatConnection(host, port, username, password, "test");
	}
	public static Connection getTomcatConnection(String host, int port,
			String username, String password, String database) throws SQLException{
		PoolProperties p = new PoolProperties();
        p.setUrl("jdbc:mysql://"+host+":"+String.valueOf(port)+"/"+database+"?useUnicode=true&useEncoding=true&characterEncoding=UTF-8");
        p.setDriverClassName("com.mysql.jdbc.Driver");
        p.setUsername(username);
        p.setPassword(password);
        
        p.setJmxEnabled(true);
        p.setTestWhileIdle(false);
        p.setTestOnBorrow(true);
        p.setValidationQuery("SELECT 1");
        p.setTestOnReturn(false);
        p.setValidationInterval(30000);
        p.setTimeBetweenEvictionRunsMillis(30000);
        p.setMaxActive(100);
        p.setInitialSize(10);
        p.setMaxWait(10000);
        p.setRemoveAbandonedTimeout(600);
        p.setMinEvictableIdleTimeMillis(30000);
        p.setMinIdle(10);
        p.setLogAbandoned(true);
        p.setRemoveAbandoned(true);
        p.setJdbcInterceptors(
                "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
                        + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer;"
                        + "org.apache.tomcat.jdbc.pool.interceptor.ResetAbandonedTimer");
        DataSource datasource = new DataSource();
        datasource.setPoolProperties(p);
        Connection conn = datasource.getConnection();
        return conn;
	}
	public static DataSource getTomcatDataSource(String host, int port,
			String username, String password) throws SQLException{
		return getTomcatDataSource(host, port, username, password, "mysql");
	}
	public static DataSource getTomcatDataSource(String host, int port,
			String username, String password, String database) throws SQLException{
		PoolProperties p = new PoolProperties();
        p.setUrl("jdbc:mysql://"+host+":"+String.valueOf(port)+"/"+database+"?useUnicode=true&useEncoding=true&characterEncoding=UTF-8");
        p.setDriverClassName("com.mysql.jdbc.Driver");
        p.setUsername(username);
        p.setPassword(password);
        
        p.setJmxEnabled(true);
        p.setTestWhileIdle(false);
        p.setTestOnBorrow(true);
        p.setValidationQuery("SELECT 1");
        p.setTestOnReturn(false);
        p.setValidationInterval(30000);
        p.setTimeBetweenEvictionRunsMillis(30000);
        p.setMaxActive(100);
        p.setInitialSize(10);
        p.setMaxWait(10000);
        p.setRemoveAbandonedTimeout(600);
        p.setMinEvictableIdleTimeMillis(30000);
        p.setMinIdle(10);
        p.setLogAbandoned(true);
        p.setRemoveAbandoned(true);
        p.setJdbcInterceptors(
                "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
                        + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer;"
                        + "org.apache.tomcat.jdbc.pool.interceptor.ResetAbandonedTimer");
        DataSource datasource = new DataSource();
        datasource.setPoolProperties(p);

        return datasource;
	}
	public static Connection getConnection(String host, int port,
			String username, String password, String database)
			throws ClassNotFoundException, SQLException {

		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		// Connection connection = (Connection) DriverManager
		// .getConnection("jdbc:mysql://" + host + "/" + database + "?"
		// + "user=" + username + "&password=" +
		// password+"&useUnicode=true&autoReconnect=true&characterEncoding=utf-8");

		Connection connection = (Connection) DriverManager
				.getConnection("jdbc:mysql://"
						+ host
						+ ":"
						+ String.valueOf(port)
						+ "/"
						+ database
						+ "?"
						+ "user="
						+ username
						+ "&password="
						+ password
						+ "&useUnicode=true&characterEncoding=utf-8&autoreconnect=true&failOverReadOnly=false&maxReconnects=10");

		// DriverManager.getConnection(DB_URL,USER,PASS);
		// connections.add(connection);
		LogService.info(DatabaseServiceConstant.TAG, "DatabaseUtility",
				"getConnection", "Connect to database(" + host + ") complete");

		return connection;

	}

	public static Connection getConnection(String host, int port,
			String username, String password) throws ClassNotFoundException,
			SQLException {

		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		Connection connection = (Connection) DriverManager.getConnection(
				"jdbc:mysql://" + host + ":"
						+ String.valueOf(port) , username, password);
		LogService.info(DatabaseServiceConstant.TAG, "DatabaseUtility",
				"getConnection", "Connect to server(" + host + ") complete");
		return connection;

	}

	/*
	 * public static Connection getConnection(int index) { return
	 * connections.get(index); }
	 */
	/*
	 * public void dropTable(int index, String table) { Statement stat = null;
	 * try { Connection con = connections.get(index);
	 * 
	 * stat = con.createStatement(); stat.executeUpdate(dropdbSQL + table);
	 * stat.close(); } catch (SQLException e) { e.printStackTrace();
	 * System.out.println("DropDB Exception :" + e.toString()); } finally {
	 * 
	 * } }
	 * 
	 * public void createTable(int index, String createTableSQL) { Statement
	 * stat = null; try { Connection con = connections.get(index);
	 * 
	 * stat = con.createStatement(); stat.executeUpdate(createTableSQL);
	 * 
	 * stat.close(); } catch (SQLException e) {
	 * System.out.println("CreateDB Exception :" + e.toString()); } finally { //
	 * Close(); } }
	 * 
	 * private void close(int index) {
	 * 
	 * }
	 */
}
