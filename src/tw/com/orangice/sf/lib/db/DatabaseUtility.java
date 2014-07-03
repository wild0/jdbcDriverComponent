package tw.com.orangice.sf.lib.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class DatabaseUtility {
	//static ArrayList<Connection> connections = new ArrayList<Connection>();
	//private static String dropdbSQL = "DROP TABLE ";
	
	/*
	public static Connection initial() throws SQLException,
			ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection connection = (Connection) DriverManager
				.getConnection("jdbc:mysql://localhost/?user=root&password=rootpassword");
		return connection;
	}
	*/
	/*
	public static void checkDatabase(Connection conn, String database) throws SQLException {
		//Connection connection = null;
		Statement statement = null;
		
		//try {
			//Class.forName("com.mysql.jdbc.Driver");
			//connection = DriverManager.getConnection("jdbc:mysql://localhost/",
			//		"root", "admin");
			statement = conn.createStatement();
			String sql = "CREATE DATABASE "+database;
			// To delete database: sql = "DROP DATABASE DBNAME";
			statement.executeUpdate(sql);
			System.out.println("Database created!");
			
		//catch (ClassNotFoundException e) {
			// No driver class found!
		//}
	}
	*/
	public static void createDatabase(Connection conn, String database) throws SQLException {
		Statement stat = null;
		//try {

			stat = conn.createStatement();
			stat.executeUpdate("CREATE DATABASE " + database);

			stat.close();
		//} catch (SQLException e) {
		//	System.out.println("CreateDB Exception :" + e.toString());
		//} finally {
			// Close();
		//}
	}

	public static Connection initial(String host, int port, String username,
			String password, String database) throws ClassNotFoundException,
			SQLException {

		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		//Connection connection = (Connection) DriverManager
		//		.getConnection("jdbc:mysql://" + host + "/" + database + "?"
		//				+ "user=" + username + "&password=" + password+"&useUnicode=true&autoReconnect=true&characterEncoding=utf-8");
		
		Connection connection = (Connection) DriverManager
				.getConnection("jdbc:mysql://" + host + "/" + database + "?"
						+ "user=" + username + "&password=" + password+"&useUnicode=true&autoReconnect=true&characterEncoding=utf-8");
		
		
		// DriverManager.getConnection(DB_URL,USER,PASS);
		//connections.add(connection);
		System.out.println("--"+connection.isValid(3000)+":"+host+","+username+","+password);
		
		return connection;

	}
	public static Connection initial(String host, int port, String username,
			String password) throws ClassNotFoundException,
			SQLException {

		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		Connection connection = (Connection) DriverManager.getConnection("jdbc:mysql://" + host + "/", username, password);
		//Connection connection = (Connection) DriverManager
		//		.getConnection("jdbc:mysql://" + host + "/" +  "?"
		//				+ "user=" + username + "&password=" + password);
		// DriverManager.getConnection(DB_URL,USER,PASS);
		//connections.add(connection);
		return connection;

	}
	
	/*
	public static Connection getConnection(int index) {
		return connections.get(index);
	}
	*/
	/*
	public void dropTable(int index, String table) {
		Statement stat = null;
		try {
			Connection con = connections.get(index);

			stat = con.createStatement();
			stat.executeUpdate(dropdbSQL + table);
			stat.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DropDB Exception :" + e.toString());
		} finally {

		}
	}

	public void createTable(int index, String createTableSQL) {
		Statement stat = null;
		try {
			Connection con = connections.get(index);

			stat = con.createStatement();
			stat.executeUpdate(createTableSQL);

			stat.close();
		} catch (SQLException e) {
			System.out.println("CreateDB Exception :" + e.toString());
		} finally {
			// Close();
		}
	}
	
	private void close(int index) {

	}
	*/
}
