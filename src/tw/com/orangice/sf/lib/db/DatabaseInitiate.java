package tw.com.orangice.sf.lib.db;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apache.tomcat.jdbc.pool.DataSource;

import tw.com.orangice.sf.lib.db.DatabaseManager;
import tw.com.orangice.sf.lib.db.component.ScriptRunner;
import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.log.LogService;
import tw.com.orangice.sf.lib.utility.DatabaseUtility;

public class DatabaseInitiate {
	DatabaseManager dm = null;
	public DatabaseManager getDatabaseManager() {
		return dm;
	}
	public DatabaseInitiate(DatabaseManager dm){
		this.dm = dm;
	}
	public DatabaseInitiate(LogService logger, String host, int port, String username, String password, String database){
		//Connection conn;
		try {
			//conn = DatabaseUtility.getConnection(host, port, username, password,database);
			DataSource ds = DatabaseUtility.getTomcatDataSource(host, port, username, password, database);
			this.dm = new DatabaseManager(logger,ds);
			LogService.debug(DatabaseServiceConstant.TAG,
					DatabaseInitiate.class.getName(), "DatabaseInitiate",
					"initinate DatabaseManager instance complete");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogService.debug(DatabaseServiceConstant.TAG,
					DatabaseInitiate.class.getName(), "DatabaseInitiate",
					"initinate DatabaseManager instance fail", e);
		} catch(Exception e){
			e.printStackTrace();
			LogService.debug(DatabaseServiceConstant.TAG,
					DatabaseInitiate.class.getName(), "DatabaseInitiate",
					"initinate DatabaseManager instance fail", e);
		}
		
	}
	
	public static int createDatabase(LogService logger, String host, int port, String username, String password, String database){
		//independenace operation
		try {
			//Connection conn = DatabaseUtility.getConnection(host, port, username, password);
			//DatabaseManager dm = new DatabaseManager(logger,conn);
			DataSource ds = DatabaseUtility.getTomcatDataSource(host, port, username, password);
			DatabaseManager dm = new DatabaseManager(logger,ds);
			
			dm.createDatabase(database);
			
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "createDatabase", "create database complete:"+ds.getName());
			return 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "createDatabase", "create database fail", e);
			return -1;
		}	
	}
	
	public void dropTable(File dbSchema) throws URISyntaxException{
		//URI uri = getClass().getResource("/tw/com/orangice/sf/paperless/res/remove_db.sql").toURI();
		//System.out.println("uri:"+uri);
		
		try {
			FileReader reader = new FileReader(dbSchema);
			
			ScriptRunner sqlScript = new ScriptRunner(dm.newConnection(), false , false);
			sqlScript.runScript(reader);
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "dropTable", "drop table complete");
			//System.out.println(schema);
			//dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "dropTable", "drop table fail", e);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "dropTable", "drop table fail", e);
			e.printStackTrace();
		}
		
	}
	public void clearTable(File dbSchema) throws URISyntaxException{
		//URI uri = getClass().getResource("/tw/com/orangice/sf/paperless/res/remove_db.sql").toURI();
		//System.out.println("uri:"+uri);
		
		try {
			FileReader reader = new FileReader(dbSchema);
			
			ScriptRunner sqlScript = new ScriptRunner(dm.newConnection(), false , false);
			sqlScript.runScript(reader);
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "clearTable", "clear table complete");
			//System.out.println(schema);
			//dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "clearTable", "clear table fail", e);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "clearTable", "clear table fail", e);
			e.printStackTrace();
		}
		
	}
	public void executeQuery(File dbSchema) throws URISyntaxException{
		//URI uri = getClass().getResource("/tw/com/orangice/sf/paperless/res/remove_db.sql").toURI();
		//System.out.println("uri:"+uri);
		
		try {
			FileReader reader = new FileReader(dbSchema);
			
			ScriptRunner sqlScript = new ScriptRunner(dm.newConnection(), false , false);
			sqlScript.runScript(reader);
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "execute", "execute query complete");
			//System.out.println(schema);
			//dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "execute", "execute query fail", e);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "execute", "execute query fail", e);
			e.printStackTrace();
		}
		
	}
	public void initTable(File dbSchema) throws URISyntaxException{
		//URI uri = getClass().getResource("/tw/com/orangice/sf/paperless/res/db.sql").toURI();
		//System.out.println("uri:"+uri);
		
		//File dbSchema = new File(uri);
		try {
			FileReader reader = new FileReader(dbSchema);
			
			ScriptRunner sqlScript = new ScriptRunner(dm.newConnection(), false , false);
			sqlScript.runScript(reader);
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "initTable", "init table complete");
			//System.out.println(schema);
			//dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "initTable", "init table fail", e);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogService.debug(DatabaseServiceConstant.TAG, DatabaseInitiate.class.getName(), "initTable", "init table fail", e);
			e.printStackTrace();
		}
		/*
		try {
			System.out.println("uri:"+url.toURI());
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		//System.out.println("size:"+dbSchema.lastModified());
	}
	public String read(File file) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(file));
		StringBuilder sb = new StringBuilder();
		try {
	        
	        String line = br.readLine();

	        while (line != null) {
	            sb.append(line);
	            sb.append(System.getProperty("line.separator"));
	            line = br.readLine();
	        }
	        String everything = sb.toString();
	    } finally {
	        br.close();
	    }
		return sb.toString();
	}
	
}
