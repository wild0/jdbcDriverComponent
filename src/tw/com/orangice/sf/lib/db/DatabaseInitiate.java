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

import tw.com.orangice.sf.lib.db.DatabaseManager;
import tw.com.orangice.sf.lib.db.DatabaseUtility;
import tw.com.orangice.sf.lib.db.component.ScriptRunner;

public class DatabaseInitiate {
	DatabaseManager dm = null;
	public DatabaseManager getDm() {
		return dm;
	}
	public DatabaseInitiate(DatabaseManager dm){
		this.dm = dm;
	}
	public DatabaseInitiate(Logger logger, String host, int port, String username, String password, String database){
		Connection conn;
		try {
			conn = DatabaseUtility.initial(host, port, username, password, database);
			this.dm = new DatabaseManager(logger,conn);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void close() {
		try {
			dm.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static  int createDatabase(Logger logger, String host, int port, String username, String password, String database){
		Connection conn;
		try {
			conn = DatabaseUtility.initial(host, port, username, password);
			DatabaseManager dm = new DatabaseManager(logger,conn);
			dm.createDatabase(database);
			conn.close();
			return 1;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}	
	}
	
	public void dropTable(File dbSchema) throws URISyntaxException{
		//URI uri = getClass().getResource("/tw/com/orangice/sf/paperless/res/remove_db.sql").toURI();
		//System.out.println("uri:"+uri);
		
		try {
			FileReader reader = new FileReader(dbSchema);
			
			ScriptRunner sqlScript = new ScriptRunner(dm.getConnection(), false , false);
			sqlScript.runScript(reader);
			//System.out.println(schema);
			//dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("IOException:"+e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("SQLException:"+e.getMessage());
			e.printStackTrace();
		}
		
		/*
		File dbSchema = new File(uri);
		try {
			String schema = read(dbSchema);
			System.out.println(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	public void initTable(File dbSchema) throws URISyntaxException{
		//URI uri = getClass().getResource("/tw/com/orangice/sf/paperless/res/db.sql").toURI();
		//System.out.println("uri:"+uri);
		
		//File dbSchema = new File(uri);
		try {
			FileReader reader = new FileReader(dbSchema);
			
			ScriptRunner sqlScript = new ScriptRunner(dm.getConnection(), false , false);
			sqlScript.runScript(reader);
			//System.out.println(schema);
			//dm.executeQuery(schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("IOException:"+e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("SQLException:"+e.getMessage());
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
		System.out.println("size:"+dbSchema.lastModified());
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
