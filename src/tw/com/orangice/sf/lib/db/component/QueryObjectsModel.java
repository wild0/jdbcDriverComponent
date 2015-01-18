package tw.com.orangice.sf.lib.db.component;

import java.sql.Connection;
import java.sql.ResultSet;



public class QueryObjectsModel {
	ResultSet rs = null;
	Connection conn = null;
	
	public QueryObjectsModel(ResultSet rs, Connection conn){
		this.rs = rs;
		this.conn = conn;
	}
	public ResultSet getResultSet(){
		return rs;
	}
	public Connection getConnection(){
		return conn;
	}
}
