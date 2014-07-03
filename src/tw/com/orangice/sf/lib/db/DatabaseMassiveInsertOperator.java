package tw.com.orangice.sf.lib.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import tw.com.orangice.sf.lib.utility.SQLUtility;

public class DatabaseMassiveInsertOperator {
	Connection conn =  null;
	PreparedStatement insertStatement = null;
	int batchSize = 100;
	long startTime = 0;
	public DatabaseMassiveInsertOperator(Connection conn){
		this.conn = conn;
	}
	public PreparedStatement createInsertSQL(String table, String[] columns) throws SQLException{
		String sql = SQLUtility.convertInsertPreparedSQL(table, columns);
		insertStatement = conn.prepareStatement(sql);
		startTime = System.currentTimeMillis();
		return insertStatement;
		
	}
	public void  putValues( Object[] values) throws SQLException{
		for(int i=0;i<values.length;i++){
			if(values[i] instanceof Integer){
				insertStatement.setInt(i+1, (Integer) values[i]);
			}
			if(values[i] instanceof Long){
				insertStatement.setLong(i+1, (Long) values[i]);
			}
			if(values[i] instanceof String){
				insertStatement.setString(i+1, (String) values[i]);
			}
		}
		insertStatement.executeUpdate();
		
	}
	public long execute() throws SQLException{
		insertStatement.executeBatch();
		insertStatement.close();
		long endTime = System.currentTimeMillis();
        long elapsedTime = (endTime - startTime)/1000;
        
        return elapsedTime;

	}
}
