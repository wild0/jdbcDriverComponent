package tw.com.orangice.sf.lib.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import tw.com.orangice.sf.lib.utility.SQLUtility;

public class DatabaseMassiveUpdateOperator {
	Connection conn =  null;
	PreparedStatement updateStatement = null;
	int batchSize = 100;
	long startTime = 0;
	public DatabaseMassiveUpdateOperator(Connection conn){
		this.conn = conn;
	}
	public PreparedStatement createInsertSQL(String table, String[] columns) throws SQLException{
		String sql = SQLUtility.convertInsertPreparedSQL(table, columns);
		updateStatement = conn.prepareStatement(sql);
		startTime = System.currentTimeMillis();
		return updateStatement;
		
	}
	public void  putValues( Object[] values) throws SQLException{
		for(int i=0;i<values.length;i++){
			if(values[i] instanceof Integer){
				updateStatement.setInt(i+1, (Integer) values[i]);
			}
			if(values[i] instanceof Long){
				updateStatement.setLong(i+1, (Long) values[i]);
			}
			if(values[i] instanceof String){
				updateStatement.setString(i+1, (String) values[i]);
			}
		}
		updateStatement.executeUpdate();
		
	}
	public long execute() throws SQLException{
		updateStatement.executeBatch();
		updateStatement.close();
		long endTime = System.currentTimeMillis();
        long elapsedTime = (endTime - startTime)/1000;
        
        return elapsedTime;

	}
}
