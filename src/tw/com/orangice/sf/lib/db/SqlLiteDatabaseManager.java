package tw.com.orangice.sf.lib.db;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;

import java.sql.Statement;

import tw.com.orangice.sf.lib.db._interface.DatabaseManagerInterface;
import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.QueryObjectsModel;
import tw.com.orangice.sf.lib.db.component.TableCompo;

public class SqlLiteDatabaseManager implements DatabaseManagerInterface {
	Connection conn = null;
	public SqlLiteDatabaseManager(String database){
		try{
			String driver = "org.sqlite.JDBC";
			Class.forName(driver);
			String dbName = database+".db"; 
			String dbUrl = "jdbc:sqlite:" + database;
			Connection conn = DriverManager.getConnection(dbUrl);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Override
	public void createTable(String tableSchema) throws SQLException, Exception {
		// TODO Auto-generated method stub
		
		Statement st = conn.createStatement();
        //st.executeUpdate("CREATE table village (id int, name varchar(20))");
        st.executeUpdate(tableSchema);
		
	}

	@Override
	public void createDatabase(String database) throws SQLException, Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long insertSQL(String table, String[] columns, Object[] values,
			String keyColumn) throws SQLException, Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int updateSQL(String table, String[] columns, Object[] values,
			CriteriaCompo condition) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int deleteSQL(String table, CriteriaCompo condition)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public QueryObjectsModel getObjects(String table, CriteriaCompo condition)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getCount(String table, CriteriaCompo condition)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public QueryObjectsModel getObjects(TableCompo tableCompo,
			CriteriaCompo condition) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getCount(TableCompo tableCompo, CriteriaCompo condition)
			throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isMongoDB() {
		// TODO Auto-generated method stub
		return false;
	}

}
