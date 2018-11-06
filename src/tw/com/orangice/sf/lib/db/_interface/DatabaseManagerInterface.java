package tw.com.orangice.sf.lib.db._interface;

import java.sql.SQLException;
import java.util.ArrayList;

import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.QueryObjectsModel;
import tw.com.orangice.sf.lib.db.component.TableCompo;

public interface DatabaseManagerInterface {
	public void createTable(String tableSchema) throws SQLException, Exception ;
	public void createDatabase(String database) throws SQLException, Exception ;
	
	public long insertSQL(String table, String[] columns, Object[] values,
			String keyColumn) throws SQLException, Exception ;
	public int updateSQL(String table, String[] columns, Object[] values,
			CriteriaCompo condition) throws SQLException;
	
	public int deleteSQL(String table, CriteriaCompo condition)
			throws SQLException;
	
	public QueryObjectsModel getObjects(String table, CriteriaCompo condition)
			throws SQLException;
	public long getCount(String table, CriteriaCompo condition)
			throws SQLException;
	public QueryObjectsModel getObjects(TableCompo tableCompo, CriteriaCompo condition)
			throws SQLException;
	public long getCount(TableCompo tableCompo, CriteriaCompo condition)
			throws SQLException;
	
	public boolean isMongoDB();
	
	
}
