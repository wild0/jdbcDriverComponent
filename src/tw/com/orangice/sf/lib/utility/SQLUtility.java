package tw.com.orangice.sf.lib.utility;

import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.TableCompo;

public class SQLUtility {
	public static String convertInsertPreparedSQL(String table, String[] columns){
		String sql = "INSERT into "+table +" (";
		for(int i = 0; i<columns.length;i++){
			if(i!=0){
				sql = sql + ",";
			}
			sql = sql + columns[i];
		}
		sql = sql + ") VALUES (";
		for(int i = 0; i<columns.length;i++){
			if(i!=0){
				sql = sql + ",";
			}
			sql = sql + "?";
			
		}
		sql = sql + ")";
		return sql; 
	}
	public static String convertInsertSQL(String table, String[] columns, Object[] values){
		String sql = "INSERT into "+table +" (";
		for(int i = 0; i<columns.length;i++){
			if(i!=0){
				sql = sql + ",";
			}
			sql = sql + columns[i];
		}
		sql = sql + ") VALUES (";
		for(int i = 0; i<columns.length;i++){
			if(i!=0){
				sql = sql + ",";
			}
			if(values[i]==null){
				sql = sql + "\"\"";
			}
			else{
				sql = sql + "\""+(values[i]).toString().replace("\"", "\\\"")+"\"";
			}
		}
		sql = sql + ")";
		//System.out.println("convertInsertSQL:"+sql);
		return sql; 
	}
	public static String convertUpdateSQL(String table, String[] columns, Object[] values, CriteriaCompo compo){
		String sql = "UPDATE  "+table +" set ";
		for(int i = 0; i<columns.length;i++){
			if(i!=0){
				sql = sql + ",";
			}
			//sql = sql + columns[i] + " = \""+values[i]+"\" ";
			if(values[i]==null){
				sql = sql  + columns[i] +  " = \"\"";
			}
			else{
				sql = sql + columns[i] + " = \""+(values[i]).toString().replace("\"", "\\\"")+"\"";
			}
		}
		
		sql = sql + " ";
		sql = sql + compo.render();
		//System.out.println("convertUpdateSQL:"+sql);
		return sql; 
	}
	
	
	public static String convertDeleteSQL(String table, CriteriaCompo condition){
		String sql = "DELETE FROM "+table+ " "+ condition.render();
		
		return sql;
		
	}
	
	public static String convertSelectSQL(String table, CriteriaCompo condition){
		String sql = "SELECT * FROM "+table+ " "+ condition.render();
		return sql;
		
	}
	public static String convertSelectSQL(TableCompo tableCompo, CriteriaCompo condition){
		String sql = "SELECT * FROM "+tableCompo.render()+ " "+ condition.render();
		//System.out.println("sql:"+sql);
		return sql;
		
	}
	public static String convertCountSQL(TableCompo tableCompo, CriteriaCompo condition){
		String sql = "SELECT COUNT(*) AS COUNT FROM "+tableCompo.render()+ " "+ condition.render();
		//System.out.println("sql:"+sql);
		return sql;
	}
	public static String convertCountSQL(String table, CriteriaCompo condition){
		String sql = "SELECT  COUNT(*) AS COUNT FROM "+table+ " "+ condition.render();
		return sql;
		
	}
}
