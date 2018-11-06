package tw.com.orangice.sf.lib.db.component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.bson.Document;

import tw.com.orangice.sf.lib.db._interface.DatabaseManagerInterface;
import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.log.LogService;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;



public class QueryObjectsModel {
	

	
	int dbType = 0;
	String calculateCollectionName = null;
	/*
	public void convert() {
		if(type==DB_MYSQL){
			
		}
		else if(type==DB_MONGO)
		DBCursor cursor = collection.find(andQuery);
	    while (cursor.hasNext()) {
	        System.out.println(cursor.next());
	    }
	}
	public void execute(){
		DBCursor cursor = collection.find(andQuery);
	}
	
	*/
	
	public boolean hasNext() throws SQLException{
		//System.out.println("QueryObjectModel:hasNext:size:"+rs.getFetchSize());
		if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			LogService.debug(DatabaseServiceConstant.TAG,
					this.getClass().getName(), "getObjects",  String.valueOf(rs.isLast()));
			
			//return !rs.isLast() ;
			return (!rs.isLast() && ((rs.getRow() != 0) || rs.isBeforeFirst()));
			//return rs.next();
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MONGO){
			//LogService.debug(DatabaseServiceConstant.TAG,
			//		this.getClass().getName(), "getObjects",  String.valueOf(cursor.hasNext())+":"+cursor.count());
			return cursor.hasNext();
			/*
			if(cursor.hasNext()){
				DBObject dbObj = cursor.next();
				return true;
			}
			else{
				return false;
			}
			*/
		}
		else {
			return false;
		}
	}
	
	Document row = null;
	public void next() throws SQLException{
		if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			rs.next();
			//return rs.next();
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MONGO){
			row =  cursor.next();
			LogService.debug(DatabaseServiceConstant.TAG,
					this.getClass().getName(), "getObjects",  String.valueOf(row.toString()));
		}
		
	}
	
	public String getString(String columnName) throws SQLException{
		if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			return rs.getString(columnName);
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MONGO){
			String tcolumn = Criteria.getSrcColumnName(columnName);//join發生時
			if(tcolumn.equals(columnName)){
				String val =  (String)row.get(tcolumn);
				if(val==null){
					return "";
				}
				else{
					return val;
				}
			}
			else{
				String val = (String)((DBObject)row.get("value")).get(tcolumn);
				if(val==null){
					return "";
				}
				else{
					return val;
				}
			}
		}
		else{
			return "";
		}
		
	}
	public int getInt(String columnName) throws SQLException, Exception{
		if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			return rs.getInt(columnName);
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MONGO){
			String tcolumn = Criteria.getSrcColumnName(columnName);
			if(tcolumn.equals(columnName)){
				if(row.get(tcolumn) instanceof Integer){
					return (Integer)row.get(tcolumn);
				}
				else if(row.get(tcolumn) instanceof Float){
					return ((Float)row.get(tcolumn)).intValue();
				}
				else if(row.get(tcolumn) instanceof Double){
					return ((Double)row.get(tcolumn)).intValue();
				}
			}
			else{
				//return (Integer)((DBObject)row.get("value")).get(tcolumn);
				
				if(row.get(tcolumn) instanceof Integer){
					return (Integer)((DBObject)row.get("value")).get(tcolumn);
				}
				else if(row.get(tcolumn) instanceof Float){
					return ((Float)((DBObject)row.get("value")).get(tcolumn)).intValue();
				}
				else if(row.get(tcolumn) instanceof Double){
					return ((Double)((DBObject)row.get("value")).get(tcolumn)).intValue();
				}
			}
		}
		
		return 0;
		
	}
	public long getLong(String columnName) throws SQLException, Exception{
		if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			return rs.getLong(columnName);
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MONGO){
			String tcolumn = Criteria.getSrcColumnName(columnName);
			System.out.println("Long tclumn:"+tcolumn+",columnName:"+columnName);
			if(tcolumn.equals(columnName)){
				return (Long)row.get(tcolumn);
			}
			else{
				//System.out.println("value:"+row.get("value"));
				//System.out.println("long:"+((DBObject)row.get("value")).get(tcolumn));
				return (Long)((DBObject)row.get("value")).get(tcolumn);
			}
					
		}
		else{
			return 0;
		}
	}
	
	public double getDouble(String columnName) throws SQLException, Exception{
		if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			return rs.getDouble(columnName);
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MONGO){
			String tcolumn = Criteria.getSrcColumnName(columnName);
			System.out.println("tclumn:"+tcolumn+",columnName:"+columnName);
			if(tcolumn.equals(columnName)){
				return (Double)row.get(tcolumn);
			}
			else{
				//System.out.println("value:"+row.get("value"));
				//System.out.println("long:"+((DBObject)row.get("value")).get(tcolumn));
				return (Double)((DBObject)row.get("value")).get(tcolumn);
			}
					
		}
		else{
			return 0;
		}
	}
	
	DatabaseManagerInterface dm = null;
	ResultSet rs = null;
	Connection conn = null;
	public QueryObjectsModel(ResultSet rs, Connection conn){
		this.rs = rs;
		this.conn = conn;
		dbType=DatabaseServiceConstant.DB_TYPE_MYSQL;
	}
	
	//DBCollection col = null;
	//DBCursor cursor = null;
	MongoCursor<Document> cursor = null;
	public QueryObjectsModel(FindIterable<Document> docs){
		//this.col = col;
		//cursor = col.find(dbObj);
		this.cursor= docs.iterator();
		dbType = DatabaseServiceConstant.DB_TYPE_MONGO;
		
	}
	public QueryObjectsModel(){
		
		dbType = DatabaseServiceConstant.DB_TYPE_MONGO;
		calculateCollectionName = String.valueOf(System.currentTimeMillis());
		
	}
	public String getCalculateCollectionKey(){
		return calculateCollectionName;
	}
	
	public void addJoin(String variable, MongoCollection<Document> collection,  String key, ArrayList<String> columns){
		//key:columns name index(orimKey)
		String map = getMapStr(key, columns);
		String reduce = getReduceStr();
		
		 System.out.println("addJoin:map:"+map); 
		 System.out.println("addJoin:reduce:"+reduce); 
		 MapReduceIterable<Document> reducedCollection = collection.mapReduce(map, reduce);

		
		//MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
		//		calculateCollectionName, MapReduceCommand.OutputType.REDUCE, null);
		
		//MapReduceOutput out = collection.mapReduce(cmd);
		//for (DBObject o : out.results()) {  
		//    System.out.println(o.toString());  
		//   }  
	
		//col.ma
		//MapReduceCommand mrc = new Map 
		//col.mapReduce(command)
		
	}
	DBCollection calculateCollection = null;
	/*
	public void execute(DBCollection calculateCollection, DBObject dbObj){
		System.out.println("execute:"+calculateCollection.getName()+","+dbObj.toString());
		if(dbType == DatabaseServiceConstant.DB_TYPE_MONGO){
			this.calculateCollection = calculateCollection;
			cursor = calculateCollection.find(dbObj);
		}
		else{
			
		}
		
	}
	*/
	
	public void close(){
		if(dbType == DatabaseServiceConstant.DB_TYPE_MONGO){
			if(cursor!=null){
				cursor.close();
				if(calculateCollection!=null){
					calculateCollection.drop();
				}
				//if(queryCollection.getName().equals(calculateCollection)){
				//	queryCollection.drop();
				//}
				
			}
		}
		else if(dbType==DatabaseServiceConstant.DB_TYPE_MYSQL){
			if(rs!=null){
				try {
					rs.close();
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	public String getMapStr(String key, ArrayList<String> columns){
		
		//key 為join的欄位
		
		String map = "function() { \n";
		map = map + " emit(this."+key+",{";
		for(int i=0;i<columns.size();i++){
			map = map + columns.get(i)+":this."+columns.get(i);
			if(i<(columns.size()-1)){
				map = map + ",";
			}
		}
		map = map + "});";
		map = map + "}";
		LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
				.getName(), "MapReduce", "MAP :" + map);
		//要顯示的欄位
		return map;
	}
	public String getReduceStr(){
		String reduce = "function(key, values){\n";
		reduce = reduce + "var result = {} ;\n";
		reduce = reduce + "values.forEach( function(value){\n";
		reduce = reduce + "var field;\n";
		reduce = reduce + "for(field in value)\n";
		//reduce = reduce + "if( value.hasOwnProperty(field) ){\n";
		//reduce = reduce + "result[field] = value[field];\n";
		//reduce = reduce + "};\n";
		
		reduce = reduce + "if( value.hasOwnProperty(field) ){\n";
		reduce = reduce + "result[field] = value[field];\n";
		reduce = reduce + "};\n";
		
		reduce = reduce + " });\n";
		reduce = reduce + "return result;\n";
		//reduce = reduce + "var field;\n";
		reduce = reduce + "};";
		
		LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
				.getName(), "MapReduce", "Reduce:" + reduce);
		
		
		return reduce;
	}
	
	
	public ResultSet getResultSet(){
		return rs;
	}
	public Connection getConnection(){
		return conn;
	}
	
}
