package tw.com.orangice.sf.lib.db.component;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TableCriteria {
	//for mongo db use
	String tableName = "";
	ArrayList<String> columns;
	String key = ""; //prikey
	
	public TableCriteria(String tableName, ArrayList<String> columns, String key){
		this.tableName = tableName;
		this.columns = columns;
		this.key = key;
	}
	
	ArrayList<BasicDBObject> joinDBObjs = new ArrayList<BasicDBObject>();
	ArrayList<BasicDBObject> normalDBObjs= new ArrayList<BasicDBObject>();
	
	public ArrayList<String> getColumns(){
		return columns;
	}
	public String getKey(){
		return key;
	}
	
	public void addJoinRelation(String keyColumn, String dstTable, String dstKey){
		//joinDBObjs.add(dbObj);
	}
	public void addNormalRelation(BasicDBObject dbObj){
		normalDBObjs.add(dbObj);
	}
	public ArrayList<BasicDBObject> getDBObjectSet(){
		return normalDBObjs;
	}
	public ArrayList<BasicDBObject> getJoinDBObjectSet(){
		return joinDBObjs;
	}
}
