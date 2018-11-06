package tw.com.orangice.sf.lib.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONException;
import org.json.JSONObject;

import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.TableCompo;
import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.log.LogService;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MongodbUtility {
	
	public static DBObject convertInsert(String database, String table, String[] columns, Object[] values, String keyColumn, long keyId){
		BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
				.add("database", database)
				.add("table", table);
		 
			//BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder.start();
		documentBuilder.add(keyColumn, keyId);
			for(int i=0;i<columns.length;i++){
				documentBuilder.add(columns[i], values[i]);
			}
			
		 
			//documentBuilder.add("data", documentBuilderDetail.get());
			return documentBuilder.get();
			//collection.insert(documentBuilder.get());
		
	}
	public static Document convertInsertDocument(String database, String table, String[] columns, Object[] values, String keyColumn, long keyId){
		HashMap<String, Object> data = new HashMap<String, Object>();
		data.put(keyColumn, keyId);
		for(int i=0;i<columns.length;i++){
			data.put(columns[i], values[i]);
		}
		Document doc = new Document(data);
		return doc;
		
		//BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
		//		.add("database", database)
		//		.add("table", table);
		 
			//BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder.start();
		//documentBuilder.add(keyColumn, keyId);
			
			
		 
			//documentBuilder.add("data", documentBuilderDetail.get());
		//	return documentBuilder;
			//collection.insert(documentBuilder.get());
		
	}
	//public static DBObject convertInsert(String database, String table, JSONObject jsonData) throws JSONException{
		//String json = "{'database' : 'mkyongDB','table' : 'hosting'," +
		//		  "'detail' : {'records' : 99, 'index' : 'vps_index1', 'active' : 'true'}}}";
		//JSONObject insertJson = new JSONObject();
		//jsonData.put("_database", database);
		//jsonData.put("_table", table);
		//insertJson.a
		//insertJson.put("data", jsonData);
			 
		//		DBObject dbObject = (DBObject)JSON.parse(jsonData.toString());
		//		return dbObject;
			 
				//collection.insert(dbObject);
	//}
	public static Document convertFilterDocument( CriteriaCompo compo){
		Document doc = compo.renderBson();
		return doc;
	}
	public static Document convertUpdateDocument( String[] columns, Object[] values){
		Document update = new Document();
		Document updateSet = new Document();
		for(int i=0;i<columns.length;i++){
			updateSet.append(columns[i], values[i]);
		}
		update.append("$set", updateSet);
		return update;
	}
	public static DBObject convertUpdateSet(String table, String[] columns, Object[] values, CriteriaCompo compo){
		
		BasicDBObject updateQuery = new BasicDBObject();
		BasicDBObject updateSetQuery = new BasicDBObject();
		
		ArrayList<BasicDBObject> rowsData = new ArrayList<BasicDBObject>();
		for(int i=0;i<columns.length;i++){
			//updateQuery.("$set", 
			//BasicDBObject rowData = new BasicDBObject();
			//rowData.put(columns[i], values[i]);
			updateSetQuery.append(columns[i], values[i]);
//			condition);
			LogService.debug(DatabaseServiceConstant.TAG, MongodbUtility.class
						.getName(), "updateSQL", "UPDATE SQL:" + columns[i]+"=>"+values[i]);
			//break;
		}
		//conditionDBObject.put("$set", rowsData);
		
		//BasicDBObject conditionDBObject = compo.renderDBObject();
		updateQuery.append("$set", updateSetQuery);
		
		//conditionDBObject.put("multi", true);
		
		//BasicDBObject searchQuery = compo.renderDBObject();
	 
		//collection.updateMulti(searchQuery, updateQuery);			
		LogService.debug(DatabaseServiceConstant.TAG, MongodbUtility.class
				.getName(), "updateSQL", "UPDATE SQL:" + updateQuery.toString());
		//below statement set multi to true.
		//collection.update(searchQuery, updateQuery, false, true);
		return updateQuery;
	}
	public  static DBObject convertDelete(String table, CriteriaCompo compo){
		//BasicDBObject documentBuilderDetail = new BasicDBObject().append("table", table);
		BasicDBObject deleteDBObject = compo.renderDBObject();
		deleteDBObject.append("table", table);
		
		
		//documentBuilderDetail.append("data",deleteDBObject );
		
		return deleteDBObject;
	}
	public  static Document convertDeleteBson(String table, CriteriaCompo compo){
		//BasicDBObject documentBuilderDetail = new BasicDBObject().append("table", table);
		//BasicDBObject deleteDBObject = compo.renderDBObject();
		//deleteDBObject.append("table", table);
		
		
		Document doc = compo.renderBson();
		//documentBuilderDetail.append("data",deleteDBObject );
		
		return doc;
	}
	
	public  static DBObject convertCondition(String table, CriteriaCompo compo){
		BasicDBObject searchQuery = compo.renderDBObject();
		return searchQuery;
	}
	public static DBObject convertSelect(String table, CriteriaCompo compo){
		return null;
	}
	public static DBObject convertSelectJoin(TableCompo tables, CriteriaCompo compo){
		return null;
	}
}
