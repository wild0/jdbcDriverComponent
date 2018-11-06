package tw.com.orangice.sf.lib.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Random;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import tw.com.orangice.sf.lib.db._interface.DatabaseManagerInterface;
import tw.com.orangice.sf.lib.db.component.Criteria;
import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.CriteriaElement;
import tw.com.orangice.sf.lib.db.component.QueryObjectsModel;
import tw.com.orangice.sf.lib.db.component.TableCompo;
import tw.com.orangice.sf.lib.db.component.TableCriteria;
import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.log.LogService;
import tw.com.orangice.sf.lib.utility.MongodbUtility;

public class MongoDatabaseManager implements DatabaseManagerInterface {
	MongoClient mongoClient;
	MongoDatabase db = null;
	String host = null;
	int port = 0;
	String username = "";
	String password = "";
	LogService logger = null;

	// Connection conn = null;

	public MongoDatabaseManager(LogService logger, String host, int port,
			String username, String password, String database) {

		try {
			MongoCredential credential = MongoCredential.createCredential(
					username, database, password.toCharArray());
			
			
			
			
			mongoClient = new MongoClient(new ServerAddress(host,
					port), Arrays.asList(credential));
			
			//MongoClientURI uri = new MongoClientURI("mongodb://root:password123@140.115.35.136:27017/pingpong");
			//MongoClient mongoClient = new MongoClient(uri);
			
			db = mongoClient.getDatabase(database);
			System.out.println("aaa:");
			MongoIterable<String> strings=mongoClient.listDatabaseNames();
			  MongoCursor<String> iterator=strings.iterator();
			  while (iterator.hasNext()) {
			    //LOG.info("Database: {}",iterator.next());
			    System.out.println("aaa:"+iterator.next());
			  }
			this.logger = logger;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * try { conn = DatabaseUtility.getMongoDBConnection(host, port,
		 * username, password, database); } catch (ClassNotFoundException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } catch
		 * (SQLException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */

	}

	public MongoDatabaseManager(LogService logger, String host, int port,
			String database) {

		try {
			// MongoCredential credential =
			// MongoCredential.createCredential(username, database,
			// password.toCharArray());
			mongoClient = new MongoClient(new ServerAddress(host,
					port));
			
			db = mongoClient.getDatabase(database);
			
			this.logger = logger;
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * try { conn = DatabaseUtility.getMongoDBConnection(host, port,
		 * username, password, database); } catch (ClassNotFoundException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } catch
		 * (SQLException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */

	}
	

	/*
	 * public int getCount(String table, CriteriaCompo condition) throws
	 * SQLException{
	 * 
	 * //Connection conn = ds.getConnection(); String sql =
	 * SQLUtility.convertCountSQL(table, condition);
	 * 
	 * Statement stat = conn.createStatement(); LogService
	 * .debug(DatabaseServiceConstant.TAG, this.getClass().getName(),
	 * "getCount", "QUERY_COUNT sql:" + sql); ResultSet rs =
	 * stat.executeQuery(sql); int count = 0; while (rs.next()) { count =
	 * rs.getInt("COUNT"); LogService.debug(DatabaseServiceConstant.TAG,
	 * this.getClass() .getName(), "getCount", "QUERY_COUNT result:" + count); }
	 * 
	 * if (conn != null) { try { conn.close(); } catch (SQLException e) {
	 * e.printStackTrace(); } } return count; }
	 * 
	 * public String insertSQL(String table, String[] columns, Object[] values,
	 * String keyColumn) {
	 * 
	 * 
	 * //
	 * logger.info("DB:Insert:"+table+"("+columns.length+","+values.length+")");
	 * DBCollection coll = db.getCollection(table); BasicDBObject obj =
	 * MongodbUtility.convertInsert(table, columns, values);
	 * 
	 * 
	 * 
	 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass() .getName(),
	 * "insertSQL", "INSERT SQL:" + obj.toString());
	 * 
	 * WriteResult rw = coll.insert(obj);
	 * 
	 * return rw.getField(name) }
	 * 
	 * 
	 * public int deleteSQL(String table, CriteriaCompo condition) throws
	 * SQLException { Connection conn = ds.getConnection(); try { String sql =
	 * SQLUtility.convertDeleteSQL(table, condition);
	 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass() .getName(),
	 * "deleteSQL", "DELETE SQL:" + sql); Statement stat =
	 * conn.createStatement(); int result = stat.executeUpdate(sql);
	 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass() .getName(),
	 * "deleteSQL", "DELETE result:" + result); return result; } catch
	 * (SQLException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); LogService.debug(DatabaseServiceConstant.TAG,
	 * this.getClass() .getName(), "deleteSQL", "DELETE fail", e); if (conn !=
	 * null) { try { conn.rollback(); } catch (SQLException ex) {
	 * ex.printStackTrace(); } } // return SQLErrorCode.SQL_INSERT_FAIL_CODE;
	 * return -1; }finally { if (conn != null) { try { conn.close(); } catch
	 * (SQLException e) { e.printStackTrace(); } } } }
	 */
	public long insertSQL(String table, String[] columns, Object[] values,
			String keyColumn) throws SQLException {
		// Connection conn = ds.getConnection();

		MongoCollection<Document> collection = checkCollection(table);
		
		// DBCollection coll = db.getCollection(db.getName());
		Random randnum = new Random();
        randnum.setSeed(System.currentTimeMillis());
		
		//long id = collection.count();
        long id = randnum.nextLong();
		Document document = MongodbUtility.convertInsertDocument(db.getName(), table,
				columns, values, keyColumn, id);

		//LogService.debug(DatabaseServiceConstant.TAG,
		//		this.getClass().getName(), "insertSQL",
		//		"INSERT SQL:" + document.toString());
		
		InsertOneOptions options = new InsertOneOptions();
		collection.insertOne(document, options);
		//String insertId = document.getObjectId("_id").toString();

		return id;
	}

	public int updateSQL(String table, String[] columns, Object[] values,
			CriteriaCompo condition) throws SQLException {
		// Connection conn = ds.getConnection();
		try {
			// DBCollection coll = db.getCollection(table);

			// condition);
			//LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
			//		.getName(), "updateSQL", "UPDATE SQL:" + table);
			MongoCollection<Document> collection = db.getCollection(table);

			//DBObject dbObj = MongodbUtility.convertUpdateSet(table, columns,
			//		values, condition);
			Document filter = MongodbUtility.convertFilterDocument(condition);
			Document update = MongodbUtility.convertUpdateDocument(columns, values);
			//LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
			//		.getName(), "updateSQL",
			//		"update(" + condition.renderDBObject().toString() + ","
			//				+ dbObj.toString() + ")");
			System.out.println("filter:"+filter.toJson());
			System.out.println("update:"+update.toJson());
			UpdateResult ur = collection.updateMany(filter, update);
			//WriteResult rw = coll.update(condition.renderDBObject(), dbObj,
			//		true, false);
			//LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
			//		.getName(), "updateSQL", "UPDATE SQL RESULT[" + rw.getN()
			//		+ "]:" + rw.getUpsertedId());
			// String sql = SQLUtility.convertUpdateSQL(table, columns, values,
			// condition);

			// Statement stat = conn.createStatement();
			// int result = stat.executeUpdate(sql);
			// .debug(DatabaseServiceConstant.TAG, this.getClass()
			// .getName(), "updateSQL", "UPDATE result:" + result);
			// stat.execute(sql, Statement.RETURN_GENERATED_KEYS);
			// stat.close();
			// return result;
			return (int)ur.getModifiedCount();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			/*
			 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
			 * .getName(), "updateSQL", "UPDATE fail", e); if (conn != null) {
			 * try { conn.rollback(); } catch (SQLException ex) {
			 * ex.printStackTrace(); } }
			 */
			// return SQLErrorCode.SQL_INSERT_FAIL_CODE;
			return -1;
		} 
	}
	
	public int dropCollection(String table){
		try{
			db.getCollection(table).drop();
			return 0;
		}
		catch(Exception e){
			e.printStackTrace();
			return -1;
		}
	}
	public MongoCollection<Document> checkCollection(String table){
		MongoCollection<Document> collection = db.getCollection(table);
		if(collection!=null){
			return collection;
		}
		else{
			db.createCollection(table);
			return db.getCollection(table);
		}
	}

	public int deleteSQL(String table, CriteriaCompo condition)
			throws SQLException {
		// Connection conn = ds.getConnection();

		// DBCollection coll = db.getCollection(table);
		MongoCollection<Document> collection = db.getCollection(table);
		// long id = coll.getCount();
		Document filter = MongodbUtility.convertDeleteBson(table, condition);

		// LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		// .getName(), "deleteSQL",
		// "["+db.getName()+"]DELETE SQL["+coll.findOne()+"]:" +
		// obj.toString());
		DeleteResult dr = collection.deleteMany(filter);


		
		return (int)dr.getDeletedCount();

		/*
		 * try { String sql = SQLUtility.convertDeleteSQL(table, condition);
		 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		 * .getName(), "deleteSQL", "DELETE SQL:" + sql); Statement stat =
		 * conn.createStatement(); int result = stat.executeUpdate(sql);
		 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		 * .getName(), "deleteSQL", "DELETE result:" + result); return result; }
		 * catch (SQLException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); LogService.debug(DatabaseServiceConstant.TAG,
		 * this.getClass() .getName(), "deleteSQL", "DELETE fail", e); if (conn
		 * != null) { try { conn.rollback(); } catch (SQLException ex) {
		 * ex.printStackTrace(); } } // return
		 * SQLErrorCode.SQL_INSERT_FAIL_CODE; return -1; }finally { if (conn !=
		 * null) { try { conn.close(); } catch (SQLException e) {
		 * e.printStackTrace(); } } }
		 */
		// return 0;
	}

//	public QueryObjectsModel getObjects(String table, CriteriaCompo condition)
//			throws SQLException {
//		// Connection conn = ds.getConnection();
//		LogService.debug(DatabaseServiceConstant.TAG,
//				this.getClass().getName(), "getObjects", "start");
//		//DBCollection coll = db.getCollection(table);
//		// DBObject dbObj = MongodbUtility.convertSelect();
//		MongoCollection<Document> collection = db.getCollection(table);
//		Document filter = MongodbUtility.convertFilterDocument(condition);
//		LogService.debug(DatabaseServiceConstant.TAG,
//				this.getClass().getName(), "getObjects", filter.toJson());
//		FindIterable<Document> iterable = collection.find(filter);
//		
//		
//		return new QueryObjectsModel(iterable);
//	}
	
	public QueryObjectsModel getObjects(String table, CriteriaCompo condition)
			throws SQLException {
		// Connection conn = ds.getConnection();
		FindIterable<Document> iterable = null;
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects", "start");
		//DBCollection coll = db.getCollection(table);
		// DBObject dbObj = MongodbUtility.convertSelect();
		MongoCollection<Document> collection = db.getCollection(table);
		Document filter = MongodbUtility.convertFilterDocument(condition);
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects", filter.toJson());
		
		String sortBy = condition.getSortBy();
		System.out.println("getObjects:sortBy:"+sortBy);
		if(!sortBy.equals("")){
			String order = condition.getOrder();
			System.out.println("getObjects:order:"+order);
			Document sort = new Document();
			if(order.equals("DESC")){
				sort.put(sortBy, 1);
				iterable = collection.find(filter).sort(Sorts.descending(sortBy));
			}
			else{
				sort.put(sortBy, 0);
				iterable = collection.find(filter).sort(Sorts.ascending(sortBy));
			}
			//iterable = collection.find(filter).sort(Sorts.descending(sortBy));
			
		}
		else{
			iterable = collection.find(filter);
		}
		
		
		
		
		
		return new QueryObjectsModel(iterable);
	}

	public long getCount(String table, CriteriaCompo condition)
			throws SQLException {
		/*
		 * //Connection conn = ds.getConnection(); String sql =
		 * SQLUtility.convertCountSQL(table, condition);
		 * 
		 * Statement stat = conn.createStatement(); LogService
		 * .debug(DatabaseServiceConstant.TAG, this.getClass().getName(),
		 * "getCount", "QUERY_COUNT sql:" + sql); ResultSet rs =
		 * stat.executeQuery(sql); int count = 0; while (rs.next()) { count =
		 * rs.getInt("COUNT"); LogService.debug(DatabaseServiceConstant.TAG,
		 * this.getClass() .getName(), "getCount", "QUERY_COUNT result:" +
		 * count); }
		 * 
		 * if (conn != null) { try { conn.close(); } catch (SQLException e) {
		 * e.printStackTrace(); } } return count;
		 */
		MongoCollection<Document> collection = db.getCollection(table);
		Document filter = MongodbUtility.convertFilterDocument(condition);
		long count = collection.count(filter);
		return count;
	}

//	public int getCount(TableCompo tableCompo, CriteriaCompo condition)
//			throws SQLException {
//
//		return 0;
//	}
	/*
	public QueryObjectsModel getObjects(TableCompo tableCompo,
			CriteriaCompo condition) throws SQLException {
		// Connection conn = ds.getConnection();

		String targetTable = "";
		Hashtable<String, TableCriteria> tables = new Hashtable<String, TableCriteria>();
		for (int i = 0; i < tableCompo.getTables().size(); i++) {

			String table = tableCompo.getTables().get(i);
			if(i==0){
				targetTable = table;
			}
			
			String variable = tableCompo.getVariables().get(i);
			String key = tableCompo.getKeys().get(i);

			System.out.println("[" + i + "]getObjects:table:" + table
					+ ",variable:" + variable + ",key:" + key);

			ArrayList<String> columns = tableCompo.getColumns(i);
			TableCriteria tc = new TableCriteria(table, columns, key);

			tables.put(variable, tc);
		}

		
		
		QueryObjectsModel qom = new QueryObjectsModel();
		
		
		//DBCollection calculateCollection = db.createCollection(
		//		qom.getCalculateCollectionKey(), new BasicDBObject());
		
		//DBCollection firstCollection = null;
		MongoCollection<Document> firstCollection = null;
		

		for (int l = 0; l < tables.size(); l++) {

			String table = tableCompo.getTables().get(l); // get table name
			//DBCollection coll = db.getCollection(table);
			MongoCollection<Document> collection = db.getCollection(table);
			firstCollection = collection;

			String tableVariable = tableCompo.getVariables().get(l); // get
																		// table
																		// variable
			TableCriteria tc = tables.get(tableVariable);

			// BasicDBObject dbObj = new BasicDBObject();
			// dbObj.put("$and", tc.getDBObjectSet());
			// dbObj.put("$and", tc.getJoinDBObjectSet());

			System.out.println("[" + l + "]getObjects:keys:"
					+ tableCompo.getKeys().get(l));
			System.out.println("[" + l + "]getObjects:column:"
					+ tableCompo.getColumns(l));

			Criteria c = condition.getJoinCriteria(tableVariable);
			if (c != null) {
				qom.addJoin(tableVariable, collection, c.getSrcColumnName(), // 作為join的Index
						tableCompo.getColumns(l));
			} else {
				qom.addJoin(tableVariable, collection, tableCompo.getKeys().get(l), // 作為join的Index
						tableCompo.getColumns(l));
			}


			// qom.addJoin(tableVariable, coll,
			// tableCompo.getKeys().get(l), //作為join的Index
			// tableCompo.getColumns(l));
			// coll.mapReduce(map, reduce, outputTarget, query)
			// qom.addQuery(coll, dbObj);

			// QueryObjectsModel qom = new QueryObjectsModel(coll, dbObj);
			// /qoms.add(qom);
			// qom.query()

		}
		
		MongoCollection<Document> collection = db.getCollection(targetTable);
		Document filter = MongodbUtility.convertFilterDocument(condition);
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects", filter.toJson());
		FindIterable<Document> iterable = collection.find(filter);


		BasicDBObject queryDBObject = new BasicDBObject();
		// ArrayList<Criteria> criterias = condition.getCriteras();
		ArrayList<String> logicals = condition.getConditions();
		ArrayList<CriteriaElement> cs = condition.getCriteriaElements();
		ArrayList<BasicDBObject> dbObjs = new ArrayList<BasicDBObject>();
		
		for (int i = 0; i < cs.size(); i++) {
			if(cs.get(i) instanceof Criteria){
			if (((Criteria)cs.get(i)).isQuote()) {
				// cs.get(i).getDBObject(queryDBObject);
				queryDBObject = ((Criteria)cs.get(i)).getJoinDBObject("value", queryDBObject);
				// dbObjs.add(dbObj);
			} else {
				BasicDBObject dbObj = null;
				if (i - 1 > -1) {
					// dbObj = cs.get(i).getDBObject(logicals.get(i-1));
					//queryDBObject = cs.get(i).getJoinDBObject("value", queryDBObject, logicals.get(i - 1));
				} else {
					// dbObj = cs.get(i).getDBObject();
					//queryDBObject = cs.get(i).getJoinDBObject("value", queryDBObject);
				}
			}
			}
			System.out.println("criteria:"+queryDBObject.toString());
			// dbObjs.add(dbObj);
		}

		// if(dbObjs.size()>0){
		// queryDbObjs.put("$and", dbObjs);
		// }

		//qom.execute(calculateCollection, queryDBObject);
		
		// DBObject dbObj = null;

		return qom;
	}
	*/

	@Override
	public void createTable(String tableSchema) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createDatabase(String database) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isMongoDB() {
		// TODO Auto-generated method stub
		return true;
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

}
