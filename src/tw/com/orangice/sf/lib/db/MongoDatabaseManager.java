package tw.com.orangice.sf.lib.db;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import tw.com.orangice.sf.lib.db._interface.DatabaseManagerInterface;
import tw.com.orangice.sf.lib.db.component.Criteria;
import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.db.component.CriteriaElement;
import tw.com.orangice.sf.lib.db.component.QueryObjectsModel;
import tw.com.orangice.sf.lib.db.component.TableCompo;
import tw.com.orangice.sf.lib.db.component.TableCriteria;
import tw.com.orangice.sf.lib.db.constant.DatabaseServiceConstant;
import tw.com.orangice.sf.lib.log.LogService;
import tw.com.orangice.sf.lib.utility.DatabaseUtility;
import tw.com.orangice.sf.lib.utility.MongodbUtility;
import tw.com.orangice.sf.lib.utility.SQLUtility;

public class MongoDatabaseManager implements DatabaseManagerInterface {

	DB db = null;
	String host = null;
	int port = 0;
	String username = "";
	String password = "";

	// Connection conn = null;

	public MongoDatabaseManager(LogService logger, String host, int port,
			String username, String password, String database) {

		try {
			MongoCredential credential = MongoCredential.createCredential(
					username, database, password.toCharArray());
			MongoClient mongoClient = new MongoClient(new ServerAddress(host,
					port), Arrays.asList(credential));
			db = mongoClient.getDB(database);
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
			MongoClient mongoClient = new MongoClient(new ServerAddress(host,
					port));
			db = mongoClient.getDB(database);
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

		DBCollection coll = db.getCollection(table);
		// DBCollection coll = db.getCollection(db.getName());
		long id = coll.getCount();
		DBObject obj = MongodbUtility.convertInsert(db.getName(), table,
				columns, values, keyColumn, id);

		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "insertSQL",
				"INSERT SQL:" + obj.toString());

		WriteResult rw = coll.insert(obj);
		// long id = (Long) obj.toMap().get(keyColumn);
		// return rw.getField("id");
		return id;
		/*
		 * try { //
		 * logger.info("DB:Insert:"+table+"("+columns.length+","+values.
		 * length+")");
		 * 
		 * String sql = SQLUtility.convertInsertSQL(table, columns, values);
		 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		 * .getName(), "insertSQL", "INSERT SQL:" + sql); Statement stat =
		 * conn.createStatement(); // stat.execute(sql,
		 * Statement.RETURN_GENERATED_KEYS);
		 * 
		 * long count = stat.executeUpdate(sql,
		 * Statement.RETURN_GENERATED_KEYS); ResultSet keys =
		 * stat.getGeneratedKeys();
		 * 
		 * keys.next(); int keyId = keys.getInt(1);
		 * 
		 * LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		 * .getName(), "insertSQL", "Insert key:" + keyId); stat.close();
		 * 
		 * return keyId; } catch (SQLException e) { // TODO Auto-generated catch
		 * block LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		 * .getName(), "insertSQL", "Insert fail", e); e.printStackTrace(); //
		 * conn.rollback(); if (conn != null) { try { conn.rollback(); } catch
		 * (SQLException ex) { ex.printStackTrace(); } } // return
		 * SQLErrorCode.SQL_INSERT_FAIL_CODE; return -1; } finally { if (conn !=
		 * null) { try { conn.close(); } catch (SQLException e) {
		 * e.printStackTrace(); } } }
		 */

	}

	public int updateSQL(String table, String[] columns, Object[] values,
			CriteriaCompo condition) throws SQLException {
		// Connection conn = ds.getConnection();
		try {
			// DBCollection coll = db.getCollection(table);

			// condition);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "updateSQL", "UPDATE SQL:" + table);

			DBCollection coll = db.getCollection(table);
			DBObject dbObj = MongodbUtility.convertUpdateSet(table, columns,
					values, condition);

			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "updateSQL",
					"update(" + condition.renderDBObject().toString() + ","
							+ dbObj.toString() + ")");

			WriteResult rw = coll.update(condition.renderDBObject(), dbObj,
					true, false);
			LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
					.getName(), "updateSQL", "UPDATE SQL RESULT[" + rw.getN()
					+ "]:" + rw.getUpsertedId());
			// String sql = SQLUtility.convertUpdateSQL(table, columns, values,
			// condition);

			// Statement stat = conn.createStatement();
			// int result = stat.executeUpdate(sql);
			// .debug(DatabaseServiceConstant.TAG, this.getClass()
			// .getName(), "updateSQL", "UPDATE result:" + result);
			// stat.execute(sql, Statement.RETURN_GENERATED_KEYS);
			// stat.close();
			// return result;
			return rw.getN();
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
		} finally {

			/*
			 * if (conn != null) { try { conn.close(); } catch (SQLException e)
			 * { e.printStackTrace(); } }
			 */
		}
	}

	public int deleteSQL(String table, CriteriaCompo condition)
			throws SQLException {
		// Connection conn = ds.getConnection();

		// DBCollection coll = db.getCollection(table);
		DBCollection coll = db.getCollection(table);
		// long id = coll.getCount();
		DBObject obj = MongodbUtility.convertDelete(table, condition);

		// LogService.debug(DatabaseServiceConstant.TAG, this.getClass()
		// .getName(), "deleteSQL",
		// "["+db.getName()+"]DELETE SQL["+coll.findOne()+"]:" +
		// obj.toString());

		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "deleteSQL", "[" + db.getName()
						+ "]DELETE SQL:" + obj.toString());
		// coll.find(obj).count();
		WriteResult rw = coll.remove(obj);

		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "deleteSQL", "[" + db.getName()
						+ "]DELETE Count:" + rw.getN());
		// return rw.getField("id");
		return rw.getN();

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

	public QueryObjectsModel getObjects(String table, CriteriaCompo condition)
			throws SQLException {
		// Connection conn = ds.getConnection();
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects", "start");
		DBCollection coll = db.getCollection(table);
		// DBObject dbObj = MongodbUtility.convertSelect();

		BasicDBObject queryDBObject = new BasicDBObject();
		ArrayList<String> logicals = condition.getConditions();
		ArrayList<CriteriaElement> cs = condition.getCriteriaElements();
		//ArrayList<BasicDBObject> dbObjs = new ArrayList<BasicDBObject>();
		for (int i = 0; i < cs.size(); i++) {

			BasicDBObject dbObj = null;
			if (i - 1 > -1) {
				// dbObj = cs.get(i).getDBObject(logicals.get(i-1));
				if(cs.get(i) instanceof Criteria){
					dbObj = ((Criteria)cs.get(i)).getDBObject(queryDBObject, logicals.get(i - 1));
				}
			} else {
				// dbObj = cs.get(i).getDBObject();
				if(cs.get(i) instanceof Criteria){
					dbObj = ((Criteria)cs.get(i)).getDBObject(queryDBObject);
				}
			}

			// dbObjs.add(dbObj);
			
		}
		// if(dbObjs.size()>0){
		// queryDbObjs.put("$and", dbObjs);
		// }
		LogService.debug(DatabaseServiceConstant.TAG,
				this.getClass().getName(), "getObjects", "QUERY sql:"
						+ queryDBObject.toString());
		// String sql = SQLUtility.convertSelectSQL(table, condition);

		// Statement stat = conn.createStatement();
		// LogService.debug(DatabaseServiceConstant.TAG,
		// this.getClass().getName(), "getObjects", "QUERY sql:" + sql);
		// ResultSet rs = stat.executeQuery(sql);

		return new QueryObjectsModel(coll, queryDBObject);
	}

	public int getCount(String table, CriteriaCompo condition)
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
		return 0;
	}

	public int getCount(TableCompo tableCompo, CriteriaCompo condition)
			throws SQLException {

		return 0;
	}

	public QueryObjectsModel getObjects(TableCompo tableCompo,
			CriteriaCompo condition) throws SQLException {
		// Connection conn = ds.getConnection();
		/*
		 * tableCompo.get
		 * 
		 * String sql = MangodbUtility..(tableCompo, condition);
		 * LogService.debug(DatabaseServiceConstant.TAG,
		 * this.getClass().getName(), "getObjects", "QUERY TableCompo sql:" +
		 * sql); Statement stat = conn.createStatement(); ResultSet rs =
		 * stat.executeQuery(sql);
		 */

		// ArrayList<QueryObjectsModel> qoms = new
		// ArrayList<QueryObjectsModel>();

		/*
		 * 所有使用的table列表
		 */

		Hashtable<String, TableCriteria> tables = new Hashtable();
		for (int i = 0; i < tableCompo.getTables().size(); i++) {

			String table = tableCompo.getTables().get(i);
			String variable = tableCompo.getVariables().get(i);
			String key = tableCompo.getKeys().get(i);

			System.out.println("[" + i + "]getObjects:table:" + table
					+ ",variable:" + variable + ",key:" + key);

			ArrayList<String> columns = tableCompo.getColumns(i);
			TableCriteria tc = new TableCriteria(table, columns, key);

			tables.put(variable, tc);
		}

		// ArrayList<Criteria> criterias = condition.getCriteras();

		/*
		 * for(int k = 0;k<tables.size();k++){ Criteria c = criterias.get(k);
		 * if(!c.isQuote()){ //join if(k-1>-1){ String logical =
		 * condition.getConditions().get(k-1); } String srcTableVar =
		 * c.getColumnTableVariable(); String srcColumnName =
		 * c.getSrcColumnName(); String dstTableVar = c.getValueTableVariable();
		 * String dstColumnName = c.getDstColumnName(); } else{ String variable
		 * = c.getColumnTableVariable(); TableCriteria tc =
		 * tables.get(variable);//取得運作的collection String logical = "";
		 * if(k-1>-1){ logical = condition.getConditions().get(k-1); }
		 * tc.addNormalRelation(c.getDBObject(logical));
		 * 
		 * 
		 * //BasicDBObject bdbc = new BasicDBObject();
		 * 
		 * } }
		 */

		QueryObjectsModel qom = new QueryObjectsModel();
		DBCollection calculateCollection = db.createCollection(
				qom.getCalculateCollectionKey(), new BasicDBObject());
		DBCollection firstCollection = null;
		// for(int l = tables.size()-1;l>-1;l--){
		// int l = 0;

		// boolean joinable = false;

		for (int l = 0; l < tables.size(); l++) {

			String table = tableCompo.getTables().get(l); // get table name
			DBCollection coll = db.getCollection(table);
			firstCollection = coll;

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
				qom.addJoin(tableVariable, coll, c.getSrcColumnName(), // 作為join的Index
						tableCompo.getColumns(l));
			} else {
				qom.addJoin(tableVariable, coll, tableCompo.getKeys().get(l), // 作為join的Index
						tableCompo.getColumns(l));
			}

			/*
			 * for(int h = 0;h<condition.getCriteras().size();h++){ Criteria c =
			 * condition.getCriteras().get(h); if(!c.isQuote()){
			 * if(c.getSrcTableVariable().equals(tableVariable)){
			 * System.out.println
			 * ("["+h+"]getObjects:table variable["+tableVariable
			 * +"] match:src:"+c.getSrcColumnName()); qom.addJoin(tableVariable,
			 * coll, c.getSrcColumnName(), //作為join的Index
			 * tableCompo.getColumns(l)); //joinable = true; } else
			 * if(c.getDstTableVariable().equals(tableVariable)){
			 * System.out.println
			 * ("["+h+"]getObjects:table variable["+tableVariable
			 * +"] match:dst:"+c.getDstColumnName()); qom.addJoin(tableVariable,
			 * coll, c.getDstColumnName(), //作為join的Index
			 * tableCompo.getColumns(l)); } else{
			 * System.out.println("["+h+"]getObjects:["
			 * +c.getDstTableVariable()+"]table variable["
			 * +tableVariable+"] match:dst:"+c.getDstColumnName()); } } }
			 */

			// qom.addJoin(tableVariable, coll,
			// tableCompo.getKeys().get(l), //作為join的Index
			// tableCompo.getColumns(l));
			// coll.mapReduce(map, reduce, outputTarget, query)
			// qom.addQuery(coll, dbObj);

			// QueryObjectsModel qom = new QueryObjectsModel(coll, dbObj);
			// /qoms.add(qom);
			// qom.query()

		}

		BasicDBObject queryDBObject = new BasicDBObject();
		// ArrayList<Criteria> criterias = condition.getCriteras();
		ArrayList<String> logicals = condition.getConditions();
		ArrayList<CriteriaElement> cs = condition.getCriteriaElements();
		ArrayList<BasicDBObject> dbObjs = new ArrayList<BasicDBObject>();
		/*
		 * for(int i=0;i<cs.size();i++){ if(cs.get(i).isQuote()){
		 * //cs.get(i).getDBObject(queryDBObject);
		 * cs.get(i).getDBObject(queryDBObject); //dbObjs.add(dbObj); } }
		 */
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

		qom.execute(calculateCollection, queryDBObject);

		// DBObject dbObj = null;

		return qom;
	}

	@Override
	public void createTable(String tableSchema) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createDatabase(String database) throws SQLException {
		// TODO Auto-generated method stub

	}

}
