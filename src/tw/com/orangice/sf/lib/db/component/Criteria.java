package tw.com.orangice.sf.lib.db.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Criteria extends CriteriaElement {
	String column = "";
	Object value = null;
	String operator = " = ";
	String function = "";
	boolean quote = true;// 是否需要加引號

	public boolean isQuote() {
		return quote;
	}

	public Criteria(String column, long value) {
		this(column, value, " = ", "", true);
	}

	public Criteria(String column, String value) {
		this(column, value, " = ", "", true);
	}

	public Criteria(String column, int value) {
		this(column, value, " = ", "", true);
	}

	public Criteria(String column, String value, String operator) {
		this(column, value, operator, "", true);
	}

	public Criteria(String column, long value, String operator) {
		this(column, (value), operator, "", true);
	}

	public Criteria(String column, int value, String operator) {
		this(column, (value), operator, "", true);
	}

	public Criteria(String column, String value, String operator, boolean quote) {
		this(column, value, operator, "", quote);
	}

	public Criteria(String column, Object value, String operator,
			String function, boolean quote) {
		this.column = column;
		this.value = value;
		this.operator = operator;
		this.function = function;
		this.quote = quote;
	}

	public boolean isColumnContainVariable() {
		return (column.contains("."));
	}

	public boolean isValueContainVariable() {
		if (value instanceof String) {
			return ((String) value).contains(".");
		} else {
			return false;
		}
	}

	public String getSrcTableVariable() {
		int index = column.indexOf('.');
		if (index != -1) {
			return column.substring(0, index);
		} else {
			return "";
		}
	}

	public String getSrcColumnName() {
		int index = column.indexOf('.');
		if (index != -1) {
			return column.substring(index + 1);
		} else {
			return column;
		}

	}

	public static String getSrcColumnName(String columnName) {
		int index = columnName.indexOf('.');
		// System.out.println("Index:"+index);
		if (index != -1) {
			return columnName.substring(index + 1);
		} else {
			return columnName;
		}

	}

	public String getDstTableVariable() {
		if (value instanceof String && !quote) {
			String valueStr = (String) value;
			int index = valueStr.indexOf('.');
			if (index != -1) {
				return valueStr.substring(0, index);
			} else {
				return "";
			}
		} else {
			return "";
		}
	}

	public String getDstColumnName() {
		if (value instanceof String && !quote) {
			String valueStr = (String) value;
			int index = valueStr.indexOf('.');
			if (index != -1) {
				return valueStr;
			} else {
				return valueStr.substring(index + 1);
			}
		} else {
			return "";
		}
	}

	public String render() {
		if (quote) {
			if (value instanceof String) {
				return column + operator + "\"" + value + "\"";
			} else {
				return column + operator + "\"" + value + "\"";
			}
		} else {
			return column + operator + value;
		}
	}

	public BasicDBObject getDBObject(BasicDBObject headDBObject) {
		// BasicDBObject dbObject = new BasicDBObject();
		/*
		 * String tcolumn = getSrcColumnName(); headDBObjec.append(tcolumn,
		 * getValue());
		 * System.out.println("getDBObject:col:"+tcolumn+",value:"+getValue());
		 */
		operator = operator.trim();
		BasicDBObject dbObject = new BasicDBObject();

		String column = getSrcColumnName();
		System.out.println("getDBObject:(" + operator + "," + column + ","
				+ value.getClass().getName() + ")");

		if (operator.equals("=")) {
			// headDBObject.append(column, value);

			headDBObject.append(column, value);
			dbObject.put(column, value);
			System.out.println("getDBObject:bobj (" + headDBObject.toString()
					+ ")");

		} else if (operator.equals("!=")) {
			// $ne
			headDBObject.append(column, new BasicDBObject("$ne", value));
			dbObject.put(column, new BasicDBObject("$ne", value));
		} else if (operator.equals(">")) {
			// $lt
			headDBObject.append(column, new BasicDBObject("$gt", value));
			dbObject.put(column, new BasicDBObject("$gt", value));
		} else if (operator.equals("<")) {
			// $lt
			headDBObject.append(column, new BasicDBObject("$lt", value));
			dbObject.put(column, new BasicDBObject("$lt", value));
			
		} else if(operator.trim().toLowerCase().equals("like")){
			
			System.out.println("mongodb like getDBObject regex "+(String)value);
			
			headDBObject.append(column, new BasicDBObject("$regex", Pattern.compile((String)value)));
			dbObject.put(column, new BasicDBObject("$regex", Pattern.compile((String)value)));
			//Document element = new Document("$regex", Pattern.compile((String)value));
			//bson.put(column, element);
		}
		System.out.println("getDBObject:result headDBObject ("
				+ headDBObject.toString() + ")");
		System.out.println("getDBObject:result (" + dbObject.toString() + ")");
		return dbObject;

		// return headDBObjec;
	}

	public BasicDBObject getJoinDBObject(String prefix,
			BasicDBObject headDBObjec) {
		// BasicDBObject dbObject = new BasicDBObject();
		String tcolumn = getSrcColumnName();
		headDBObjec.append(prefix + "." + tcolumn, getValue());
		System.out.println("getDBObject:col:" + prefix + "." + tcolumn
				+ ",value:" + getValue());

		return headDBObjec;
	}

	public Object getValue() {
		return value;
	}

	public BasicDBObject getDBObject(BasicDBObject headDBObject,
			String condition) {
		BasicDBObject dbObject = new BasicDBObject();
		operator = operator.trim();
		String column = getSrcColumnName();
		System.out.println("getDBObject[" + condition + "]:(" + operator + ","
				+ column + "," + value.getClass().getName() + ")");

		if (operator.equals("=")) {
			// headDBObject.append(column, value);
			BasicDBObject bobj = new BasicDBObject("$eq", value);
			headDBObject.append(column, value);
			dbObject.put(column, value);
			System.out.println("getDBObject:bobj (" + bobj.toString() + ")");
		} else if (operator.equals("!=")) {
			// $ne
			BasicDBObject bobj = new BasicDBObject("$ne", value);
			headDBObject.append(column, bobj);
			dbObject.put(column, bobj);
			System.out.println("getDBObject:bobj (" + bobj.toString() + ")");
		} else if (operator.equals(">")) {
			// $lt
			BasicDBObject bobj = new BasicDBObject("$gt", value);
			headDBObject.append(column, bobj);
			dbObject.put(column, bobj);
			System.out.println("getDBObject:bobj (" + bobj.toString() + ")");
		} else if (operator.equals("<")) {
			// $lt
			BasicDBObject bobj = new BasicDBObject("$lt", value);
			headDBObject.append(column, bobj);
			dbObject.put(column, bobj);
			System.out.println("getDBObject:bobj (" + bobj.toString() + ")");
		}

		System.out.println("getDBObject:result (" + dbObject.toString() + ")");
		return dbObject;
		// return headDBObject;
	}
	
	public Document getBson() {
		System.out.println("mongodb getBson "+ String.valueOf(value) );
		Document bson = new Document();
		operator = operator.trim();
		String column = getSrcColumnName();
	
		if (operator.equals("=")) {
			System.out.println("mongodb like = "+String.valueOf(value));
			Document element = new Document("$eq", value);
			bson.put(column, element);
			
		} else if (operator.equals("!=")) {
			// $ne
			System.out.println("mongodb like != "+String.valueOf(value));
			Document element = new Document("$ne", value);
			bson.put(column, element);
			
		} else if (operator.equals(">")) {
			// $gt
			System.out.println("mongodb like > "+String.valueOf(value));
			Document element = new Document("$gt", value);
			bson.put(column, element);
			
		} else if (operator.equals("<")) {
			// $lt
			System.out.println("mongodb like < "+String.valueOf(value));
			Document element = new Document("$lt", value);
			bson.put(column, element);
		}
		else if(operator.trim().toLowerCase().equals("like")){
			System.out.println("mongodb like regex in bson "+String.valueOf(value));
			
			//Document element = new Document("$regex", Pattern.compile(String.valueOf(value)));
			//bson.put(column, element);
			bson.put(column, Pattern.compile(String.valueOf(value)));
		}
		else{
			
		}
		System.out.println("mongodb like complete "+bson);
		return bson;
	}

	public BasicDBObject getJoinDBObject(String prefix,
			BasicDBObject headDBObject, String condition) {
		BasicDBObject dbObject = new BasicDBObject();

		String column = getSrcColumnName();

		/*
		 * if (operator.equals("=")) { // headDBObject.append(column, value);
		 * dbObject.put(prefix+"."+column, new BasicDBObject("$eq", value)); }
		 * else if (operator.equals("!=")) { // $ne
		 * dbObject.put(prefix+"."+column, new BasicDBObject("$ne", value)); }
		 * else if (operator.equals(">")) { // $lt
		 * dbObject.put(prefix+"."+column, new BasicDBObject("$gt", value)); }
		 * else if (operator.equals("<")) { // $lt
		 * dbObject.put(prefix+"."+column, new BasicDBObject("$lt", value)); }
		 */

		if (operator.equals("=")) {
			// headDBObject.append(column, value);

			headDBObject.append(column, value);
			dbObject.put(column, value);
		} else if (operator.equals("!=")) {
			// $ne
			headDBObject.append(column, new BasicDBObject("$ne", value));
			dbObject.put(column, new BasicDBObject("$ne", value));
		} else if (operator.equals(">")) {
			// $lt
			headDBObject.append(column, new BasicDBObject("$gt", value));
			dbObject.put(column, new BasicDBObject("$gt", value));
		} else if (operator.equals("<")) {
			// $lt
			headDBObject.append(column, new BasicDBObject("$lt", value));
			dbObject.put(column, new BasicDBObject("$lt", value));
		}

		return headDBObject;
	}

	@Override
	public String renderWhere() {
		// TODO Auto-generated method stub
		try {
			String buf = "";

			if(render().trim().equals("")){
				return "";
			}
			else{
				buf = buf + " WHERE " + render();
				return buf;
			}

			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return "";
		}
	}

}
