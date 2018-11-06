package tw.com.orangice.sf.lib.db.component;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;

public class CriteriaCompo extends CriteriaElement {

	public String[] columns;

	String sortBy = "";
	String order = "";

	String secondSortBy = "";
	String secondOrder = "";

	int start = 0;

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getLimit() {
		return limit;
	}
	public String getSortBy(){
		return sortBy;
	}
	public String getOrder(){
		return order;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	int limit = 0;
	String groupBy = "";

	ArrayList<CriteriaElement> criteriaElements = new ArrayList<CriteriaElement>();
	ArrayList<String> conditions = new ArrayList<String>();

	public CriteriaCompo(Criteria c) {
		criteriaElements.add(c);
	}

	public CriteriaCompo() {

	}

	public ArrayList<CriteriaElement> getCriteriaElements() {
		return criteriaElements;
	}

	public Criteria getJoinCriteria(String tableVariable) {
		for (int i = 0; i < criteriaElements.size(); i++) {
			CriteriaElement c = criteriaElements.get(i);
			if (c instanceof Criteria) {
				Criteria criteria = (Criteria) c;
				if (!criteria.isQuote()
						&& criteria.getSrcTableVariable().equals(tableVariable)) {
					return criteria;
				}
			}
		}
		return null;
	}

	public ArrayList<String> getConditions() {
		return conditions;
	}

	public void addFirstCriteria(CriteriaElement c) {
		criteriaElements.add(c);
	}

	public void addFirst(CriteriaElement c, String condition) {
		criteriaElements.add(0, c);
		conditions.add(0, condition);
	}

	public void add(CriteriaElement c, String condition) {
		criteriaElements.add(c);
		conditions.add(condition);
	}

	public void setOrder(String order) {
		this.order = order;
	}

	public void setSortBy(String sortBy) {
		this.sortBy = sortBy;
	}

	public void setSecondOrder(String order) {
		this.secondOrder = order;
	}

	public void setSecondSortBy(String sortBy) {
		this.secondSortBy = sortBy;
	}

	public BasicDBObject renderDBObject() {

		int count = 0;

		boolean quote = false;
		BasicDBObject headDBObject = new BasicDBObject();

		ArrayList<Object> orArr = new ArrayList<Object>();
		// ArrayList<Object> inArr = new ArrayList<Object>();

		ArrayList<Object> addArr = new ArrayList<Object>();
		if (criteriaElements.size() == 1) {
			((Criteria) criteriaElements.get(0)).getDBObject(headDBObject);

		} else {

			for (int i = 0; i < criteriaElements.size(); i++) {
				Criteria c = (Criteria) criteriaElements.get(i);

				Bson bson = c.getBson();

				// BasicDBObject rowQuery = null;
				// if(i-1<0){
				// rowQuery= c.getDBObject(headDBObject);
				// addArr.add(rowQuery);
				// conditionQuery.a
				// }
				// else{

				// rowQuery = c.getDBObject(headDBObject, conditions.get(i));
				if (conditions.get(i).equals("and")) {
					addArr.add(bson);
				} else if (conditions.get(i).equals("or")) {
					orArr.add(bson);
				}
				// }
			}

			headDBObject.put("$and", addArr);
			headDBObject.put("$or", orArr);
		}

		return headDBObject;

	}

	public Document renderBson() {
		int count = 0;
		boolean quote = false;
		Document document = new Document();

		ArrayList<Object> orArr = new ArrayList<Object>();
		ArrayList<Object> andArr = new ArrayList<Object>();

		System.out.println("renderBson:criteriaElements.size():"
				+ criteriaElements.size() + ":"+conditions.size());
		if (criteriaElements.size() == 1) {
			// 只有一筆condition
			Criteria c = (Criteria) criteriaElements.get(0);
			//document.append(c.column, c.value);
			
			//System.out.println("mongodb like regex(single) "+(String)c.value);
			//Document element = new Document("$regex", Pattern.compile((String)c.value));
			if(c.operator.equals("like")){
				document.put(c.column, Pattern.compile((String)c.value));
			}
			else{
				document.put(c.column, c.value);
			}
			
			//document.append(c.column, element);
			
		} else {
			for (int i = 0; i < criteriaElements.size(); i++) {
				Criteria c = (Criteria) criteriaElements.get(i);
				Bson bson = c.getBson();
				int j = i-1;
				if (conditions.size() > 0 && j>=0) {
					System.out.println("mongodb like regex(multi) condition"+conditions.get(j).trim().toLowerCase());
					if (conditions.get(j).trim().toLowerCase().equals("and")) {
						andArr.add(bson);
						System.out.println("mongodb like regex(multi) and condition, size:"+andArr.size());
					} else if (conditions.get(j).trim().toLowerCase().equals("or")) {
						orArr.add(bson);
						System.out.println("mongodb like regex(multi) or condition, size:"+orArr.size());
					}
				}
				else if(j==-1){
					andArr.add(bson);
					orArr.add(bson);
				}
			}
			if (andArr.size() > 1) {
				document.put("$and", andArr);
			}
			if (orArr.size() > 1) {
				document.put("$or", orArr);
			}
			System.out.println("mongodb like regex(multi) "+document.toJson());
		}

		return document;
	}

	public String render() {
		try {
			String buf = "";

			int count = 0;

			boolean quote = false;

			for (int i = 0; i < criteriaElements.size(); i++) {
				// System.out.println(String.valueOf(!quote)+","+String.valueOf(i<conditions.size())+","+String.valueOf(conditions.get(i).toLowerCase()));
				if (!quote && i < conditions.size()
						&& conditions.get(i).toLowerCase().trim().equals("or")) {

					CriteriaElement c = criteriaElements.get(i);
					if (i != 0) {
						buf = buf + " " + conditions.get(count) + " ";
						count++;
					}

					buf = buf + " ( ";
					quote = true;

					buf = buf + c.render();
				} else {
					CriteriaElement c = criteriaElements.get(i);
					if (i != 0) {
						buf = buf + " " + conditions.get(count) + " ";
						count++;
					}
					buf = buf + c.render();
				}

				if (quote && i < conditions.size()
						&& conditions.get(i).toLowerCase().trim().equals("and")) {
					buf = buf + " ) ";
					quote = false;
				} else if (quote && i >= conditions.size()) {
					buf = buf + " ) ";
					quote = false;
				}

			}
			if (!sortBy.equals("")) {
				buf = buf + " ORDER BY " + sortBy + " " + order + " ";
			}
			if (!secondSortBy.equals("")) {
				buf = buf + ", " + secondSortBy + " " + secondOrder + " ";
			}
			if (limit != 0) {
				buf = buf + " limit " + start + "," + limit + " ";
			}

			// System.out.println("CriteriaCompo render:"+buf);
			return buf;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return "";
		}
	}

	@Override
	public String renderWhere() {
		// TODO Auto-generated method stub
		try {
			String buf = "";
			if (criteriaElements.size() == 0) {
				buf = buf + render();
				return buf;
			} else if (criteriaElements.size() > 0) {
				buf = buf + " WHERE " + render();
				return buf;
			} else {
				return "";
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return "";
		}
	}
}
