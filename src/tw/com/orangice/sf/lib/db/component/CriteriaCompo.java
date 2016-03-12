package tw.com.orangice.sf.lib.db.component;

import java.util.ArrayList;
import java.util.Hashtable;

import com.mongodb.BasicDBObject;

public class CriteriaCompo extends CriteriaElement {

	public String[] columns;

	String sortBy = "";
	String order = "";
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
				BasicDBObject rowQuery = null;
				// if(i-1<0){
				// rowQuery= c.getDBObject(headDBObject);
				// addArr.add(rowQuery);
				// conditionQuery.a
				// }
				// else{

				rowQuery = c.getDBObject(headDBObject, conditions.get(i));
				if (conditions.get(i).equals("and")) {
					addArr.add(rowQuery);
				} else if (conditions.get(i).equals("or")) {
					orArr.add(rowQuery);
				}
				// }
			}

			headDBObject.put("$and", addArr);
			headDBObject.put("$or", orArr);
		}

		return headDBObject;

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
			if(criteriaElements.size()==0){
				buf = buf +  render();
				return buf;
			}else if(criteriaElements.size()>0){
				buf = buf + " WHERE " + render();
				return buf;
			}
			else{
				return "";
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return "";
		}
	}
}
