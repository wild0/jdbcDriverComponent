package tw.com.orangice.sf.lib.db;

import java.util.ArrayList;

public class CriteriaCompo {
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
	
	ArrayList<Criteria> criterias = new ArrayList<Criteria>();
	ArrayList<String> conditions = new ArrayList<String>();
	
	public CriteriaCompo(Criteria c){
		criterias.add(c);
	}
	public CriteriaCompo(){
		
	}
	
	public void addFirstCriteria(Criteria c){
		criterias.add(c);
	}
	public void add(Criteria c, String condition){
		criterias.add(c);
		conditions.add(condition);
	}
	public void setOrder(String order){
		this.order = order;
	}
	public void setSortBy(String sortBy){
		this.sortBy = sortBy;
	}
	public String render(){
		String buf = "";
		if(criterias.size()>0){
			buf = buf  + " WHERE ";
		}
		int count = 0;
		for(int i=0;i<criterias.size();i++){
			Criteria c = criterias.get(i);
			if(i!=0){
				buf = buf + " "+ conditions.get(count)+" ";
				count++;
			}
			
			buf = buf + c.render();
			
		}
		if(!sortBy.equals("")){
			buf = buf + " ORDER BY "+sortBy+" "+order+" ";
		}
		if(limit!=0){
			buf = buf+ " limit ("+start+","+limit+")";
		}
		
		
		System.out.println("CriteriaCompo render:"+buf);
		return buf;
	}
}
