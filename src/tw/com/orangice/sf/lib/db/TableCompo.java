package tw.com.orangice.sf.lib.db;

import java.util.ArrayList;

public class TableCompo {
	ArrayList<String> tables = new ArrayList<String>();
	ArrayList<String> variables = new ArrayList<String>();
	public TableCompo(){
		
	}
	public void add(String table, String variable){
		tables.add(table);
		variables.add(variable);
	}
	public String render(){
		String buf = "";
		for(int i=0;i<tables.size();i++){
			String table = tables.get(i);
			String variable = variables.get(i);
			if(i!=0){
				buf = buf+ ",";
			}
			buf = buf + " "+table+" as "+ variable ;
		}
		return buf;
	}
}
