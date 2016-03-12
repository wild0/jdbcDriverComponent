package tw.com.orangice.sf.lib.db.component;

import java.util.ArrayList;

public class TableCompo {
	ArrayList<String> tables = new ArrayList<String>();
	ArrayList<String> variables = new ArrayList<String>();
	ArrayList<String> keys = new ArrayList<String>();
	ArrayList<ArrayList<String>> columns = new ArrayList<ArrayList<String>>();
	//String columns[] ;
	
	public TableCompo(){
		
	}
	public void add(String table, String variable, String[] tableColumns, String keyColumn){
		tables.add(table);
		variables.add(variable);
		
		ArrayList<String > cols = new ArrayList<String>();
		for(int i=0;i<tableColumns.length;i++){
			cols.add(tableColumns[i]);
		}
		keys.add(keyColumn);
		columns.add(cols);
		
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
	public ArrayList<String> getTables(){
		return tables;
	}
	public ArrayList<String> getVariables(){
		return variables;
	}
	public ArrayList<String> getKeys(){
		return keys;
	}
	public ArrayList<String> getColumns(int i){
		return columns.get(i);
	}
}
