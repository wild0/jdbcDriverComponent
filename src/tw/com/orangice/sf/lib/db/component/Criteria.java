package tw.com.orangice.sf.lib.db.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

public class Criteria {
	String column = "";
	String value = "";
	String operator = " = ";
	String function = "";
	boolean quote = true;
	
	
	public Criteria(String column, long value){
		this(column, String.valueOf(value), " = ", "", true);
	}
	public Criteria(String column, String value){
		this(column, value, " = ", "", true);
	}
	public Criteria(String column, String value, String operator){
		this(column, value, operator, "", true);
	}
	public Criteria(String column, long value, String operator){
		this(column, String.valueOf(value), operator, "", true);
	}
	public Criteria(String column, String value, String operator,  boolean quote){
		this(column, value, operator, "", quote);
	}
	public Criteria(String column, String value, String operator, String function, boolean quote){
		this.column = column;
		this.value = value;
		this.operator = operator;
		this.function = function;
		this.quote = quote;
	}
	
	public String render(){
		if(quote){
			return column + operator +"\""+value +"\"";
		}
		else{
			return column + operator +value ;
		}
	}
	
	
}
