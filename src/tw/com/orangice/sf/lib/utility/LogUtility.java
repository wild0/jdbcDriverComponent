package tw.com.orangice.sf.lib.utility;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;



public class LogUtility {
	static Logger logger = null;
	public static void initial(Logger _logger){
		
		logger = _logger;
	}
	public static Logger getLogger(){
		return logger;
	}
}
