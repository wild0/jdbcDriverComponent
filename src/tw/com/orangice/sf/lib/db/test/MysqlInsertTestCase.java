/**
 * 
 */
package tw.com.orangice.sf.lib.db.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tw.com.orangice.sf.lib.db.DatabaseManager;
import tw.com.orangice.sf.lib.db.MongoDatabaseManager;
import tw.com.orangice.sf.lib.log.LogService;

/**
 * @author roy
 *
 */
public class MysqlInsertTestCase {
	
	DatabaseManager mysqlMgr = null;

	public MysqlInsertTestCase() {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		LogService logService = new LogService("_test");
		mysqlMgr = new DatabaseManager(LogService.getInstance(), "localhost", 3306, "root", "boy627", "acer_paperless");
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try{
			
			mysqlMgr.insertSQL("_test", new String[]{"test_text","test_int"}, new Object[]{"1",2}, "id");
			assert(true);
		}
		catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
