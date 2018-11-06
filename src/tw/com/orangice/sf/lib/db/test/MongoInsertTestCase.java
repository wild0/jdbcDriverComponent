package tw.com.orangice.sf.lib.db.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tw.com.orangice.sf.lib.db.MongoDatabaseManager;

public class MongoInsertTestCase {
	MongoDatabaseManager mongoMgr = null;
	public MongoInsertTestCase() {
		
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		//mongoMgr = new MongoDatabaseManager(null, "localhost", 27017, "root", "password123", "test-java-collection");
		//mongoMgr.dropCollection("test-java-table");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		
		//fail("Not yet implemented"); // TODO
		try{
			mongoMgr = new MongoDatabaseManager(null, "140.115.35.136", 27017, "root", "password123", "test-java-collection");
			mongoMgr.dropCollection("test-java-table");
			mongoMgr.insertSQL("test-java-table", new String[]{"aaa","bbb"}, new String[]{"1","2"}, "code");
			assert(true);
		}
		catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
